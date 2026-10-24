"""
Decathlon Star Schema Builder (OLTP -> Data Warehouse)

Reads the normalized tables in  decathlon_oltp  and builds a Kimball-style
star schema in  decathlon_dw  (same database, separate schema).

DIMENSIONS
----------
    dim_date            calendar, one row per day (+ Unknown member)
    dim_customer        SCD Type 2   (built by 05_scd_type2.py)
    dim_product         SCD Type 2   (built by 05_scd_type2.py)
    dim_supplier        SCD Type 2   (built by 05_scd_type2.py)
    dim_warehouse       Type 1 (overwrite)
    dim_campaign        Type 1
    dim_advertisement   Type 1

FACTS  (grain = one row per business event / line)
-----
    fact_sales_line           one sales order line
    fact_payment              one payment
    fact_shipment             one shipment
    fact_return               one return
    fact_inventory_snapshot   one product x warehouse snapshot row
    fact_purchase_order_line  one purchase order line
    fact_ad_event             one advertising event
    fact_service_case         one after-sales service case
    fact_risk_case            one risk / fraud case

KEY IDEAS
---------
- Facts store SURROGATE keys. For SCD dimensions the key is the dimension
  VERSION that was valid on the event date (point-in-time join), so an order
  placed before a customer moved keeps pointing at the old city.
- Every dimension has an Unknown member with key -1; a fact never has a NULL
  dimension key, and missing dates also map to date_key -1.
- The business id of each fact (sales_line_id, payment_id, ...) is its
  primary key, so the load is an idempotent UPSERT: run it any number of
  times. Rows deleted from OLTP by CDC are removed from the facts as well
  (turn off with DW_PRUNE_FACTS=0).
- Everything runs in ONE transaction: all changes apply or none do.

USAGE
-----
    python3 src/04_build_star_schema.py                  # build / refresh
    python3 src/04_build_star_schema.py --dry-run        # roll everything back
    python3 src/04_build_star_schema.py --effective-date 2025-06-01
    python3 src/04_build_star_schema.py --rebuild-facts  # drop + reload facts
    python3 src/04_build_star_schema.py --rebuild-all    # DELETES SCD history

Pipeline order
    first time : 02 -> 04 -> 06
    every cycle: 03 (CDC) -> 04 (runs 05 inside) -> 06

Environment options
    OLTP_SCHEMA=decathlon_oltp   DW_SCHEMA=decathlon_dw
    DIM_DATE_START=2015-01-01    DIM_DATE_END=2035-12-31
    DW_PRUNE_FACTS=1
"""

import importlib.util
import os
import re
import sys
import traceback

import psycopg


# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

DATABASE = os.getenv("PGDATABASE", "decathlon_oltp")
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_USER = os.getenv("PGUSER", os.getenv("USER", "apple"))
DB_PASSWORD = os.getenv("PGPASSWORD") or None

SRC = os.getenv("OLTP_SCHEMA", "decathlon_oltp")
DW = os.getenv("DW_SCHEMA", "decathlon_dw")

for _name in (SRC, DW):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", _name):
        raise ValueError(f"Unsafe schema name: {_name!r}")

DATE_START = os.getenv("DIM_DATE_START", "2015-01-01")
DATE_END = os.getenv("DIM_DATE_END", "2035-12-31")
PRUNE_FACTS = os.getenv("DW_PRUNE_FACTS", "1") != "0"

DRY_RUN = "--dry-run" in sys.argv
REBUILD_FACTS = "--rebuild-facts" in sys.argv
REBUILD_ALL = "--rebuild-all" in sys.argv


# ---------------------------------------------------------
# SMALL SQL HELPERS
# ---------------------------------------------------------

def dk(expr):
    """date expression -> yyyymmdd integer key, -1 when NULL."""
    return f"COALESCE(TO_CHAR(({expr})::date, 'YYYYMMDD')::integer, -1)"


def key(alias, column):
    """surrogate key from a LEFT JOINed dimension, -1 when not found."""
    return f"COALESCE({alias}.{column}, -1)"


def flag(expr):
    """boolean / 0-1 / 'true' text -> 1 or 0, whatever type OLTP inferred."""
    return (
        f"CASE WHEN LOWER(({expr})::text) IN ('1', 't', 'true') "
        "THEN 1 ELSE 0 END"
    )


def scd_lookup(alias, dim, nk, src_expr, date_expr):
    """Point-in-time join to the dimension version valid on date_expr."""
    day = f"COALESCE(({date_expr})::date, CURRENT_DATE)"
    return (
        f"LEFT JOIN {DW}.{dim} {alias} ON {alias}.{nk} = ({src_expr})::text "
        f"AND {day} >= {alias}.effective_from "
        f"AND {day} < {alias}.effective_to"
    )


def t1_lookup(alias, dim, nk, src_expr):
    """Plain join to a Type 1 dimension."""
    return (
        f"LEFT JOIN {DW}.{dim} {alias} ON {alias}.{nk} = ({src_expr})::text"
    )


# ---------------------------------------------------------
# TYPE 1 DIMENSIONS
# ---------------------------------------------------------

TYPE1_DIMS = {
    "dim_warehouse": {
        "key": "warehouse_key",
        "nk": "warehouse_id",
        "attrs": [
            ("warehouse_name", "TEXT"),
            ("city", "TEXT"),
            ("state", "TEXT"),
        ],
        "source_table": "warehouses",
        "source": f"""
            SELECT warehouse_id::text    AS warehouse_id,
                   warehouse_name::text  AS warehouse_name,
                   warehouse_city::text  AS city,
                   warehouse_state::text AS state
            FROM {SRC}.warehouses
        """,
    },
    "dim_campaign": {
        "key": "campaign_key",
        "nk": "campaign_id",
        "attrs": [
            ("campaign_name", "TEXT"),
            ("campaign_type", "TEXT"),
            ("campaign_objective", "TEXT"),
            ("campaign_status", "TEXT"),
            ("start_date", "DATE"),
            ("end_date", "DATE"),
            ("budget", "NUMERIC"),
        ],
        "source_table": "campaigns",
        "source": f"""
            SELECT campaign_id::text          AS campaign_id,
                   campaign_name::text        AS campaign_name,
                   campaign_type::text        AS campaign_type,
                   campaign_objective::text   AS campaign_objective,
                   campaign_status::text      AS campaign_status,
                   campaign_start_date::date  AS start_date,
                   campaign_end_date::date    AS end_date,
                   campaign_budget::numeric   AS budget
            FROM {SRC}.campaigns
        """,
    },
    "dim_advertisement": {
        "key": "advertisement_key",
        "nk": "advertisement_id",
        "attrs": [
            ("campaign_id", "TEXT"),
            ("advertisement_name", "TEXT"),
            ("advertisement_channel", "TEXT"),
            ("advertisement_format", "TEXT"),
            ("advertisement_status", "TEXT"),
            ("ad_platform", "TEXT"),
            ("creative_id", "TEXT"),
        ],
        "source_table": "advertisements",
        "source": f"""
            SELECT advertisement_id::text      AS advertisement_id,
                   campaign_id::text           AS campaign_id,
                   advertisement_name::text    AS advertisement_name,
                   advertisement_channel::text AS advertisement_channel,
                   advertisement_format::text  AS advertisement_format,
                   advertisement_status::text  AS advertisement_status,
                   ad_platform::text           AS ad_platform,
                   creative_id::text           AS creative_id
            FROM {SRC}.advertisements
        """,
    },
}


def create_dim_date(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DW}.dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE,
            calendar_year INTEGER,
            calendar_quarter INTEGER,
            calendar_month INTEGER,
            month_name TEXT,
            year_month TEXT,
            week_of_year INTEGER,
            day_of_month INTEGER,
            day_of_week INTEGER,
            day_name TEXT,
            is_weekend BOOLEAN
        )
    """)
    cur.execute(f"""
        INSERT INTO {DW}.dim_date
        VALUES (-1, NULL, NULL, NULL, NULL, 'Unknown', 'Unknown', NULL,
                NULL, NULL, 'Unknown', NULL)
        ON CONFLICT DO NOTHING
    """)
    cur.execute(
        f"""
        INSERT INTO {DW}.dim_date
        SELECT TO_CHAR(g.d, 'YYYYMMDD')::integer,
               g.d::date,
               EXTRACT(YEAR FROM g.d)::integer,
               EXTRACT(QUARTER FROM g.d)::integer,
               EXTRACT(MONTH FROM g.d)::integer,
               TRIM(TO_CHAR(g.d, 'Month')),
               TO_CHAR(g.d, 'YYYY-MM'),
               EXTRACT(WEEK FROM g.d)::integer,
               EXTRACT(DAY FROM g.d)::integer,
               EXTRACT(ISODOW FROM g.d)::integer,
               TRIM(TO_CHAR(g.d, 'Day')),
               EXTRACT(ISODOW FROM g.d) IN (6, 7)
        FROM generate_series(%s::timestamp, %s::timestamp,
                             INTERVAL '1 day') AS g(d)
        ON CONFLICT (date_key) DO NOTHING
        """,
        (DATE_START, DATE_END),
    )


def create_and_load_type1_dim(cur, dim, cfg):
    nk, surrogate = cfg["nk"], cfg["key"]
    names = [n for n, _ in cfg["attrs"]]
    columns = ",\n            ".join(f"{n} {t}" for n, t in cfg["attrs"])

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DW}.{dim} (
            {surrogate} BIGSERIAL PRIMARY KEY,
            {nk} TEXT NOT NULL UNIQUE,
            {columns},
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # Unknown member.
    unknown_values = ["-1", "'UNKNOWN'"] + [
        "'Unknown'" if t == "TEXT" else "NULL" for _, t in cfg["attrs"]
    ]
    cur.execute(
        f"INSERT INTO {DW}.{dim} ({surrogate}, {nk}, {', '.join(names)}) "
        f"VALUES ({', '.join(unknown_values)}) ON CONFLICT DO NOTHING"
    )

    # Type 1 upsert: insert new keys, overwrite changed values only.
    set_clause = ", ".join(f"{n} = EXCLUDED.{n}" for n in names)
    old_row = ", ".join(f"t.{n}" for n in names)
    new_row = ", ".join(f"EXCLUDED.{n}" for n in names)
    cur.execute(
        f"""
        INSERT INTO {DW}.{dim} AS t ({nk}, {', '.join(names)})
        SELECT {nk}, {', '.join(names)} FROM ({cfg['source']}) s
        ON CONFLICT ({nk}) DO UPDATE
        SET {set_clause}, updated_at = NOW()
        WHERE ({old_row}) IS DISTINCT FROM ({new_row})
        """
    )


# ---------------------------------------------------------
# FACT DEFINITIONS
#   cols = (column, postgres type, select expression)
#   The FIRST column is the primary key (business id of the event).
#   Columns ending in "date_key" reference dim_date; the others in
#   KEY_REFS reference their dimension.
# ---------------------------------------------------------

KEY_REFS = {
    "customer_key": "dim_customer",
    "product_key": "dim_product",
    "supplier_key": "dim_supplier",
    "warehouse_key": "dim_warehouse",
    "campaign_key": "dim_campaign",
    "advertisement_key": "dim_advertisement",
}

FACTS = [
    {
        "name": "fact_sales_line",
        "src": "sales_order_items",
        "cols": [
            ("sales_line_id", "TEXT", "i.sales_line_id::text"),
            ("order_id", "TEXT", "i.order_id::text"),
            ("order_number", "TEXT", "o.order_number::text"),
            ("order_date_key", "INTEGER", dk("o.order_date")),
            ("customer_key", "BIGINT", key("dc", "customer_key")),
            ("product_key", "BIGINT", key("dp", "product_key")),
            ("order_status", "TEXT", "o.order_status::text"),
            ("sales_channel", "TEXT", "o.sales_channel::text"),
            ("store_id", "TEXT", "o.store_id::text"),
            ("currency", "TEXT", "o.currency::text"),
            ("quantity", "NUMERIC", "i.quantity::numeric"),
            ("unit_price", "NUMERIC", "i.unit_price::numeric"),
            ("gross_line_amount", "NUMERIC", "i.gross_line_amount::numeric"),
            ("discount_amount", "NUMERIC", "i.discount_amount::numeric"),
            ("tax_amount", "NUMERIC", "i.tax_amount::numeric"),
            ("net_line_amount", "NUMERIC", "i.net_line_amount::numeric"),
        ],
        "frm": (
            f"{SRC}.sales_order_items i "
            f"JOIN {SRC}.sales_orders o ON o.order_id = i.order_id "
            + scd_lookup("dc", "dim_customer", "customer_id",
                         "o.customer_id", "o.order_date") + " "
            + scd_lookup("dp", "dim_product", "product_id",
                         "i.product_id", "o.order_date")
        ),
    },
    {
        "name": "fact_payment",
        "src": "payments",
        "cols": [
            ("payment_id", "TEXT", "p.payment_id::text"),
            ("order_id", "TEXT", "p.order_id::text"),
            ("payment_date_key", "INTEGER", dk("p.payment_date")),
            ("customer_key", "BIGINT", key("dc", "customer_key")),
            ("payment_method", "TEXT", "p.payment_method::text"),
            ("payment_gateway", "TEXT", "p.payment_gateway::text"),
            ("payment_status", "TEXT", "p.payment_status::text"),
            ("currency", "TEXT", "p.currency::text"),
            ("payment_amount", "NUMERIC", "p.payment_amount::numeric"),
        ],
        "frm": (
            f"{SRC}.payments p "
            f"JOIN {SRC}.sales_orders o ON o.order_id = p.order_id "
            + scd_lookup("dc", "dim_customer", "customer_id",
                         "o.customer_id", "p.payment_date")
        ),
    },
    {
        "name": "fact_shipment",
        "src": "shipments",
        "cols": [
            ("shipment_id", "TEXT", "sh.shipment_id::text"),
            ("order_id", "TEXT", "sh.order_id::text"),
            ("dispatch_date_key", "INTEGER", dk("sh.dispatch_date")),
            ("expected_delivery_date_key", "INTEGER",
             dk("sh.expected_delivery_date")),
            ("actual_delivery_date_key", "INTEGER",
             dk("sh.actual_delivery_date")),
            ("customer_key", "BIGINT", key("dc", "customer_key")),
            ("carrier_name", "TEXT", "sh.carrier_name::text"),
            ("tracking_number", "TEXT", "sh.tracking_number::text"),
            ("shipment_status", "TEXT", "sh.shipment_status::text"),
            ("delivery_days", "INTEGER",
             "(sh.actual_delivery_date::date - sh.dispatch_date::date)"),
            ("delay_days", "INTEGER",
             "(sh.actual_delivery_date::date "
             "- sh.expected_delivery_date::date)"),
            ("is_delivered", "SMALLINT",
             "CASE WHEN sh.actual_delivery_date IS NOT NULL "
             "THEN 1 ELSE 0 END"),
            ("is_late", "SMALLINT",
             "CASE WHEN sh.actual_delivery_date::date "
             "> sh.expected_delivery_date::date THEN 1 ELSE 0 END"),
        ],
        "frm": (
            f"{SRC}.shipments sh "
            f"JOIN {SRC}.sales_orders o ON o.order_id = sh.order_id "
            + scd_lookup("dc", "dim_customer", "customer_id",
                         "o.customer_id", "sh.dispatch_date")
        ),
    },
    {
        "name": "fact_return",
        "src": "returns",
        "cols": [
            ("return_id", "TEXT", "r.return_id::text"),
            ("order_id", "TEXT", "r.order_id::text"),
            ("sales_line_id", "TEXT", "r.sales_line_id::text"),
            ("return_date_key", "INTEGER", dk("r.return_date")),
            ("customer_key", "BIGINT", key("dc", "customer_key")),
            ("product_key", "BIGINT", key("dp", "product_key")),
            ("return_reason", "TEXT", "r.return_reason::text"),
            ("return_status", "TEXT", "r.return_status::text"),
            ("refund_amount", "NUMERIC", "r.refund_amount::numeric"),
            ("days_since_order", "INTEGER",
             "(r.return_date::date - o.order_date::date)"),
        ],
        "frm": (
            f"{SRC}.returns r "
            f"JOIN {SRC}.sales_orders o ON o.order_id = r.order_id "
            f"LEFT JOIN {SRC}.sales_order_items li "
            "ON li.sales_line_id = r.sales_line_id "
            + scd_lookup("dc", "dim_customer", "customer_id",
                         "o.customer_id", "r.return_date") + " "
            + scd_lookup("dp", "dim_product", "product_id",
                         "COALESCE(r.product_id, li.product_id)",
                         "r.return_date")
        ),
    },
    {
        "name": "fact_inventory_snapshot",
        "src": "inventory_snapshots",
        "cols": [
            ("inventory_id", "TEXT", "inv.inventory_id::text"),
            ("snapshot_date_key", "INTEGER", dk("inv.last_stock_update")),
            ("product_key", "BIGINT", key("dp", "product_key")),
            ("warehouse_key", "BIGINT", key("w", "warehouse_key")),
            ("stock_on_hand", "NUMERIC", "inv.stock_on_hand::numeric"),
            ("reserved_stock", "NUMERIC", "inv.reserved_stock::numeric"),
            ("available_stock", "NUMERIC", "inv.available_stock::numeric"),
            ("damaged_stock", "NUMERIC", "inv.damaged_stock::numeric"),
            ("in_transit_stock", "NUMERIC", "inv.in_transit_stock::numeric"),
            ("inventory_status", "TEXT", "inv.inventory_status::text"),
        ],
        "frm": (
            f"{SRC}.inventory_snapshots inv "
            + scd_lookup("dp", "dim_product", "product_id",
                         "inv.product_id", "inv.last_stock_update") + " "
            + t1_lookup("w", "dim_warehouse", "warehouse_id",
                        "inv.warehouse_id")
        ),
    },
    {
        "name": "fact_purchase_order_line",
        "src": "purchase_order_items",
        "cols": [
            ("purchase_order_item_id", "TEXT", "poi.purchase_order_item_id::text"),
            ("purchase_order_id", "TEXT", "poi.purchase_order_id::text"),
            ("purchase_order_date_key", "INTEGER",
             dk("po.purchase_order_date")),
            ("expected_receipt_date_key", "INTEGER",
             dk("po.expected_receipt_date")),
            ("supplier_key", "BIGINT", key("ds", "supplier_key")),
            ("warehouse_key", "BIGINT", key("w", "warehouse_key")),
            ("product_key", "BIGINT", key("dp", "product_key")),
            ("purchase_order_status", "TEXT",
             "po.purchase_order_status::text"),
            ("currency", "TEXT", "po.purchase_order_currency::text"),
            ("quantity_ordered", "NUMERIC", "poi.quantity_ordered::numeric"),
            ("quantity_received", "NUMERIC", "poi.quantity_received::numeric"),
            ("unit_cost", "NUMERIC", "poi.unit_cost::numeric"),
            ("line_amount", "NUMERIC", "poi.line_amount::numeric"),
        ],
        "frm": (
            f"{SRC}.purchase_order_items poi "
            f"JOIN {SRC}.purchase_orders po "
            "ON po.purchase_order_id = poi.purchase_order_id "
            + scd_lookup("ds", "dim_supplier", "supplier_id",
                         "po.supplier_id", "po.purchase_order_date") + " "
            + scd_lookup("dp", "dim_product", "product_id",
                         "poi.product_id", "po.purchase_order_date") + " "
            + t1_lookup("w", "dim_warehouse", "warehouse_id",
                        "po.warehouse_id")
        ),
    },
    {
        "name": "fact_ad_event",
        "src": "ad_events",
        "cols": [
            ("ad_event_id", "TEXT", "e.ad_event_id::text"),
            ("event_date_key", "INTEGER", dk("e.event_timestamp")),
            ("event_timestamp", "TIMESTAMP", "e.event_timestamp::timestamp"),
            ("campaign_key", "BIGINT", key("dcam", "campaign_key")),
            ("advertisement_key", "BIGINT", key("dad", "advertisement_key")),
            ("customer_key", "BIGINT", key("dc", "customer_key")),
            ("product_key", "BIGINT", key("dp", "product_key")),
            ("event_type", "TEXT", "e.event_type::text"),
            ("impressions", "SMALLINT", flag("e.impression_flag")),
            ("clicks", "SMALLINT", flag("e.click_flag")),
            ("conversions", "SMALLINT", flag("e.conversion_flag")),
            ("conversion_value", "NUMERIC", "e.conversion_value::numeric"),
            ("conversion_order_id", "TEXT", "e.conversion_order_id::text"),
        ],
        "frm": (
            f"{SRC}.ad_events e "
            + t1_lookup("dcam", "dim_campaign", "campaign_id",
                        "e.campaign_id") + " "
            + t1_lookup("dad", "dim_advertisement", "advertisement_id",
                        "e.advertisement_id") + " "
            + scd_lookup("dc", "dim_customer", "customer_id",
                         "e.customer_id", "e.event_timestamp") + " "
            + scd_lookup("dp", "dim_product", "product_id",
                         "e.product_id", "e.event_timestamp")
        ),
    },
    {
        "name": "fact_service_case",
        "src": "service_cases",
        "cols": [
            ("service_case_id", "TEXT", "sc.service_case_id::text"),
            ("service_case_number", "TEXT", "sc.service_case_number::text"),
            ("open_date_key", "INTEGER", dk("sc.case_open_date")),
            ("resolution_date_key", "INTEGER", dk("sc.resolution_date")),
            ("customer_key", "BIGINT", key("dc", "customer_key")),
            ("product_key", "BIGINT", key("dp", "product_key")),
            ("service_type", "TEXT", "sc.service_type::text"),
            ("issue_type", "TEXT", "sc.issue_type::text"),
            ("case_priority", "TEXT", "sc.case_priority::text"),
            ("case_status", "TEXT", "sc.case_status::text"),
            ("resolution_days", "INTEGER",
             "(sc.resolution_date::date - sc.case_open_date::date)"),
        ],
        "frm": (
            f"{SRC}.service_cases sc "
            + scd_lookup("dc", "dim_customer", "customer_id",
                         "sc.customer_id", "sc.case_open_date") + " "
            + scd_lookup("dp", "dim_product", "product_id",
                         "sc.product_id", "sc.case_open_date")
        ),
    },
    {
        "name": "fact_risk_case",
        "src": "risk_cases",
        "cols": [
            ("risk_case_id", "TEXT", "rc.risk_case_id::text"),
            ("risk_case_number", "TEXT", "rc.risk_case_number::text"),
            ("order_id", "TEXT", "rc.order_id::text"),
            ("payment_id", "TEXT", "rc.payment_id::text"),
            ("order_date_key", "INTEGER", dk("o.order_date")),
            ("customer_key", "BIGINT", key("dc", "customer_key")),
            ("risk_score", "NUMERIC", "rc.risk_score::numeric"),
            ("risk_level", "TEXT", "rc.risk_level::text"),
            ("risk_decision", "TEXT", "rc.risk_decision::text"),
            ("is_fraud", "SMALLINT", flag("rc.fraud_flag")),
            ("investigation_status", "TEXT", "rc.investigation_status::text"),
        ],
        "frm": (
            f"{SRC}.risk_cases rc "
            f"LEFT JOIN {SRC}.sales_orders o ON o.order_id = rc.order_id "
            + scd_lookup("dc", "dim_customer", "customer_id",
                         "rc.customer_id", "o.order_date")
        ),
    },
]


def fact_ddl(fact):
    defs = []
    for position, (name, pg_type, _) in enumerate(fact["cols"]):
        if position == 0:
            defs.append(f"{name} {pg_type} PRIMARY KEY")
        elif name.endswith("date_key"):
            defs.append(
                f"{name} {pg_type} NOT NULL "
                f"REFERENCES {DW}.dim_date (date_key)"
            )
        elif name in KEY_REFS:
            defs.append(
                f"{name} {pg_type} NOT NULL "
                f"REFERENCES {DW}.{KEY_REFS[name]} ({name})"
            )
        else:
            defs.append(f"{name} {pg_type}")

    defs.append("loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
    body = ",\n            ".join(defs)
    return f"CREATE TABLE IF NOT EXISTS {DW}.{fact['name']} (\n            {body}\n        )"


def create_fact(cur, fact):
    cur.execute(fact_ddl(fact))

    for name, _, _ in fact["cols"]:
        if name.endswith("_key"):
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS ix_{fact['name']}_{name} "
                f"ON {DW}.{fact['name']} ({name})"
            )


def load_fact(cur, fact):
    """UPSERT all rows; optionally remove rows that left OLTP."""
    table = f"{DW}.{fact['name']}"
    names = [c[0] for c in fact["cols"]]
    pk = names[0]
    select_list = ",\n               ".join(
        f"{expr} AS {name}" for name, _, expr in fact["cols"]
    )
    set_clause = ", ".join(f"{n} = EXCLUDED.{n}" for n in names[1:])

    cur.execute(
        f"""
        INSERT INTO {table} ({', '.join(names)})
        SELECT {select_list}
        FROM {fact['frm']}
        ON CONFLICT ({pk}) DO UPDATE
        SET {set_clause}, loaded_at = NOW()
        """
    )
    upserted = cur.rowcount
    pruned = 0

    if PRUNE_FACTS:
        cur.execute(
            f"""
            DELETE FROM {table} f
            WHERE NOT EXISTS (
                SELECT 1 FROM {SRC}.{fact['src']} s
                WHERE s.{pk}::text = f.{pk}
            )
            """
        )
        pruned = cur.rowcount

    return upserted, pruned


# ---------------------------------------------------------
# AUDIT AND CHECKS
# ---------------------------------------------------------

def write_audit_and_checks(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DW}.dw_load_audit (
            table_name TEXT PRIMARY KEY,
            row_count BIGINT NOT NULL,
            source_rows BIGINT,
            unknown_key_rows BIGINT,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    report = []

    entries = [(f["name"], f["src"], f["cols"]) for f in FACTS]
    entries += [(d, c["source_table"], None) for d, c in TYPE1_DIMS.items()]

    for table_name, source_table, cols in entries:
        cur.execute(f"SELECT COUNT(*) FROM {DW}.{table_name}")
        rows = cur.fetchone()[0]

        # Type 1 dims hold one extra row: the Unknown member.
        if cols is None:
            rows -= 1

        cur.execute(f"SELECT COUNT(*) FROM {SRC}.{source_table}")
        source_rows = cur.fetchone()[0]

        unknown_rows = None
        if cols is not None:
            key_cols = [
                c[0] for c in cols
                if c[0] in KEY_REFS
            ]
            if key_cols:
                condition = " OR ".join(f"{c} = -1" for c in key_cols)
                cur.execute(
                    f"SELECT COUNT(*) FROM {DW}.{table_name} "
                    f"WHERE {condition}"
                )
                unknown_rows = cur.fetchone()[0]

        cur.execute(
            f"""
            INSERT INTO {DW}.dw_load_audit
                (table_name, row_count, source_rows, unknown_key_rows,
                 loaded_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (table_name) DO UPDATE SET
                row_count = EXCLUDED.row_count,
                source_rows = EXCLUDED.source_rows,
                unknown_key_rows = EXCLUDED.unknown_key_rows,
                loaded_at = NOW()
            """,
            (table_name, rows, source_rows, unknown_rows),
        )
        report.append((table_name, rows, source_rows, unknown_rows))

    return report


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def connect_postgres():
    kwargs = {
        "dbname": DATABASE,
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "connect_timeout": 10,
        "autocommit": True,
    }
    if DB_PASSWORD:
        kwargs["password"] = DB_PASSWORD
    return psycopg.connect(**kwargs)


def require_oltp_tables(conn):
    needed = {f["src"] for f in FACTS}
    needed |= {c["source_table"] for c in TYPE1_DIMS.values()}
    needed |= {"customers", "loyalty_accounts", "products", "brands",
               "categories", "suppliers", "sales_orders"}

    missing = []
    with conn.cursor() as cur:
        for table_name in sorted(needed):
            cur.execute("SELECT to_regclass(%s)", (f"{SRC}.{table_name}",))
            if cur.fetchone()[0] is None:
                missing.append(table_name)

    if missing:
        raise RuntimeError(
            "OLTP tables missing: " + ", ".join(missing)
            + ". Run 02_load_postgres_oltp.py first."
        )


def confirm_rebuild_all():
    print(
        "\nWARNING: --rebuild-all drops the whole "
        f"'{DW}' schema, including the SCD history "
        "(old customer/product/supplier versions) that cannot be rebuilt "
        "from the current OLTP data."
    )
    answer = input("Type YES to continue: ").strip()
    if answer != "YES":
        print("Cancelled.")
        sys.exit(0)


def print_report(report):
    print("\nWAREHOUSE LOAD REPORT")
    print(f"{'table':28} {'rows':>9} {'source':>9} {'unknown keys':>13}")
    print("-" * 62)
    for table_name, rows, source_rows, unknown_rows in report:
        unknown = "" if unknown_rows is None else str(unknown_rows)
        print(f"{table_name:28} {rows:>9} {source_rows:>9} {unknown:>13}")

    mismatches = [r for r in report if r[1] != r[2]]
    unknowns = [r for r in report if r[3]]

    if mismatches:
        print("\nWARNING: row counts differ from OLTP for: "
              + ", ".join(m[0] for m in mismatches))
    if unknowns:
        print("NOTE: rows pointing at an Unknown (-1) dimension member: "
              + ", ".join(f"{u[0]}={u[3]}" for u in unknowns))


def load_scd_module():
    """Load 05_scd_type2.py from the same folder as this script."""
    path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "05_scd_type2.py"
    )
    if not os.path.exists(path):
        raise RuntimeError(f"Missing file: {path}")

    spec = importlib.util.spec_from_file_location("scd_type2", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    needed = ("get_as_of", "run_scd", "print_scd_summary")
    absent = [n for n in needed if not hasattr(module, n)]
    if absent:
        raise RuntimeError(
            f"{path} is not the SCD script (missing: {', '.join(absent)}). "
            "It must start with 'Decathlon Slowly Changing Dimensions'. "
            "Replace it with the real 05_scd_type2.py."
        )
    return module


def main():
    scd = load_scd_module()
    as_of = scd.get_as_of(sys.argv)

    if REBUILD_ALL:
        confirm_rebuild_all()

    print("Connecting to PostgreSQL...")
    with connect_postgres() as conn:
        require_oltp_tables(conn)

        with conn.transaction(force_rollback=DRY_RUN):
            with conn.cursor() as cur:
                if REBUILD_ALL:
                    cur.execute(f"DROP SCHEMA IF EXISTS {DW} CASCADE")
                    print(f"Dropped schema {DW}.")

                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DW}")

                if REBUILD_FACTS and not REBUILD_ALL:
                    for fact in reversed(FACTS):
                        cur.execute(
                            f"DROP TABLE IF EXISTS {DW}.{fact['name']} CASCADE"
                        )
                    print("Dropped fact tables (they are rebuilt below).")

                print("Building dimensions...")
                create_dim_date(cur)

                for dim, cfg in TYPE1_DIMS.items():
                    create_and_load_type1_dim(cur, dim, cfg)
                    print(f"  {dim} refreshed")

                # SCD Type 2 dimensions: must come BEFORE the facts.
                scd_results = scd.run_scd(cur, as_of)
                scd.print_scd_summary(scd_results, as_of)

                print("\nLoading facts...")
                for fact in FACTS:
                    create_fact(cur, fact)
                    upserted, pruned = load_fact(cur, fact)
                    extra = f", pruned {pruned}" if pruned else ""
                    print(f"  {fact['name']}: {upserted} rows{extra}")

                report = write_audit_and_checks(cur)

    print_report(report)

    if DRY_RUN:
        print("\nDRY RUN: everything was rolled back.")
    else:
        print(f"\nSUCCESS: star schema '{DW}' is up to date.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nStar schema build failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        if "foreign key" in str(exc).lower():
            print(
                "\nHint: a date outside dim_date? Widen DIM_DATE_START / "
                "DIM_DATE_END and rerun.",
                file=sys.stderr,
            )
        sys.exit(1)
