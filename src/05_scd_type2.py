"""
Decathlon Slowly Changing Dimensions (SCD Type 2 + Type 1 hybrid)

Owns the three history-tracked dimensions of the star schema:

    dim_customer   (tracks status, language, location, loyalty tier)
    dim_product    (tracks price, MRP, status, brand/category assignment)
    dim_supplier   (tracks status, rating, lead time, MOQ, risk, compliance)

Type 2  -> a change closes the current row and inserts a NEW row.
Type 1  -> a change overwrites the value in ALL versions of that row.

Usage
-----
    python3 src/05_scd_type2.py
    python3 src/05_scd_type2.py --dry-run
    python3 src/05_scd_type2.py --effective-date 2025-06-01

Normally 04_build_star_schema.py calls this module for you.
Needs PostgreSQL 13+ (uses trim_scale).
"""

import os
import re
import sys
import traceback
from datetime import date

import psycopg


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

FIRST_VERSION_START = "1900-01-01"
OPEN_END = "9999-12-31"


SCD_DIMS = {
    "dim_customer": {
        "key": "customer_key",
        "nk": "customer_id",
        "attrs": [
            ("first_name", "TEXT", 1),
            ("last_name", "TEXT", 1),
            ("full_name", "TEXT", 1),
            ("registration_date", "DATE", 1),
            ("customer_status", "TEXT", 2),
            ("preferred_language", "TEXT", 2),
            ("country", "TEXT", 2),
            ("city", "TEXT", 2),
            ("state", "TEXT", 2),
            ("loyalty_tier", "TEXT", 2),
        ],
        "source": f"""
            SELECT c.customer_id::text                          AS customer_id,
                   c.customer_first_name::text                  AS first_name,
                   c.customer_last_name::text                   AS last_name,
                   TRIM(CONCAT_WS(' ', c.customer_first_name,
                                       c.customer_last_name))   AS full_name,
                   c.registration_date::date                    AS registration_date,
                   c.customer_status::text                      AS customer_status,
                   c.preferred_language::text                   AS preferred_language,
                   c.customer_country::text                     AS country,
                   c.customer_city::text                        AS city,
                   c.customer_state::text                       AS state,
                   l.loyalty_tier::text                         AS loyalty_tier
            FROM {SRC}.customers c
            LEFT JOIN (
                SELECT DISTINCT ON (customer_id) customer_id, loyalty_tier
                FROM {SRC}.loyalty_accounts
                ORDER BY customer_id,
                         loyalty_points::numeric DESC NULLS LAST,
                         loyalty_account_id
            ) l ON l.customer_id = c.customer_id
        """,
    },
    "dim_product": {
        "key": "product_key",
        "nk": "product_id",
        "attrs": [
            ("sku", "TEXT", 1),
            ("product_name", "TEXT", 1),
            ("product_description", "TEXT", 1),
            ("brand_name", "TEXT", 1),
            ("category_name", "TEXT", 1),
            ("sub_category_id", "TEXT", 1),
            ("sport_id", "TEXT", 1),
            ("gender", "TEXT", 1),
            ("age_group", "TEXT", 1),
            ("size", "TEXT", 1),
            ("color", "TEXT", 1),
            ("material", "TEXT", 1),
            ("currency", "TEXT", 1),
            ("brand_id", "TEXT", 2),
            ("category_id", "TEXT", 2),
            ("product_type", "TEXT", 2),
            ("product_status", "TEXT", 2),
            ("selling_price", "NUMERIC", 2),
            ("mrp", "NUMERIC", 2),
        ],
        "source": f"""
            SELECT p.product_id::text           AS product_id,
                   p.sku::text                  AS sku,
                   p.product_name::text         AS product_name,
                   p.product_description::text  AS product_description,
                   b.brand_name::text           AS brand_name,
                   cat.category_name::text      AS category_name,
                   p.sub_category_id::text      AS sub_category_id,
                   p.sport_id::text             AS sport_id,
                   p.gender::text               AS gender,
                   p.age_group::text            AS age_group,
                   p.size::text                 AS size,
                   p.color::text                AS color,
                   p.material::text             AS material,
                   p.currency::text             AS currency,
                   p.brand_id::text             AS brand_id,
                   p.category_id::text          AS category_id,
                   p.product_type::text         AS product_type,
                   p.product_status::text       AS product_status,
                   p.selling_price::numeric     AS selling_price,
                   p.mrp::numeric               AS mrp
            FROM {SRC}.products p
            LEFT JOIN {SRC}.brands b      ON b.brand_id = p.brand_id
            LEFT JOIN {SRC}.categories cat ON cat.category_id = p.category_id
        """,
    },
    "dim_supplier": {
        "key": "supplier_key",
        "nk": "supplier_id",
        "attrs": [
            ("supplier_name", "TEXT", 1),
            ("supplier_type", "TEXT", 1),
            ("supplier_country", "TEXT", 1),
            ("supplier_status", "TEXT", 2),
            ("supplier_rating", "NUMERIC", 2),
            ("lead_days", "NUMERIC", 2),
            ("moq", "NUMERIC", 2),
            ("risk_score", "NUMERIC", 2),
            ("compliance_status", "TEXT", 2),
        ],
        "source": f"""
            SELECT s.supplier_id::text                  AS supplier_id,
                   s.supplier_name::text                AS supplier_name,
                   s.supplier_type::text                AS supplier_type,
                   s.supplier_country::text             AS supplier_country,
                   s.supplier_status::text              AS supplier_status,
                   s.supplier_rating::numeric           AS supplier_rating,
                   s.supplier_lead_days::numeric        AS lead_days,
                   s.supplier_moq::numeric              AS moq,
                   s.supplier_risk_score::numeric       AS risk_score,
                   s.supplier_compliance_status::text   AS compliance_status
            FROM {SRC}.suppliers s
        """,
    },
}

SOURCE_TABLES = [
    "customers", "loyalty_accounts", "products", "brands", "categories",
    "suppliers",
]


def attr_names(cfg, scd_type=None):
    return [n for n, _, k in cfg["attrs"] if scd_type in (None, k)]


def hash_expression(cfg, alias="s"):
    """md5 over the Type 2 attributes; NULL-safe and scale-safe."""
    parts = []
    for name, pg_type, kind in cfg["attrs"]:
        if kind != 2:
            continue
        ref = f"{alias}.{name}"
        if pg_type == "NUMERIC":
            ref = f"trim_scale({ref})"
        parts.append(f"COALESCE({ref}::text, '~null~')")
    return "md5(concat_ws('|', " + ", ".join(parts) + "))"


def changed_columns_expr(cols):
    cases = ", ".join(
        f"CASE WHEN d.{c} IS DISTINCT FROM s.{c} THEN '{c}'::text END"
        for c in cols
    )
    return f"ARRAY_REMOVE(ARRAY[{cases}], NULL)"


def json_expr(alias, cols):
    pairs = ", ".join(f"'{c}'::text, {alias}.{c}" for c in cols)
    return f"jsonb_build_object({pairs})"


def distinct_condition(cols):
    return " OR ".join(f"d.{c} IS DISTINCT FROM s.{c}" for c in cols)


def create_table_sql(dim, cfg):
    columns = ",\n    ".join(f"{n} {t}" for n, t, _ in cfg["attrs"])
    return f"""
        CREATE TABLE IF NOT EXISTS {DW}.{dim} (
            {cfg['key']} BIGSERIAL PRIMARY KEY,
            {cfg['nk']} TEXT NOT NULL,
            {columns},
            effective_from DATE NOT NULL,
            effective_to DATE NOT NULL DEFAULT DATE '{OPEN_END}',
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            version_no INTEGER NOT NULL DEFAULT 1,
            row_hash TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """


def unknown_member_sql(dim, cfg):
    names = (
        [cfg["key"], cfg["nk"]]
        + attr_names(cfg)
        + ["effective_from", "effective_to", "is_current",
           "version_no", "row_hash"]
    )
    values = (
        ["-1", "'UNKNOWN'"]
        + ["'Unknown'" if t == "TEXT" else "NULL" for _, t, _ in cfg["attrs"]]
        + [f"DATE '{FIRST_VERSION_START}'", f"DATE '{OPEN_END}'",
           "TRUE", "1", "'unknown'"]
    )
    return (
        f"INSERT INTO {DW}.{dim} ({', '.join(names)}) "
        f"VALUES ({', '.join(values)}) ON CONFLICT DO NOTHING"
    )


def ensure_scd_tables(cur):
    """Create schema, change log and the three SCD dimensions (idempotent)."""
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DW}")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DW}.scd_change_log (
            change_id BIGSERIAL PRIMARY KEY,
            dimension_name TEXT NOT NULL,
            natural_key TEXT NOT NULL,
            change_type TEXT NOT NULL
                CHECK (change_type IN ('NEW', 'TYPE2', 'TYPE1')),
            changed_columns TEXT[],
            old_values JSONB,
            new_values JSONB,
            effective_date DATE NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_scd_change_log_dim
        ON {DW}.scd_change_log (dimension_name, natural_key)
    """)

    for dim, cfg in SCD_DIMS.items():
        cur.execute(create_table_sql(dim, cfg))
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{dim}_current "
            f"ON {DW}.{dim} ({cfg['nk']}) WHERE is_current"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS ix_{dim}_asof "
            f"ON {DW}.{dim} ({cfg['nk']}, effective_from, effective_to)"
        )
        cur.execute(unknown_member_sql(dim, cfg))


def apply_scd(cur, dim, cfg, as_of):
    """Compare OLTP with one dimension and apply Type 1 / Type 2 changes."""
    table = f"{DW}.{dim}"
    nk, key = cfg["nk"], cfg["key"]
    t1 = attr_names(cfg, 1)
    t2 = attr_names(cfg, 2)
    everything = attr_names(cfg)
    stg = f"stg_{dim}"
    as_of_text = as_of.isoformat()

    cur.execute(
        f"SELECT MAX(effective_from) FROM {table} "
        f"WHERE effective_from > DATE '{FIRST_VERSION_START}' "
        f"AND effective_from < DATE '{OPEN_END}'"
    )
    latest = cur.fetchone()[0]
    if latest is not None and latest > as_of:
        raise ValueError(
            f"{dim}: --effective-date {as_of_text} is earlier than the "
            f"latest existing version start ({latest}). Use a later date."
        )

    cur.execute(f"DROP TABLE IF EXISTS pg_temp.{stg}")
    cur.execute(
        f"CREATE TEMP TABLE {stg} ON COMMIT DROP AS "
        f"SELECT s.*, {hash_expression(cfg)} AS row_hash "
        f"FROM ({cfg['source']}) s"
    )
    cur.execute(f"CREATE INDEX ON {stg} ({nk})")

    stats = {"new_keys": 0, "type2_versions": 0, "type1_updates": 0}

    if t1:
        cur.execute(
            f"""
            INSERT INTO {DW}.scd_change_log
                (dimension_name, natural_key, change_type, changed_columns,
                 old_values, new_values, effective_date)
            SELECT '{dim}', s.{nk}, 'TYPE1',
                   {changed_columns_expr(t1)},
                   {json_expr('d', t1)},
                   {json_expr('s', t1)},
                   %s::date
            FROM {table} d
            JOIN {stg} s ON s.{nk} = d.{nk}
            WHERE d.is_current AND ({distinct_condition(t1)})
            """,
            (as_of_text,),
        )
        set_clause = ", ".join(f"{c} = s.{c}" for c in t1)
        cur.execute(
            f"""
            UPDATE {table} d SET {set_clause}
            FROM {stg} s
            WHERE s.{nk} = d.{nk} AND ({distinct_condition(t1)})
            """
        )
        stats["type1_updates"] = cur.rowcount

    cur.execute(
        f"""
        INSERT INTO {DW}.scd_change_log
            (dimension_name, natural_key, change_type, changed_columns,
             old_values, new_values, effective_date)
        SELECT '{dim}', s.{nk}, 'NEW', NULL, NULL,
               {json_expr('s', everything)}, %s::date
        FROM {stg} s
        WHERE NOT EXISTS (SELECT 1 FROM {table} d WHERE d.{nk} = s.{nk})
        """,
        (as_of_text,),
    )
    stats["new_keys"] = cur.rowcount

    cur.execute(
        f"""
        INSERT INTO {DW}.scd_change_log
            (dimension_name, natural_key, change_type, changed_columns,
             old_values, new_values, effective_date)
        SELECT '{dim}', s.{nk}, 'TYPE2',
               {changed_columns_expr(t2)},
               {json_expr('d', t2)},
               {json_expr('s', t2)},
               %s::date
        FROM {table} d
        JOIN {stg} s ON s.{nk} = d.{nk}
        WHERE d.is_current AND d.row_hash <> s.row_hash
        """,
        (as_of_text,),
    )
    cur.execute(
        f"""
        UPDATE {table} d
        SET effective_to = %s::date, is_current = FALSE
        FROM {stg} s
        WHERE s.{nk} = d.{nk} AND d.is_current AND d.row_hash <> s.row_hash
        """,
        (as_of_text,),
    )
    stats["type2_versions"] = cur.rowcount

    insert_cols = ", ".join(
        [nk] + everything
        + ["effective_from", "effective_to", "is_current",
           "version_no", "row_hash"]
    )
    select_cols = ", ".join(f"s.{c}" for c in [nk] + everything)
    cur.execute(
        f"""
        INSERT INTO {table} ({insert_cols})
        SELECT {select_cols},
               CASE WHEN v.max_version IS NULL
                    THEN DATE '{FIRST_VERSION_START}'
                    ELSE %s::date END,
               DATE '{OPEN_END}',
               TRUE,
               COALESCE(v.max_version, 0) + 1,
               s.row_hash
        FROM {stg} s
        LEFT JOIN (
            SELECT {nk}, MAX(version_no) AS max_version
            FROM {table} GROUP BY {nk}
        ) v ON v.{nk} = s.{nk}
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} d WHERE d.{nk} = s.{nk} AND d.is_current
        )
        """,
        (as_of_text,),
    )

    return stats


def run_scd(cur, as_of):
    """
    Apply SCD logic to every tracked dimension.
    MUST be called inside an open transaction (temp tables are
    ON COMMIT DROP). Returns {dimension: stats}.
    """
    ensure_scd_tables(cur)
    results = {}

    for dim, cfg in SCD_DIMS.items():
        results[dim] = apply_scd(cur, dim, cfg, as_of)

    return results


def print_scd_summary(results, as_of):
    print(f"\nSCD RESULT (effective date {as_of.isoformat()})")
    print(f"{'dimension':16} {'new keys':>9} {'type2 new ver':>14} "
          f"{'type1 rows':>11}")
    print("-" * 54)
    for dim, s in results.items():
        print(f"{dim:16} {s['new_keys']:>9} {s['type2_versions']:>14} "
              f"{s['type1_updates']:>11}")


def get_as_of(argv):
    """Read --effective-date YYYY-MM-DD from argv (default: today)."""
    if "--effective-date" in argv:
        i = argv.index("--effective-date")
        try:
            return date.fromisoformat(argv[i + 1])
        except (IndexError, ValueError):
            raise ValueError("Use --effective-date YYYY-MM-DD") from None
    return date.today()


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


def require_source_tables(conn):
    missing = []
    with conn.cursor() as cur:
        for table_name in SOURCE_TABLES:
            cur.execute("SELECT to_regclass(%s)", (f"{SRC}.{table_name}",))
            if cur.fetchone()[0] is None:
                missing.append(table_name)
    if missing:
        raise RuntimeError(
            "OLTP tables missing: " + ", ".join(missing)
            + ". Run 02_load_postgres_oltp.py first."
        )


def main():
    dry_run = "--dry-run" in sys.argv
    as_of = get_as_of(sys.argv)

    print("Connecting to PostgreSQL...")
    with connect_postgres() as conn:
        require_source_tables(conn)

        with conn.transaction(force_rollback=dry_run):
            with conn.cursor() as cur:
                results = run_scd(cur, as_of)

    print_scd_summary(results, as_of)

    if dry_run:
        print("\nDRY RUN: everything was rolled back.")
    else:
        print(f"\nSUCCESS. History: SELECT * FROM {DW}.scd_change_log "
              "ORDER BY change_id DESC;")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nSCD loader failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
