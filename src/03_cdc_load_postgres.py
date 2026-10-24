"""
Decathlon Silver-to-PostgreSQL CDC Loader (snapshot-diff change capture)

Run AFTER 02_load_postgres_oltp.py has created the tables once.

What it does on every run
-------------------------
1. Reads the latest Silver CSVs from the processed S3 API (in memory).
2. Compares every table with what is currently in PostgreSQL:
       new key      -> INSERT
       changed row  -> UPDATE (only the changed columns)
       missing key  -> DELETE
3. Writes every change to  decathlon_oltp.cdc_change_log
   (operation I/U/D, old values, new values, changed columns).
4. Records the run in decathlon_oltp.cdc_batch.

Safety
------
- The whole batch runs in ONE transaction: all changes apply or none do.
- Inserts/updates run parent-first, deletes run child-first, so foreign
  keys stay valid.
- If the Silver files have not changed since the last successful batch,
  the run is skipped.
- Delete guard: if a batch would delete more than CDC_MAX_DELETE_PCT percent
  of a table (default 50), it aborts (protects against a partial file).
- An advisory lock prevents two CDC runs at the same time.
- If the batch fails, rerun the script: nothing was changed, it simply retries.

Usage
-----
    python3 src/03_cdc_load_postgres.py              # apply changes
    python3 src/03_cdc_load_postgres.py --dry-run    # show changes, apply nothing
    python3 src/03_cdc_load_postgres.py --force      # run even if files look unchanged

Environment options
-------------------
    CDC_APPLY_DELETES=0     do not delete rows that disappeared from Silver
    CDC_MAX_DELETE_PCT=50   abort if deletes exceed this % of a table
"""

import hashlib
import json
import os
import sys
import traceback
from decimal import Decimal
from io import BytesIO

import pandas as pd
import psycopg
import requests
from psycopg import sql
from psycopg.types.json import Jsonb
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

BASE_URL = "https://zucau19vp0.execute-api.us-east-1.amazonaws.com"
PROJECT = "Decathlon"
SILVER_PREFIX = "silver/2025/01/01/"

DATABASE = os.getenv("PGDATABASE", "decathlon_oltp")
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_USER = os.getenv("PGUSER", os.getenv("USER", "apple"))
DB_PASSWORD = os.getenv("PGPASSWORD") or None

SCHEMA = "decathlon_oltp"
RUN_KEY = f"{PROJECT}/{SILVER_PREFIX}"

DRY_RUN = "--dry-run" in sys.argv
FORCE = "--force" in sys.argv or os.getenv("CDC_FORCE", "0") == "1"
APPLY_DELETES = os.getenv("CDC_APPLY_DELETES", "1") != "0"
MAX_DELETE_PCT = float(os.getenv("CDC_MAX_DELETE_PCT", "50"))
ADVISORY_LOCK_ID = 727201

session = requests.Session()
session.headers.update({"User-Agent": "Decathlon-Silver-CDC-Loader/1.0"})
_retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=None,
)
session.mount("https://", HTTPAdapter(max_retries=_retry))
session.mount("http://", HTTPAdapter(max_retries=_retry))


# ---------------------------------------------------------
# TABLE DEFINITIONS (same as 02_load_postgres_oltp.py)
# ---------------------------------------------------------

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "customer_addresses": ["address_id"],
    "loyalty_accounts": ["loyalty_account_id"],
    "products": ["product_id"],
    "categories": ["category_id"],
    "brands": ["brand_id"],
    "sales_orders": ["order_id"],
    "sales_order_items": ["sales_line_id"],
    "payments": ["payment_id"],
    "shipments": ["shipment_id"],
    "returns": ["return_id"],
    "suppliers": ["supplier_id"],
    "warehouses": ["warehouse_id"],
    "supplier_contracts": ["contract_id"],
    "inventory_snapshots": ["inventory_id"],
    "purchase_orders": ["purchase_order_id"],
    "purchase_order_items": ["purchase_order_item_id"],
    "service_cases": ["service_case_id"],
    "campaigns": ["campaign_id"],
    "advertisements": ["advertisement_id"],
    "ad_events": ["ad_event_id"],
    "risk_cases": ["risk_case_id"],
    "employees": ["employee_id"],
}

# Parents first. Deletes use the reverse of this order.
LOAD_ORDER = [
    "customers", "categories", "brands", "products", "customer_addresses",
    "loyalty_accounts", "suppliers", "warehouses", "campaigns", "employees",
    "sales_orders", "sales_order_items", "payments", "shipments", "returns",
    "supplier_contracts", "inventory_snapshots", "purchase_orders",
    "purchase_order_items", "service_cases", "advertisements", "ad_events",
    "risk_cases",
]

PG_TYPE_MAP = {
    "text": "TEXT",
    "character varying": "TEXT",
    "bigint": "BIGINT",
    "integer": "BIGINT",
    "numeric": "NUMERIC",
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMP",
    "boolean": "BOOLEAN",
}


# ---------------------------------------------------------
# VALUE HANDLING
# ---------------------------------------------------------

def is_null(value):
    """True for None, real NaN/NaT/NA and empty / whitespace-only strings."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(value, str) and value.strip() == ""


def normalize_value(value, pg_type):
    """Convert a CSV string to the Python type matching the column."""
    if is_null(value):
        return None

    value = str(value).strip()

    if pg_type == "BOOLEAN":
        if value.lower() in {"1", "true"}:
            return True
        if value.lower() in {"0", "false"}:
            return False
        return None

    if pg_type == "BIGINT":
        return int(float(value))

    if pg_type == "NUMERIC":
        return value

    if pg_type == "DATE":
        return pd.to_datetime(value, errors="raise").date()

    if pg_type == "TIMESTAMP":
        return pd.to_datetime(value, errors="raise").to_pydatetime()

    return value


def canon(value, pg_type):
    """
    Canonical string for comparison. Applied to BOTH the incoming CSV value
    and the value read from PostgreSQL, so '12.5' equals Decimal('12.50'),
    and date/time objects compare by ISO text.
    """
    if value is None:
        return None

    if pg_type == "BOOLEAN":
        return "true" if value else "false"

    if pg_type == "BIGINT":
        return str(int(value))

    if pg_type == "NUMERIC":
        return format(Decimal(str(value)).normalize(), "f")

    if pg_type in ("DATE", "TIMESTAMP"):
        return value.isoformat() if hasattr(value, "isoformat") else str(value)

    return str(value)


def make_key(canon_row, pk_idx):
    return "|".join(canon_row[i] for i in pk_idx)


# ---------------------------------------------------------
# S3 PROCESSED API
# ---------------------------------------------------------

def list_silver_files():
    response = session.get(
        f"{BASE_URL}/processed-files",
        params={"project": PROJECT, "path": SILVER_PREFIX},
        timeout=30,
    )
    response.raise_for_status()

    files = response.json().get("files", [])
    project_prefix = f"{PROJECT}/{SILVER_PREFIX}"
    selected = {}

    for item in files:
        if not isinstance(item, dict):
            continue

        key = item.get("key", "")

        if not key.startswith(project_prefix):
            continue

        filename = key[len(project_prefix):]

        if "/" in filename or not filename.lower().endswith(".csv"):
            continue

        table_name = filename[:-4].lower()

        if table_name not in PRIMARY_KEYS:
            continue

        if table_name in selected:
            raise RuntimeError(
                f"Duplicate Silver file found for table: {table_name}"
            )

        selected[table_name] = {
            "path": key[len(f"{PROJECT}/"):],
            "meta": item,
        }

    missing = sorted(set(PRIMARY_KEYS) - set(selected))

    if missing:
        raise RuntimeError(
            "Silver files missing for these expected tables: "
            + ", ".join(missing)
        )

    print(f"Discovered {len(selected)} expected Silver entity CSVs.")
    return selected


def compute_fingerprint(file_map):
    stable_fields = (
        "size", "Size", "last_modified", "LastModified", "etag", "ETag",
    )
    payload = {}

    for table_name, info in sorted(file_map.items()):
        entry = {"key": info["path"]}
        for field in stable_fields:
            if field in info["meta"]:
                entry[field] = info["meta"][field]
        payload[table_name] = entry

    text = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def download_csv_to_memory(relative_path):
    response = session.get(
        f"{BASE_URL}/processed-download",
        params={"project": PROJECT, "path": relative_path},
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    download_url = payload.get("download_url")

    if not download_url:
        raise RuntimeError(
            f"No download_url returned for {relative_path}. "
            f"API response: {payload}"
        )

    csv_response = session.get(download_url, timeout=60)
    csv_response.raise_for_status()

    return pd.read_csv(
        BytesIO(csv_response.content), dtype=str, keep_default_na=False
    )


def validate_table(table_name, df):
    if df.empty:
        raise ValueError(f"Table '{table_name}' contains no rows.")

    if df.columns.duplicated().any():
        raise ValueError(
            f"Table '{table_name}' has duplicate columns: "
            f"{df.columns[df.columns.duplicated()].tolist()}"
        )

    primary_key = PRIMARY_KEYS[table_name]
    missing_columns = [c for c in primary_key if c not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Table '{table_name}' is missing primary-key columns "
            f"{missing_columns}."
        )

    for column in primary_key:
        if df[column].map(is_null).any():
            raise ValueError(
                f"Table '{table_name}' has NULL/blank values in "
                f"primary-key column '{column}'."
            )

    if df.duplicated(subset=primary_key, keep=False).any():
        raise ValueError(
            f"Table '{table_name}' has duplicate primary-key values."
        )

    print(f"  Validated {table_name}: {len(df)} rows.")


def download_and_validate_all(file_map):
    frames = {}

    for table_name in LOAD_ORDER:
        print(f"Downloading and validating: {table_name}")
        df = download_csv_to_memory(file_map[table_name]["path"])
        validate_table(table_name, df)
        frames[table_name] = df

    return frames


# ---------------------------------------------------------
# DATABASE HELPERS
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


def ensure_cdc_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {s}.cdc_batch ("
                "batch_id BIGSERIAL PRIMARY KEY, "
                "source_fingerprint TEXT NOT NULL, "
                "status TEXT NOT NULL, "
                "started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                "finished_at TIMESTAMPTZ, "
                "inserts BIGINT NOT NULL DEFAULT 0, "
                "updates BIGINT NOT NULL DEFAULT 0, "
                "deletes BIGINT NOT NULL DEFAULT 0, "
                "detail TEXT)"
            ).format(s=sql.Identifier(SCHEMA))
        )
        cur.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {s}.cdc_change_log ("
                "change_id BIGSERIAL PRIMARY KEY, "
                "batch_id BIGINT NOT NULL, "
                "table_name TEXT NOT NULL, "
                "pk_value TEXT NOT NULL, "
                "operation CHAR(1) NOT NULL "
                "CHECK (operation IN ('I', 'U', 'D')), "
                "changed_columns TEXT[], "
                "old_values JSONB, "
                "new_values JSONB, "
                "captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            ).format(s=sql.Identifier(SCHEMA))
        )
        cur.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS idx_cdc_change_log_table "
                "ON {s}.cdc_change_log (table_name, batch_id)"
            ).format(s=sql.Identifier(SCHEMA))
        )


def require_tables(conn):
    missing = []

    with conn.cursor() as cur:
        for table_name in LOAD_ORDER:
            cur.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.{table_name}",))
            if cur.fetchone()[0] is None:
                missing.append(table_name)

    if missing:
        raise RuntimeError(
            "These tables do not exist yet: "
            + ", ".join(missing)
            + ". Run 02_load_postgres_oltp.py once before using CDC."
        )


def last_success_fingerprint(conn):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT source_fingerprint FROM {}.cdc_batch "
                "WHERE status = 'SUCCESS' ORDER BY batch_id DESC LIMIT 1"
            ).format(sql.Identifier(SCHEMA))
        )
        row = cur.fetchone()
    return row[0] if row else None


def start_batch(conn, fingerprint):
    with conn.cursor() as cur:
        # A batch still RUNNING means an earlier process crashed.
        cur.execute(
            sql.SQL(
                "UPDATE {}.cdc_batch SET status = 'ABORTED', "
                "finished_at = NOW() WHERE status = 'RUNNING'"
            ).format(sql.Identifier(SCHEMA))
        )
        cur.execute(
            sql.SQL(
                "INSERT INTO {}.cdc_batch (source_fingerprint, status) "
                "VALUES (%s, 'RUNNING') RETURNING batch_id"
            ).format(sql.Identifier(SCHEMA)),
            (fingerprint,),
        )
        return cur.fetchone()[0]


def fail_batch(conn, batch_id, message):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "UPDATE {}.cdc_batch SET status = 'FAILED', "
                "finished_at = NOW(), detail = %s WHERE batch_id = %s"
            ).format(sql.Identifier(SCHEMA)),
            (message[:2000], batch_id),
        )


def get_table_columns(cur, table_name):
    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (SCHEMA, table_name),
    )
    return [
        (name, PG_TYPE_MAP.get(data_type, "TEXT"))
        for name, data_type in cur.fetchall()
    ]


# ---------------------------------------------------------
# CHANGE DETECTION
# ---------------------------------------------------------

def build_plan(cur, table_name, df):
    """Compare incoming CSV rows with the table; return the change plan."""
    columns = get_table_columns(cur, table_name)
    names = [name for name, _ in columns]
    types = [pg_type for _, pg_type in columns]

    if set(names) != set(df.columns):
        added = sorted(set(df.columns) - set(names))
        removed = sorted(set(names) - set(df.columns))
        raise RuntimeError(
            f"Schema changed for '{table_name}' "
            f"(new columns: {added}, missing columns: {removed}). "
            "CDC cannot alter table structure. Run "
            "02_load_postgres_oltp.py --restart for a full reload."
        )

    pk_idx = [names.index(c) for c in PRIMARY_KEYS[table_name]]

    # Current database rows.
    cur.execute(
        sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(", ").join(sql.Identifier(n) for n in names),
            sql.Identifier(SCHEMA),
            sql.Identifier(table_name),
        )
    )
    existing = {}
    for row in cur.fetchall():
        canon_row = tuple(canon(v, t) for v, t in zip(row, types))
        existing[make_key(canon_row, pk_idx)] = canon_row

    # Incoming CSV rows.
    incoming = {}
    for record in df.to_dict(orient="records"):
        try:
            raw = tuple(
                normalize_value(record[n], t) for n, t in zip(names, types)
            )
        except Exception as exc:
            raise RuntimeError(
                f"Cannot convert a value in '{table_name}': {exc}. If a "
                "column type changed, run 02_load_postgres_oltp.py --restart."
            ) from exc

        canon_row = tuple(canon(v, t) for v, t in zip(raw, types))
        incoming[make_key(canon_row, pk_idx)] = (raw, canon_row)

    inserts, updates, deletes = [], [], []

    for key, (raw, canon_row) in incoming.items():
        if key not in existing:
            inserts.append((key, raw, canon_row))
        elif existing[key] != canon_row:
            changed = [
                i for i in range(len(names))
                if existing[key][i] != canon_row[i]
            ]
            updates.append((key, changed, raw, existing[key], canon_row))

    for key, old_canon in existing.items():
        if key not in incoming:
            deletes.append((key, old_canon))

    return {
        "names": names,
        "types": types,
        "pk_idx": pk_idx,
        "existing_count": len(existing),
        "unchanged": len(incoming) - len(inserts) - len(updates),
        "inserts": inserts,
        "updates": updates,
        "deletes": deletes,
    }


def guard_deletes(plans):
    if not APPLY_DELETES:
        return

    for table_name, plan in plans.items():
        total = plan["existing_count"]
        removed = len(plan["deletes"])

        if total and removed * 100.0 / total > MAX_DELETE_PCT:
            raise RuntimeError(
                f"Delete guard: batch would delete {removed} of {total} "
                f"rows in '{table_name}' (limit {MAX_DELETE_PCT:g}%). "
                "The Silver file may be incomplete. If this is intended, "
                "rerun with CDC_MAX_DELETE_PCT=100."
            )


# ---------------------------------------------------------
# APPLY CHANGES
# ---------------------------------------------------------

LOG_SQL = sql.SQL(
    "INSERT INTO {}.cdc_change_log "
    "(batch_id, table_name, pk_value, operation, changed_columns, "
    "old_values, new_values) VALUES (%s, %s, %s, %s, %s, %s, %s)"
).format(sql.Identifier(SCHEMA))


def as_dict(names, canon_row, indexes=None):
    indexes = range(len(names)) if indexes is None else indexes
    return {names[i]: canon_row[i] for i in indexes}


def apply_upserts(cur, batch_id, table_name, plan):
    names = plan["names"]
    pk_names = [names[i] for i in plan["pk_idx"]]
    log_rows = []

    # INSERT
    if plan["inserts"]:
        insert_stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(SCHEMA),
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(n) for n in names),
            sql.SQL(", ").join(sql.Placeholder() for _ in names),
        )
        cur.executemany(insert_stmt, [raw for _, raw, _ in plan["inserts"]])

        for key, _, canon_row in plan["inserts"]:
            log_rows.append((
                batch_id, table_name, key, "I", None, None,
                Jsonb(as_dict(names, canon_row)),
            ))

    # UPDATE (only the changed columns)
    for key, changed, raw, old_canon, new_canon in plan["updates"]:
        set_clause = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(names[i]))
            for i in changed
        )
        where_clause = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(sql.Identifier(c)) for c in pk_names
        )
        update_stmt = sql.SQL("UPDATE {}.{} SET {} WHERE {}").format(
            sql.Identifier(SCHEMA),
            sql.Identifier(table_name),
            set_clause,
            where_clause,
        )
        params = [raw[i] for i in changed] + [raw[i] for i in plan["pk_idx"]]
        cur.execute(update_stmt, params)

        log_rows.append((
            batch_id, table_name, key, "U",
            [names[i] for i in changed],
            Jsonb(as_dict(names, old_canon, changed)),
            Jsonb(as_dict(names, new_canon, changed)),
        ))

    if log_rows:
        cur.executemany(LOG_SQL, log_rows)


def apply_deletes(cur, batch_id, table_name, plan):
    if not plan["deletes"]:
        return

    names = plan["names"]
    pk_names = [names[i] for i in plan["pk_idx"]]

    delete_stmt = sql.SQL("DELETE FROM {}.{} WHERE {}").format(
        sql.Identifier(SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(sql.Identifier(c)) for c in pk_names
        ),
    )

    cur.executemany(
        delete_stmt,
        [
            [old_canon[i] for i in plan["pk_idx"]]
            for _, old_canon in plan["deletes"]
        ],
    )

    cur.executemany(
        LOG_SQL,
        [
            (
                batch_id, table_name, key, "D", None,
                Jsonb(as_dict(names, old_canon)), None,
            )
            for key, old_canon in plan["deletes"]
        ],
    )


def sync_audit_and_checkpoint(cur, fingerprint):
    """
    Keep load_audit and 02's checkpoint consistent with the new data, so a
    later normal run of 02 does not think the Silver files changed and
    trigger a needless full reload.
    """
    counts = {}

    for table_name in LOAD_ORDER:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(SCHEMA), sql.Identifier(table_name)
            )
        )
        counts[table_name] = cur.fetchone()[0]

    cur.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.load_audit",))
    if cur.fetchone()[0] is not None:
        for table_name, count in counts.items():
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.load_audit "
                    "(table_name, row_count, loaded_at) "
                    "VALUES (%s, %s, NOW()) "
                    "ON CONFLICT (table_name) DO UPDATE SET "
                    "row_count = EXCLUDED.row_count, loaded_at = NOW()"
                ).format(sql.Identifier(SCHEMA)),
                (table_name, count),
            )

    cur.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.load_checkpoint",))
    if cur.fetchone()[0] is not None:
        for stage, detail in (
            ("source_fingerprint", fingerprint),
            ("tables_loaded", json.dumps(counts)),
        ):
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.load_checkpoint "
                    "(run_key, stage, status, detail, updated_at) "
                    "VALUES (%s, %s, 'SUCCESS', %s, NOW()) "
                    "ON CONFLICT (run_key, stage) DO UPDATE SET "
                    "status = 'SUCCESS', detail = EXCLUDED.detail, "
                    "updated_at = NOW()"
                ).format(sql.Identifier(SCHEMA)),
                (RUN_KEY, stage, detail),
            )


def apply_cdc(conn, frames, batch_id, fingerprint):
    """Detect and apply all changes in ONE transaction."""
    with conn.transaction(force_rollback=DRY_RUN):
        with conn.cursor() as cur:
            plans = {
                table_name: build_plan(cur, table_name, frames[table_name])
                for table_name in LOAD_ORDER
            }

            guard_deletes(plans)

            # Parents first for inserts/updates.
            for table_name in LOAD_ORDER:
                apply_upserts(cur, batch_id, table_name, plans[table_name])

            # Children first for deletes.
            if APPLY_DELETES:
                for table_name in reversed(LOAD_ORDER):
                    apply_deletes(
                        cur, batch_id, table_name, plans[table_name]
                    )

            totals = {
                "inserts": sum(len(p["inserts"]) for p in plans.values()),
                "updates": sum(len(p["updates"]) for p in plans.values()),
                "deletes": (
                    sum(len(p["deletes"]) for p in plans.values())
                    if APPLY_DELETES else 0
                ),
            }

            if not DRY_RUN:
                sync_audit_and_checkpoint(cur, fingerprint)
                cur.execute(
                    sql.SQL(
                        "UPDATE {}.cdc_batch SET status = 'SUCCESS', "
                        "finished_at = NOW(), inserts = %s, updates = %s, "
                        "deletes = %s WHERE batch_id = %s"
                    ).format(sql.Identifier(SCHEMA)),
                    (
                        totals["inserts"], totals["updates"],
                        totals["deletes"], batch_id,
                    ),
                )

    return plans, totals


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def print_summary(plans, totals):
    print("\nCHANGES DETECTED")
    print(f"{'table':24} {'insert':>7} {'update':>7} {'delete':>7} "
          f"{'same':>7}")
    print("-" * 56)

    for table_name in LOAD_ORDER:
        plan = plans[table_name]
        deletes = len(plan["deletes"]) if APPLY_DELETES else 0
        print(
            f"{table_name:24} {len(plan['inserts']):>7} "
            f"{len(plan['updates']):>7} {deletes:>7} "
            f"{plan['unchanged']:>7}"
        )

    print("-" * 56)
    print(
        f"{'TOTAL':24} {totals['inserts']:>7} {totals['updates']:>7} "
        f"{totals['deletes']:>7}"
    )

    if not APPLY_DELETES:
        skipped = sum(len(p["deletes"]) for p in plans.values())
        if skipped:
            print(f"\n(CDC_APPLY_DELETES=0: {skipped} deletes were skipped.)")


def main():
    print("Discovering Silver tables in S3...")
    file_map = list_silver_files()
    fingerprint = compute_fingerprint(file_map)

    print("\nConnecting to PostgreSQL...")
    with connect_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            if not cur.fetchone()[0]:
                raise RuntimeError("Another CDC run is already in progress.")

        require_tables(conn)
        ensure_cdc_tables(conn)

        if not DRY_RUN and not FORCE:
            if last_success_fingerprint(conn) == fingerprint:
                print(
                    "\nNo new Silver files since the last successful CDC "
                    "batch. Nothing to do. (Use --force to compare anyway.)"
                )
                return

        if DRY_RUN:
            print("\nDRY RUN: changes are computed but rolled back.")
            batch_id = 0
        else:
            batch_id = start_batch(conn, fingerprint)
            print(f"\nStarted CDC batch {batch_id}")

        try:
            frames = download_and_validate_all(file_map)
            plans, totals = apply_cdc(conn, frames, batch_id, fingerprint)
        except Exception as exc:
            if not DRY_RUN:
                fail_batch(conn, batch_id, str(exc))
                print(
                    f"\nBatch {batch_id} FAILED and was rolled back. "
                    "No data was changed. Fix the cause and rerun."
                )
            raise

    print_summary(plans, totals)

    if DRY_RUN:
        print("\nDRY RUN finished: nothing was changed.")
    else:
        print(f"\nSUCCESS: CDC batch {batch_id} committed.")
        print(f"Review changes: SELECT * FROM {SCHEMA}.cdc_change_log "
              f"WHERE batch_id = {batch_id};")


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        print(f"\nAPI request failed: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\nCDC loader failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
