"""
Decathlon Silver-to-PostgreSQL OLTP Loader (checkpoint / resume enabled)

- Reads normalized Silver CSVs from the processed S3 API (kept in memory).
- Loads 23 entity tables into PostgreSQL schema: decathlon_oltp.
- Stages (tracked in decathlon_oltp.load_checkpoint):
    1. tables_loaded   - ALL tables dropped/created/loaded in ONE transaction,
                         checkpoint committed in that same transaction.
    2. primary_keys    - each PK added in its own transaction (idempotent).
    3. foreign_keys    - orphan pre-check, then each FK in its own transaction.
    4. audit_written   - load_audit table + row-count verification.
- If the script stops or fails, just run it again: completed stages are
  skipped and execution resumes at the first unfinished stage.
- If the Silver files in S3 change (different size / modified time / etag),
  the checkpoints are reset automatically and a full reload happens.

Run:
    python3 src/02_load_postgres_oltp.py              # normal / resume
    python3 src/02_load_postgres_oltp.py --restart    # ignore checkpoints
    (or FORCE_RELOAD=1 python3 src/02_load_postgres_oltp.py)
"""

import hashlib
import json
import os
import sys
import traceback
from io import BytesIO

import pandas as pd
import psycopg
import requests
from psycopg import sql
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

FORCE_RELOAD = (
    os.getenv("FORCE_RELOAD", "0") == "1" or "--restart" in sys.argv
)

session = requests.Session()
session.headers.update({"User-Agent": "Decathlon-Silver-OLTP-Loader/2.0"})
_retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=None,
)
session.mount("https://", HTTPAdapter(max_retries=_retry))
session.mount("http://", HTTPAdapter(max_retries=_retry))

# Checkpoint stage names
STAGE_FINGERPRINT = "source_fingerprint"
STAGE_LOADED = "tables_loaded"
STAGE_PK = "primary_keys"
STAGE_FK = "foreign_keys"
STAGE_AUDIT = "audit_written"
LATER_STAGES = [STAGE_PK, STAGE_FK, STAGE_AUDIT]


# ---------------------------------------------------------
# TABLE DEFINITIONS
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

# Parent tables must be loaded before their child tables.
LOAD_ORDER = [
    "customers",
    "categories",
    "brands",
    "products",
    "customer_addresses",
    "loyalty_accounts",
    "suppliers",
    "warehouses",
    "campaigns",
    "employees",
    "sales_orders",
    "sales_order_items",
    "payments",
    "shipments",
    "returns",
    "supplier_contracts",
    "inventory_snapshots",
    "purchase_orders",
    "purchase_order_items",
    "service_cases",
    "advertisements",
    "ad_events",
    "risk_cases",
]

FOREIGN_KEYS = [
    ("customer_addresses", "customer_id", "customers", "customer_id"),
    ("loyalty_accounts", "customer_id", "customers", "customer_id"),
    ("sales_orders", "customer_id", "customers", "customer_id"),
    ("sales_order_items", "order_id", "sales_orders", "order_id"),
    ("sales_order_items", "product_id", "products", "product_id"),
    ("payments", "order_id", "sales_orders", "order_id"),
    ("shipments", "order_id", "sales_orders", "order_id"),
    ("returns", "order_id", "sales_orders", "order_id"),
    ("returns", "sales_line_id", "sales_order_items", "sales_line_id"),
    ("returns", "product_id", "products", "product_id"),
    ("service_cases", "customer_id", "customers", "customer_id"),
    ("service_cases", "order_id", "sales_orders", "order_id"),
    ("service_cases", "product_id", "products", "product_id"),
    ("advertisements", "campaign_id", "campaigns", "campaign_id"),
    ("ad_events", "campaign_id", "campaigns", "campaign_id"),
    ("ad_events", "advertisement_id", "advertisements", "advertisement_id"),
    ("ad_events", "customer_id", "customers", "customer_id"),
    ("ad_events", "product_id", "products", "product_id"),
    ("ad_events", "conversion_order_id", "sales_orders", "order_id"),
    ("risk_cases", "customer_id", "customers", "customer_id"),
    ("risk_cases", "order_id", "sales_orders", "order_id"),
    ("risk_cases", "payment_id", "payments", "payment_id"),
    ("supplier_contracts", "supplier_id", "suppliers", "supplier_id"),
    ("supplier_contracts", "warehouse_id", "warehouses", "warehouse_id"),
    ("inventory_snapshots", "product_id", "products", "product_id"),
    ("inventory_snapshots", "warehouse_id", "warehouses", "warehouse_id"),
    ("purchase_orders", "supplier_id", "suppliers", "supplier_id"),
    ("purchase_orders", "warehouse_id", "warehouses", "warehouse_id"),
    ("purchase_order_items", "purchase_order_id", "purchase_orders",
     "purchase_order_id"),
    ("purchase_order_items", "product_id", "products", "product_id"),
]

# Columns that may legitimately be NULL (a NULL never violates an FK).
OPTIONAL_FOREIGN_KEYS = {
    ("service_cases", "order_id"),
    ("service_cases", "product_id"),
    ("risk_cases", "order_id"),
    ("risk_cases", "payment_id"),
    ("returns", "sales_line_id"),
    ("returns", "product_id"),
    ("ad_events", "conversion_order_id"),
}

# All ID columns use TEXT so identifiers stay consistent across tables.
DATE_HINTS = ("date", "timestamp", "_at")
INTEGER_HINTS = ("quantity", "points", "flag", "days", "moq")
DECIMAL_HINTS = (
    "price", "amount", "total", "fee", "tax", "discount", "mrp",
    "budget", "value", "score", "rating", "cost", "stock",
)
BOOLEAN_VALUES = ["0", "1", "True", "False", "true", "false"]


# ---------------------------------------------------------
# NULL HANDLING  (root cause of the 'nan' foreign-key error)
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


def clean_values(series):
    """Non-null, stripped string values of a column."""
    cleaned = [str(v).strip() for v in series if not is_null(v)]
    return pd.Series(cleaned, dtype="object")


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

        # Ignore data_quality_issues.csv and any other non-entity CSVs.
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
            + ". Run 01_extract_transform_normalize.py first."
        )

    print(f"Discovered {len(selected)} expected Silver entity CSVs.")
    return selected


def compute_fingerprint(file_map):
    """Stable hash of the Silver file listing (key/size/modified/etag)."""
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
    """Download a Silver CSV through the API; never written to disk."""
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

    # dtype=str + keep_default_na=False: blanks stay "" (never become the
    # float NaN that was later written as the text 'nan').
    return pd.read_csv(
        BytesIO(csv_response.content),
        dtype=str,
        keep_default_na=False,
    )


# ---------------------------------------------------------
# VALIDATION AND TYPE INFERENCE
# ---------------------------------------------------------

def validate_table(table_name, df):
    if df.empty:
        raise ValueError(f"Table '{table_name}' contains no rows.")

    if df.columns.duplicated().any():
        duplicates = df.columns[df.columns.duplicated()].tolist()
        raise ValueError(
            f"Table '{table_name}' has duplicate columns: {duplicates}"
        )

    if df.columns.isnull().any():
        raise ValueError(f"Table '{table_name}' has a missing column name.")

    primary_key = PRIMARY_KEYS.get(table_name)

    if not primary_key:
        raise ValueError(
            f"No primary key configured for table '{table_name}'."
        )

    missing_columns = [c for c in primary_key if c not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Table '{table_name}' is missing primary-key columns "
            f"{missing_columns}. Available columns: {df.columns.tolist()}"
        )

    for column in primary_key:
        if df[column].map(is_null).any():
            raise ValueError(
                f"Table '{table_name}' contains NULL/blank values in "
                f"primary-key column '{column}'."
            )

    duplicate_mask = df.duplicated(subset=primary_key, keep=False)

    if duplicate_mask.any():
        duplicates = (
            df.loc[duplicate_mask, primary_key]
            .head(10)
            .to_dict(orient="records")
        )
        raise ValueError(
            f"Table '{table_name}' has duplicate primary-key "
            f"combinations for {primary_key}: {duplicates}"
        )

    print(
        f"  Validated {table_name}: "
        f"{len(df)} rows, {len(df.columns)} columns."
    )


def is_identifier(column):
    return (
        column.endswith("_id")
        or column in {
            "sku", "order_number", "contract_number",
            "tracking_number", "service_case_number",
            "risk_case_number", "creative_id",
        }
    )


def postgres_type(column, series):
    # Preserve identifiers as strings, even if they look numeric.
    if is_identifier(column):
        return "TEXT"

    values = clean_values(series)

    if len(values) == 0:
        return "TEXT"

    lower = column.lower()

    if any(hint in lower for hint in DATE_HINTS):
        try:
            parsed = pd.to_datetime(values, errors="coerce", format="mixed")
            if parsed.notna().all():
                has_time = (parsed.dt.normalize() != parsed).any()
                if "timestamp" in lower or lower.endswith("_at") or has_time:
                    return "TIMESTAMP"
                return "DATE"
        except (TypeError, ValueError, AttributeError):
            pass

    if lower.endswith("_flag") and values.isin(BOOLEAN_VALUES).all():
        return "BOOLEAN"

    numeric = pd.to_numeric(values, errors="coerce")

    if numeric.notna().all():
        if any(hint in lower for hint in DECIMAL_HINTS):
            return "NUMERIC"

        if any(hint in lower for hint in INTEGER_HINTS):
            if (numeric % 1 == 0).all():
                return "BIGINT"
            return "NUMERIC"

        if lower.endswith("_count") and (numeric % 1 == 0).all():
            return "BIGINT"

    return "TEXT"


def infer_column_types(df):
    return {column: postgres_type(column, df[column]) for column in df.columns}


def normalize_value(value, pg_type):
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
        # autocommit: transactions are explicit via conn.transaction()
        "autocommit": True,
    }

    if DB_PASSWORD:
        kwargs["password"] = DB_PASSWORD

    return psycopg.connect(**kwargs)


def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(SCHEMA)
            )
        )


def constraint_exists(cur, table_name, constraint_name):
    cur.execute(
        """
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_schema = %s
          AND table_name = %s
          AND constraint_name = %s
        """,
        (SCHEMA, table_name, constraint_name),
    )
    return cur.fetchone() is not None


# ---------------------------------------------------------
# CHECKPOINT HELPERS
# ---------------------------------------------------------

def ensure_checkpoint_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {}.load_checkpoint ("
                "run_key TEXT NOT NULL, "
                "stage TEXT NOT NULL, "
                "status TEXT NOT NULL, "
                "detail TEXT, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                "PRIMARY KEY (run_key, stage))"
            ).format(sql.Identifier(SCHEMA))
        )


def get_stage(conn, stage):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT status, detail FROM {}.load_checkpoint "
                "WHERE run_key = %s AND stage = %s"
            ).format(sql.Identifier(SCHEMA)),
            (RUN_KEY, stage),
        )
        row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def set_stage(conn, stage, status, detail=None):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "INSERT INTO {}.load_checkpoint "
                "(run_key, stage, status, detail, updated_at) "
                "VALUES (%s, %s, %s, %s, NOW()) "
                "ON CONFLICT (run_key, stage) DO UPDATE SET "
                "status = EXCLUDED.status, "
                "detail = EXCLUDED.detail, "
                "updated_at = NOW()"
            ).format(sql.Identifier(SCHEMA)),
            (RUN_KEY, stage, status, detail),
        )


def clear_stages(conn, stages):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "DELETE FROM {}.load_checkpoint "
                "WHERE run_key = %s AND stage = ANY(%s)"
            ).format(sql.Identifier(SCHEMA)),
            (RUN_KEY, list(stages)),
        )


def reset_checkpoints(conn):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "DELETE FROM {}.load_checkpoint WHERE run_key = %s"
            ).format(sql.Identifier(SCHEMA)),
            (RUN_KEY,),
        )


def run_stage(conn, stage, func):
    """Run func() unless the stage is already SUCCESS; record the result."""
    status, _ = get_stage(conn, stage)

    if status == "SUCCESS":
        print(f"[checkpoint] '{stage}' already done - skipping.")
        return

    print(f"[stage] Running '{stage}'...")

    try:
        func()
    except Exception as exc:
        set_stage(conn, stage, "FAILED", str(exc)[:2000])
        raise

    set_stage(conn, stage, "SUCCESS")
    print(f"[stage] '{stage}' completed.")


# ---------------------------------------------------------
# STAGE 1: DOWNLOAD + LOAD (ONE TRANSACTION)
# ---------------------------------------------------------

def download_and_validate_all(file_map):
    frames = {}

    for table_name in LOAD_ORDER:
        print(f"Downloading and validating: {table_name}")
        df = download_csv_to_memory(file_map[table_name]["path"])
        validate_table(table_name, df)
        frames[table_name] = df

    return frames


def create_table(cur, table_name, column_types):
    column_defs = [
        sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(pg_type))
        for column, pg_type in column_types.items()
    ]

    cur.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(SCHEMA),
            sql.Identifier(table_name),
            sql.SQL(", ").join(column_defs),
        )
    )


def insert_rows(cur, table_name, df, column_types):
    columns = list(df.columns)

    insert_stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )

    rows = [
        tuple(
            normalize_value(record[column], column_types[column])
            for column in columns
        )
        for record in df.to_dict(orient="records")
    ]

    if rows:
        cur.executemany(insert_stmt, rows)

    return len(rows)


def load_all_tables(conn, frames, fingerprint):
    """
    Drop/create/load every table AND write the checkpoint inside ONE
    transaction. Either everything (data + checkpoint) is committed, or
    nothing is, so a restart never sees a half-loaded database.
    """
    row_counts = {}

    with conn.transaction():
        with conn.cursor() as cur:
            # Clean rebuild: removes old PK/FK constraints and stale types.
            for table_name in reversed(LOAD_ORDER):
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                        sql.Identifier(SCHEMA),
                        sql.Identifier(table_name),
                    )
                )

            for table_name in LOAD_ORDER:
                df = frames[table_name]
                column_types = infer_column_types(df)
                create_table(cur, table_name, column_types)
                count = insert_rows(cur, table_name, df, column_types)
                row_counts[table_name] = count
                print(f"Loaded {table_name}: {count} rows")

        # Checkpoint written inside the same transaction.
        clear_stages(conn, LATER_STAGES)
        set_stage(conn, STAGE_FINGERPRINT, "SUCCESS", fingerprint)
        set_stage(conn, STAGE_LOADED, "SUCCESS", json.dumps(row_counts))

    return row_counts


# ---------------------------------------------------------
# STAGE 2: PRIMARY KEYS
# ---------------------------------------------------------

def add_primary_keys(conn):
    for table_name, columns in PRIMARY_KEYS.items():
        constraint_name = f"pk_{table_name}"

        with conn.cursor() as cur:
            if constraint_exists(cur, table_name, constraint_name):
                continue

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {}.{} ADD CONSTRAINT {} "
                        "PRIMARY KEY ({})"
                    ).format(
                        sql.Identifier(SCHEMA),
                        sql.Identifier(table_name),
                        sql.Identifier(constraint_name),
                        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                    )
                )

        print(f"  Added {constraint_name}")


# ---------------------------------------------------------
# STAGE 3: FOREIGN KEYS
# ---------------------------------------------------------

def count_orphans(cur, child, child_col, parent, parent_col):
    cur.execute(
        sql.SQL(
            "SELECT COUNT(*) FROM {s}.{c} AS c "
            "WHERE c.{cc} IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM {s}.{p} AS p "
            "WHERE p.{pc} = c.{cc})"
        ).format(
            s=sql.Identifier(SCHEMA),
            c=sql.Identifier(child),
            cc=sql.Identifier(child_col),
            p=sql.Identifier(parent),
            pc=sql.Identifier(parent_col),
        )
    )
    return cur.fetchone()[0]


def add_foreign_keys(conn):
    pending = []

    with conn.cursor() as cur:
        for child, child_col, parent, parent_col in FOREIGN_KEYS:
            name = f"fk_{child}_{child_col}"
            if not constraint_exists(cur, child, name):
                pending.append((name, child, child_col, parent, parent_col))

        # Report ALL orphan problems at once, before changing anything.
        problems = []
        for name, child, child_col, parent, parent_col in pending:
            orphans = count_orphans(cur, child, child_col, parent, parent_col)
            if orphans:
                kind = (
                    "optional"
                    if (child, child_col) in OPTIONAL_FOREIGN_KEYS
                    else "required"
                )
                problems.append(
                    f"{child}.{child_col} -> {parent}.{parent_col}: "
                    f"{orphans} orphan row(s) ({kind} FK)"
                )

    if problems:
        raise RuntimeError(
            "Foreign-key data problems found (nothing was altered):\n  "
            + "\n  ".join(problems)
        )

    for name, child, child_col, parent, parent_col in pending:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {}.{} ADD CONSTRAINT {} "
                        "FOREIGN KEY ({}) REFERENCES {}.{} ({})"
                    ).format(
                        sql.Identifier(SCHEMA),
                        sql.Identifier(child),
                        sql.Identifier(name),
                        sql.Identifier(child_col),
                        sql.Identifier(SCHEMA),
                        sql.Identifier(parent),
                        sql.Identifier(parent_col),
                    )
                )
        print(f"  Added {name}")


# ---------------------------------------------------------
# STAGE 4: AUDIT
# ---------------------------------------------------------

def write_audit(conn, row_counts):
    with conn.cursor() as cur:
        # Verify actual table counts match what was loaded.
        for table_name, expected in row_counts.items():
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(SCHEMA), sql.Identifier(table_name)
                )
            )
            actual = cur.fetchone()[0]
            if actual != expected:
                raise RuntimeError(
                    f"Row-count mismatch in {table_name}: "
                    f"loaded {expected}, found {actual}."
                )

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {}.load_audit ("
                    "table_name TEXT PRIMARY KEY, "
                    "row_count BIGINT NOT NULL, "
                    "loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
                ).format(sql.Identifier(SCHEMA))
            )

            for table_name, count in row_counts.items():
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {}.load_audit "
                        "(table_name, row_count, loaded_at) "
                        "VALUES (%s, %s, NOW()) "
                        "ON CONFLICT (table_name) DO UPDATE SET "
                        "row_count = EXCLUDED.row_count, "
                        "loaded_at = NOW()"
                    ).format(sql.Identifier(SCHEMA)),
                    (table_name, count),
                )


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    print("Discovering Silver tables in S3...")
    file_map = list_silver_files()
    fingerprint = compute_fingerprint(file_map)

    print("\nConnecting to PostgreSQL...")
    with connect_postgres() as conn:
        create_schema(conn)
        ensure_checkpoint_table(conn)

        # Decide whether existing checkpoints can be trusted.
        if FORCE_RELOAD:
            print("[checkpoint] --restart requested: resetting checkpoints.")
            reset_checkpoints(conn)
        else:
            _, stored_fp = get_stage(conn, STAGE_FINGERPRINT)
            if stored_fp and stored_fp != fingerprint:
                print(
                    "[checkpoint] Silver files changed since the last load: "
                    "resetting checkpoints for a full reload."
                )
                reset_checkpoints(conn)

        # ---- Stage 1: load all tables (one transaction) ----
        status, detail = get_stage(conn, STAGE_LOADED)

        if status == "SUCCESS":
            row_counts = json.loads(detail)
            print(
                "[checkpoint] 'tables_loaded' already committed "
                f"({len(row_counts)} tables) - skipping download and load."
            )
        else:
            print("[stage] Running 'tables_loaded'...")
            try:
                frames = download_and_validate_all(file_map)
                row_counts = load_all_tables(conn, frames, fingerprint)
            except Exception as exc:
                # The load transaction was rolled back automatically.
                set_stage(conn, STAGE_LOADED, "FAILED", str(exc)[:2000])
                raise
            print("[stage] 'tables_loaded' committed.")

        # ---- Stage 2-4: resumable, each skipped when already done ----
        run_stage(conn, STAGE_PK, lambda: add_primary_keys(conn))
        run_stage(conn, STAGE_FK, lambda: add_foreign_keys(conn))
        run_stage(conn, STAGE_AUDIT, lambda: write_audit(conn, row_counts))

    print("\nSUCCESS: Silver snapshot loaded into PostgreSQL.")
    print(f"Database: {DATABASE}")
    print(f"Schema: {SCHEMA}")
    print(f"Entity tables loaded: {len(row_counts)}")
    print(f"Total rows loaded: {sum(row_counts.values())}")


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        print(f"\nAPI request failed: {exc}", file=sys.stderr)
        print("Re-run the script to resume from the last checkpoint.")
        sys.exit(1)
    except Exception as exc:
        print(f"\nLoader failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        print("\nRe-run the script to resume from the last checkpoint.")
        sys.exit(1)
