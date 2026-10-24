

"""

Decathlon Raw-to-Silver Pipeline

================================

Run from project root:

    python3 src/01_extract_transform_normalize.py

Dependencies:

    pip install pandas requests

API:

    GET  /files

    GET  /download

    POST /upload-url

    PUT  <presigned-upload-url>

    GET  /processed-files

Default scope:

    Raw:    Decathlon/2025/01/01/

    Silver: Decathlon/silver/2025/01/01/

Set SOURCE_PREFIX and PROCESSED_PREFIX before running if required.

"""

import io

import os

import re

import sys

from pathlib import PurePosixPath

import pandas as pd

import requests
import traceback

# ============================================================

# 1. CONFIGURATION

# ============================================================

BASE_URL = (

    "https://zucau19vp0.execute-api.us-east-1.amazonaws.com"

)

PROJECT = "Decathlon"

# Start with one day. Expand to 2025/ only after reviewing

# how repeated master records should be reconciled across dates.

SOURCE_PREFIX = os.getenv("SOURCE_PREFIX", "2025/01/01/")

PROCESSED_PREFIX = os.getenv(

    "PROCESSED_PREFIX", "silver/2025/01/01/"

)

TIMEOUT = 60

UPLOAD_TIMEOUT = 120

# PII is removed from Silver outputs by default.

# Source data is never overwritten.

PII_POLICY = os.getenv("PII_POLICY", "remove").lower()

if PII_POLICY not in {"remove", "review"}:

    raise ValueError("PII_POLICY must be 'remove' or 'review'.")

session = requests.Session()

SOURCE_FILES = {

    "after_sales": "after_sales_and_service_managemen",

    "corporate": "corporate_management",

    "customer": "customer_management",

    "marketing": "marketing_management",

    "product": "product_management",

    "risk": "risk_security_and_fraud_manageme",

    "sales": "sales_and_commerce_management",

    "supply_chain": "supply_chain_management",

}

EXPECTED_SOURCES = set(SOURCE_FILES)

# ============================================================

# 2. NORMALIZED ENTITY DEFINITIONS

# ============================================================

ENTITY_SPECS = {

    "customers": (

        "customer", "customer_id",

        [

            "customer_id", "customer_first_name", "customer_last_name",

            "customer_email", "customer_phone", "registration_date",

            "customer_status", "preferred_language", "customer_country",

            "customer_city", "customer_state",

        ],

    ),

    "customer_addresses": (

        "customer", "address_id",

        [

            "address_id", "customer_id", "address_type", "city", "state",

            "country", "postal_code",

        ],

    ),

    "loyalty_accounts": (

        "customer", "loyalty_account_id",

        [

            "loyalty_account_id", "customer_id", "loyalty_tier",

            "loyalty_points",

        ],

    ),

    "products": (

        "product", "product_id",

        [

            "product_id", "sku", "product_name", "product_description",

            "brand_id", "category_id", "sub_category_id", "sport_id",

            "product_type", "product_status", "gender", "age_group",

            "size", "color", "material", "selling_price", "mrp", "currency",

        ],

    ),

    "categories": (

        "product", "category_id",

        ["category_id", "category_name"],

    ),

    "brands": (

        "product", "brand_id",

        ["brand_id", "brand_name"],

    ),

    "sales_orders": (

        "sales", "order_id",

        [

            "order_id", "order_number", "customer_id", "order_date",

            "order_status", "sales_channel", "store_id", "currency",

            "order_subtotal", "shipping_fee", "order_tax_amount", "order_total",

        ],

    ),

    "sales_order_items": (

        "sales", "sales_line_id",

        [

            "sales_line_id", "order_id", "product_id", "sku", "quantity",

            "unit_price", "gross_line_amount", "discount_amount",

            "tax_amount", "net_line_amount",

        ],

    ),

    "payments": (

        "sales", "payment_id",

        [

            "payment_id", "order_id", "payment_method", "payment_gateway",

            "payment_amount", "payment_status", "payment_date", "currency",

        ],

    ),

    "shipments": (

        "sales", "shipment_id",

        [

            "shipment_id", "order_id", "carrier_name", "tracking_number",

            "shipment_status", "dispatch_date", "expected_delivery_date",

            "actual_delivery_date",

        ],

    ),

    "returns": (

        "sales", "return_id",

        [

            "return_id", "order_id", "sales_line_id", "product_id",

            "return_reason", "return_status", "return_date", "refund_amount",

        ],

    ),

    "suppliers": (

        "supply_chain", "supplier_id",

        [

            "supplier_id", "supplier_name", "supplier_type",

            "supplier_status", "supplier_country", "supplier_rating",

            "supplier_lead_days", "supplier_moq", "supplier_risk_score",

            "supplier_compliance_status",

        ],

    ),

    "warehouses": (

        "supply_chain", "warehouse_id",

        [

            "warehouse_id", "warehouse_name", "warehouse_city",

            "warehouse_state",

        ],

    ),

    "supplier_contracts": (

        "supply_chain", "contract_id",

        ["contract_id", "contract_number", "supplier_id", "warehouse_id"],

    ),

    "inventory_snapshots": (

        "supply_chain", "inventory_id",

        [

            "inventory_id", "product_id", "warehouse_id", "stock_on_hand",

            "reserved_stock", "available_stock", "damaged_stock",

            "in_transit_stock", "inventory_status", "last_stock_update",

        ],

    ),

    "purchase_orders": (

        "supply_chain", "purchase_order_id",

        [

            "purchase_order_id", "supplier_id", "warehouse_id",

            "purchase_order_date", "purchase_order_status",

            "expected_receipt_date", "purchase_order_currency",

            "purchase_order_total",

        ],

    ),

"purchase_order_items": (
    "supply_chain",
    "purchase_order_item_id",
    [
        "purchase_order_item_id",
        "purchase_order_id",
        "product_id",
        "quantity_ordered",
        "quantity_received",
        "unit_cost",
        "line_amount",
    ],
),

    "service_cases": (

        "after_sales", "service_case_id",

        [

            "service_case_id", "service_case_number", "customer_id", "order_id",

            "product_id", "service_type", "issue_type", "case_open_date",

            "case_priority", "case_status", "resolution_date",

        ],

    ),

    "campaigns": (

        "marketing", "campaign_id",

        [

            "campaign_id", "campaign_name", "campaign_type",

            "campaign_objective", "campaign_status", "campaign_start_date",

            "campaign_end_date", "campaign_budget",

        ],

    ),

    "advertisements": (

        "marketing", "advertisement_id",

        [

            "advertisement_id", "campaign_id", "advertisement_name",

            "advertisement_channel", "advertisement_format",

            "advertisement_status", "ad_platform", "creative_id",

        ],

    ),

    "ad_events": (

        "marketing", "ad_event_id",

        [

            "ad_event_id", "campaign_id", "advertisement_id", "customer_id",

            "product_id", "event_type", "event_timestamp", "impression_flag",

            "click_flag", "conversion_flag", "conversion_order_id",

            "conversion_value",

        ],

    ),

    "risk_cases": (

        "risk", "risk_case_id",

        [

            "risk_case_id", "risk_case_number", "customer_id", "order_id",

            "payment_id", "risk_score", "risk_level", "risk_decision",

            "fraud_flag", "investigation_status",

        ],

    ),

    "employees": (

        "corporate", "employee_id",

        [

            "employee_id", "employee_name", "employee_email", "department_id",

            "role_id", "manager_id", "hire_date", "employee_status",

            "employment_type", "store_id",

        ],

    ),

}

# Source-to-target aliases for known naming differences.
# Keys are source column names; values are normalized target names.
COLUMN_ALIASES = {
    "shipments": {
        "carrier": "carrier_name",
        "shipment_date": "dispatch_date",
        "delivery_date": "actual_delivery_date",
    },
    "purchase_orders": {
        "order_date": "purchase_order_date",
        "order_status": "purchase_order_status",
        "currency": "purchase_order_currency",
        "total_amount": "purchase_order_total",
    },
        "purchase_order_items": {
        "ordered_quantity": "quantity_ordered",
        "received_quantity": "quantity_received",
    },
}

REQUIRED_FIELDS = {

    "customers": ["customer_id"],

    "customer_addresses": ["address_id", "customer_id"],

    "loyalty_accounts": ["loyalty_account_id", "customer_id"],

    "products": ["product_id"],

    "categories": ["category_id"],

    "brands": ["brand_id"],

    "sales_orders": ["order_id", "customer_id"],

    "sales_order_items": ["sales_line_id", "order_id", "product_id"],

    "payments": ["payment_id", "order_id"],

    "shipments": ["shipment_id", "order_id"],

    "returns": ["return_id", "order_id"],

    "suppliers": ["supplier_id", "supplier_name"],

    "warehouses": ["warehouse_id", "warehouse_name"],

    "supplier_contracts": ["contract_id", "supplier_id", "warehouse_id"],

    "inventory_snapshots": ["inventory_id", "product_id", "warehouse_id"],

    "purchase_orders": ["purchase_order_id", "supplier_id", "warehouse_id"],

    "purchase_order_items": [

        "purchase_order_item_id", "purchase_order_id", "product_id"

    ],

    "service_cases": ["service_case_id"],

    "campaigns": ["campaign_id"],

    "advertisements": ["advertisement_id"],

    "ad_events": ["ad_event_id"],

    "risk_cases": ["risk_case_id"],

    "employees": ["employee_id"],

}

FOREIGN_KEYS = {

    "customer_addresses": {

        "customer_id": ("customers", "customer_id"),

    },

    "loyalty_accounts": {

        "customer_id": ("customers", "customer_id"),

    },

    "sales_orders": {

        "customer_id": ("customers", "customer_id"),

    },

    "sales_order_items": {

        "order_id": ("sales_orders", "order_id"),

        "product_id": ("products", "product_id"),

    },

    "payments": {

        "order_id": ("sales_orders", "order_id"),

    },

    "shipments": {

        "order_id": ("sales_orders", "order_id"),

    },

    "returns": {

        "order_id": ("sales_orders", "order_id"),

        "sales_line_id": ("sales_order_items", "sales_line_id"),

        "product_id": ("products", "product_id"),

    },

    "service_cases": {

        "customer_id": ("customers", "customer_id"),

        "order_id": ("sales_orders", "order_id"),

        "product_id": ("products", "product_id"),

    },

    "advertisements": {

        "campaign_id": ("campaigns", "campaign_id"),

    },

    "ad_events": {

        "campaign_id": ("campaigns", "campaign_id"),

        "advertisement_id": ("advertisements", "advertisement_id"),

        "customer_id": ("customers", "customer_id"),

        "product_id": ("products", "product_id"),

        "conversion_order_id": ("sales_orders", "order_id"),

    },

    "risk_cases": {

        "customer_id": ("customers", "customer_id"),

        "order_id": ("sales_orders", "order_id"),

        "payment_id": ("payments", "payment_id"),

    },

    "supplier_contracts": {

        "supplier_id": ("suppliers", "supplier_id"),

        "warehouse_id": ("warehouses", "warehouse_id"),

    },

    "inventory_snapshots": {

        "product_id": ("products", "product_id"),

        "warehouse_id": ("warehouses", "warehouse_id"),

    },

    "purchase_orders": {

        "supplier_id": ("suppliers", "supplier_id"),

        "warehouse_id": ("warehouses", "warehouse_id"),

    },

    
    
    "purchase_order_items": {
        "purchase_order_id": ("purchase_orders", "purchase_order_id"),
        "product_id": ("products", "product_id"),
    },



}

OPTIONAL_FOREIGN_KEYS = {

    ("ad_events", "conversion_order_id"),

    ("service_cases", "order_id"),

    ("service_cases", "product_id"),

    ("risk_cases", "order_id"),

    ("risk_cases", "payment_id"),

    ("returns", "sales_line_id"),

    ("returns", "product_id"),

}

# ============================================================

# 3. API HELPERS

# ============================================================

def api_get(endpoint, params):

    response = session.get(

        f"{BASE_URL}{endpoint}",

        params=params,

        timeout=TIMEOUT,

    )

    response.raise_for_status()

    return response.json()

def api_list_raw_files():

    result = api_get(

        "/files",

        {"project": PROJECT, "path": SOURCE_PREFIX},

    )

    files = result.get("files", [])

    if not isinstance(files, list):

        raise ValueError("GET /files did not return a files list.")

    return files

def get_object_key(item):

    if isinstance(item, str):

        key = item

    elif isinstance(item, dict):

        key = item.get("key") or item.get("path") or item.get("name")

    else:

        return None

    if not key:

        return None

    key = str(key).lstrip("/")

    prefix = f"{PROJECT}/"

    if key.startswith(prefix):

        key = key[len(prefix):]

    # A filename-only response is interpreted relative to SOURCE_PREFIX.

    if "/" not in key:

        key = f"{SOURCE_PREFIX.rstrip('/')}/{key}"

    return key

def download_csv(object_key):

    result = api_get(

        "/download",

        {"project": PROJECT, "path": object_key},

    )

    url = result.get("download_url")

    if not url:

        raise ValueError(

            f"GET /download did not return download_url for {object_key}"

        )

    response = session.get(url, timeout=TIMEOUT)

    response.raise_for_status()

    return pd.read_csv(

        io.BytesIO(response.content),

        dtype=str,

        keep_default_na=False,

    )


def upload_csv_to_s3(df, filename):
    """
    Upload using the documented API:
        POST /upload-url
        PUT <presigned URL>
    """

    folder = PROCESSED_PREFIX.strip("/")
    object_path = f"{folder}/{filename}" if folder else filename

    payload = {
        "project": PROJECT,
        "path": object_path,
    }

    # Step 1: Request a presigned URL.
    response = session.post(
        f"{BASE_URL}/upload-url",
        json=payload,
        timeout=TIMEOUT,
    )

    # Check the POST response, not put_response.
    if not response.ok:
        print("\nUPLOAD URL REQUEST FAILED")
        print("HTTP status:", response.status_code)
        print("Response:", response.text[:2000])
        response.raise_for_status()

    result = response.json()
    upload_url = result.get("upload_url")

    if not upload_url:
        raise ValueError(
            "POST /upload-url did not return upload_url."
        )

    # Step 2: Serialize the DataFrame to CSV.
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    data = buffer.getvalue().encode("utf-8")

    # Step 3: Upload the CSV to S3 using the presigned URL.
    try:
        put_response = session.put(
            upload_url,
            data=data,
            timeout=UPLOAD_TIMEOUT,
        )
    except requests.RequestException as exc:
        print(f"Upload request failed for {filename}: {exc}")
        raise

    if not put_response.ok:
        print("\nS3 UPLOAD ERROR")
        print("HTTP status:", put_response.status_code)
        print("S3 response:", put_response.text[:2000])
        put_response.raise_for_status()

    print(
        f"  UPLOADED {filename}: rows={len(df)}, "
        f"bytes={len(data)}, "
        f"HTTP={put_response.status_code}"
    )

# ============================================================

# 4. STANDARDIZATION

# ============================================================

def clean_column_name(column):

    value = str(column).strip().lower()

    value = re.sub(r"[^a-z0-9]+", "_", value)

    return value.strip("_")

def transform_dataframe(df):

    df = df.copy()

    df.columns = [clean_column_name(c) for c in df.columns]

    if df.columns.duplicated().any():

        duplicates = df.columns[df.columns.duplicated()].tolist()

        raise ValueError(f"Duplicate standardized columns: {duplicates}")

    for column in df.columns:

        df[column] = df[column].map(

            lambda x: x.strip() if isinstance(x, str) else x

        )

    df = df.replace(r"^\s*$", pd.NA, regex=True)

    return df

def identify_source(filename):

    stem = clean_column_name(PurePosixPath(filename).stem)

    matches = [

        name for name, token in SOURCE_FILES.items()

        if token in stem

    ]

    if len(matches) == 1:

        return matches[0]

    # Explicit fallbacks for the real filenames in the API guide.

    if "after_sales" in stem or "service_managemen" in stem:

        return "after_sales"

    if "corporate" in stem:

        return "corporate"

    if "customer" in stem:

        return "customer"

    if "marketing" in stem:

        return "marketing"

    if "product" in stem:

        return "product"

    if "risk" in stem or "fraud" in stem:

        return "risk"

    if "sales" in stem or "commerce" in stem:

        return "sales"

    if "supply" in stem or "chain" in stem:

        return "supply_chain"

    return None

# ============================================================

# 5. QUALITY ISSUE HELPERS

# ============================================================

def add_issue(issues, table, rule, severity, details, source_file=""):

    issues.append({

        "source_file": source_file,

        "table_name": table,

        "rule": rule,

        "severity": severity,

        "details": str(details),

    })

def nonblank(series):

    values = series.astype("string").str.strip()

    return values.notna() & values.ne("")

def numeric_values(df, column):

    return pd.to_numeric(df[column], errors="coerce")

# ============================================================

# 6. BUILD CANDIDATE TABLES

# ============================================================


def build_entity_tables(source_name, df, issues, filename):
    result = {}

    for table_name, spec in ENTITY_SPECS.items():
        if not isinstance(spec, tuple) or len(spec) != 3:
            raise ValueError(
                f"Invalid ENTITY_SPECS entry: {table_name!r}. "
                f"Expected (source_name, primary_key, columns), got: {spec!r}"
            )

        expected_source, key, configured_columns = spec

        if expected_source != source_name:
            continue

        entity_source = df.copy()

        # Keep only rows that contain return-related information.
        
        # Returns: retain only records with a valid primary key.
        # Rows with return details but no return_id are not loaded.
        # They must be captured by data-quality validation separately.
        if table_name == "returns":
            if "return_id" not in entity_source.columns:
                raise ValueError(
                    "The source CSV is missing the required return_id column."
                )

            has_return_id = nonblank(entity_source["return_id"])

            entity_source = entity_source.loc[
                has_return_id
            ].copy()


            

                

        # Rename source columns to match the target schema.
        aliases = COLUMN_ALIASES.get(table_name, {})

        rename_map = {
            source_col: target_col
            for source_col, target_col in aliases.items()
            if source_col in entity_source.columns
            and target_col not in entity_source.columns
        }

        if rename_map:
            entity_source = entity_source.rename(columns=rename_map)

        # Derive a purchase-order item key only if the composite key is unique.
        if (
            table_name == "purchase_order_items"
            and key not in entity_source.columns
        ):
            pair_columns = ["purchase_order_id", "product_id"]

            if not set(pair_columns).issubset(entity_source.columns):
                add_issue(
                    issues,
                    table_name,
                    "SOURCE_KEY_MISSING",
                    "ERROR",
                    "Cannot derive purchase_order_item_id: "
                    "purchase_order_id and product_id are not both present.",
                    filename,
                )
                continue

            pair = entity_source[pair_columns].astype("string").apply(
                lambda col: col.str.strip()
            )

            pair_populated = (
                pair.notna().all(axis=1)
                & pair.ne("").all(axis=1)
            )

            pair_keys = (
                pair["purchase_order_id"]
                + "::"
                + pair["product_id"]
            )

            duplicate_pairs = (
                pair_populated
                & pair_keys.duplicated(keep=False)
            )

            if duplicate_pairs.any():
                add_issue(
                    issues,
                    table_name,
                    "COMPOSITE_KEY_NOT_UNIQUE",
                    "ERROR",
                    f"{int(duplicate_pairs.sum())} rows repeat the same "
                    "(purchase_order_id, product_id); item key was not fabricated.",
                    filename,
                )
                continue

            entity_source[key] = pair_keys.where(
                pair_populated, pd.NA
            )

        if key not in entity_source.columns:
            add_issue(
                issues,
                table_name,
                "SOURCE_KEY_MISSING",
                "ERROR",
                f"Required key '{key}' is absent from source schema.",
                filename,
            )
            continue

        available = [
            column
            for column in configured_columns
            if column in entity_source.columns
        ]

        derived_columns = {
    "purchase_order_items": {"line_amount"},
}

        missing = [
            column
            for column in configured_columns
            if column not in entity_source.columns
            and column not in derived_columns.get(table_name, set())
        ]

        if missing:
            add_issue(
                issues,
                table_name,
                "SCHEMA_MISSING_COLUMN",
                "WARNING",
                f"Configured source columns not present: {missing}",
                filename,
            )

        if key not in available:
            available.insert(0, key)

        entity = entity_source[available].copy()

        entity[key] = (
            entity[key]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

        # Calculate line_amount when the required values are available.
        if (
            table_name == "purchase_order_items"
            and "line_amount" not in entity.columns
        ):
            if {"quantity_ordered", "unit_cost"}.issubset(entity.columns):
                quantity = pd.to_numeric(
                    entity["quantity_ordered"], errors="coerce"
                )
                unit_cost = pd.to_numeric(
                    entity["unit_cost"], errors="coerce"
                )

                entity["line_amount"] = (quantity * unit_cost).round(2)

        # Remove rows with missing primary keys and report them.
        null_key = (
            entity[key].isna()
            | entity[key].astype("string").str.strip().eq("")
        )

        if null_key.any():
            add_issue(
                issues,
                table_name,
                "PK_NULL",
                "ERROR",
                f"{int(null_key.sum())} rows have missing {key}.",
                filename,
            )

            entity = entity.loc[~null_key].copy()

        # Master tables: report conflicting attributes for the same key.
        master_tables = {
            "suppliers",
            "warehouses",
            "customers",
            "products",
            "categories",
            "brands",
        }

        if table_name in master_tables:
            attributes = [
                column for column in entity.columns
                if column != key
            ]

            if attributes and not entity.empty:
                counts = entity.groupby(key)[attributes].nunique(
                    dropna=False
                )

                conflicts = counts.index[
                    counts.gt(1).any(axis=1)
                ]

                if len(conflicts):
                    add_issue(
                        issues,
                        table_name,
                        "MASTER_ATTRIBUTE_CONFLICT",
                        "ERROR",
                        f"{len(conflicts)} keys have conflicting attributes.",
                        filename,
                    )

                    entity = entity[
                        ~entity[key].isin(conflicts)
                    ].copy()

            entity = entity.drop_duplicates(subset=[key])

        else:
            entity = entity.drop_duplicates()

            duplicate_mask = entity.duplicated(
                subset=[key], keep=False
            )

            if duplicate_mask.any():
                add_issue(
                    issues,
                    table_name,
                    "PK_DUPLICATE",
                    "ERROR",
                    f"{int(duplicate_mask.sum())} rows share a primary key. "
                    "Conflicting rows were excluded.",
                    filename,
                )

                entity = entity.loc[~duplicate_mask].copy()

        result[table_name] = entity

    return result


def validate_required_fields(tables, issues):

    for table_name, columns in REQUIRED_FIELDS.items():

        if table_name not in tables:

            continue

        df = tables[table_name]

        for column in columns:

            if column not in df.columns:

                add_issue(

                    issues, table_name, "REQUIRED_COLUMN_MISSING", "ERROR",

                    f"Required column '{column}' is absent.",

                )

                continue

            missing = ~nonblank(df[column])

            if missing.any():

                add_issue(

                    issues, table_name, "REQUIRED_VALUE_MISSING", "ERROR",

                    f"{int(missing.sum())} rows have a blank {column}.",

                )

def validate_foreign_keys(tables, issues):

    for child_table, relationships in FOREIGN_KEYS.items():

        if child_table not in tables:

            continue

        child_df = tables[child_table]

        for child_column, (parent_table, parent_column) in (

            relationships.items()

        ):

            if child_column not in child_df.columns:

                add_issue(

                    issues, child_table, "FK_COLUMN_MISSING", "ERROR",

                    f"Missing relationship column '{child_column}'.",

                )

                continue

            if parent_table not in tables:

                add_issue(

                    issues, child_table, "FK_PARENT_TABLE_MISSING", "ERROR",

                    f"Parent table '{parent_table}' was not created.",

                )

                continue

            parent_df = tables[parent_table]

            if parent_column not in parent_df.columns:

                add_issue(

                    issues, child_table, "FK_PARENT_KEY_MISSING", "ERROR",

                    f"Missing parent key {parent_table}.{parent_column}.",

                )

                continue

            values = child_df[child_column].astype("string").str.strip()

            parent_values = set(

                parent_df[parent_column].dropna().astype("string").str.strip()

            )

            populated = values.notna() & values.ne("")

            optional = (child_table, child_column) in OPTIONAL_FOREIGN_KEYS

            if not optional:

                missing = ~populated

                if missing.any():

                    add_issue(

                        issues, child_table, "FK_NULL", "ERROR",

                        f"{int(missing.sum())} rows have a blank "

                        f"required foreign key '{child_column}'.",

                    )

            orphan = populated & ~values.isin(parent_values)

            if orphan.any():

                add_issue(

                    issues, child_table, "FK_ORPHAN", "ERROR",

                    f"{int(orphan.sum())} values in '{child_column}' "

                    f"do not exist in {parent_table}.{parent_column}.",

                )

def validate_numeric_columns(tables, issues):

    numeric_rules = {

        "sales_order_items": [

            "quantity", "unit_price", "gross_line_amount",

            "discount_amount", "tax_amount", "net_line_amount",

        ],

        "sales_orders": [

            "order_subtotal", "shipping_fee",

            "order_tax_amount", "order_total",

        ],

        "payments": ["payment_amount"],

        "inventory_snapshots": [

            "stock_on_hand", "reserved_stock", "available_stock",

            "damaged_stock", "in_transit_stock",

        ],

        "purchase_orders": ["purchase_order_total"],

        "purchase_order_items": [

            "quantity_ordered", "quantity_received", "unit_cost", "line_amount",

        ],

    }

    for table_name, columns in numeric_rules.items():

        if table_name not in tables:

            continue

        df = tables[table_name]

        for column in columns:

            if column not in df.columns:

                continue

            numbers = numeric_values(df, column)

            invalid = nonblank(df[column]) & numbers.isna()

            if invalid.any():

                add_issue(

                    issues, table_name, "INVALID_NUMERIC", "ERROR",

                    f"{int(invalid.sum())} nonnumeric values in {column}.",

                )

            if (numbers.dropna() < 0).any():

                add_issue(

                    issues, table_name, "NEGATIVE_VALUE", "ERROR",

                    f"Column '{column}' contains negative values.",

                )

def validate_business_rules(tables, issues):

    # Sales line: quantity x unit price = gross.

    if "sales_order_items" in tables:

        df = tables["sales_order_items"]

        required = {"quantity", "unit_price", "gross_line_amount"}

        if required.issubset(df.columns):

            expected = (

                numeric_values(df, "quantity")

                * numeric_values(df, "unit_price")

            )

            actual = numeric_values(df, "gross_line_amount")

            mismatch = (

                expected.notna() & actual.notna()

                & ((expected - actual).abs() > 0.02)

            )

            if mismatch.any():

                add_issue(

                    issues, "sales_order_items", "GROSS_AMOUNT_MISMATCH",

                    "ERROR",

                    f"{int(mismatch.sum())} rows fail quantity x unit_price "

                    "= gross_line_amount.",

                )

        required = {"gross_line_amount", "discount_amount", "net_line_amount"}

        if required.issubset(df.columns):

            expected = (

                numeric_values(df, "gross_line_amount")

                - numeric_values(df, "discount_amount")

            )

            actual = numeric_values(df, "net_line_amount")

            mismatch = (

                expected.notna() & actual.notna()

                & ((expected - actual).abs() > 0.02)

            )

            if mismatch.any():

                add_issue(

                    issues, "sales_order_items", "NET_AMOUNT_MISMATCH",

                    "ERROR",

                    f"{int(mismatch.sum())} rows fail gross - discount = net.",

                )

    # Order total = subtotal + shipping + tax.

    if "sales_orders" in tables:

        df = tables["sales_orders"]

        required = {

            "order_subtotal", "shipping_fee",

            "order_tax_amount", "order_total",

        }

        if required.issubset(df.columns):

            expected = (

                numeric_values(df, "order_subtotal")

                + numeric_values(df, "shipping_fee")

                + numeric_values(df, "order_tax_amount")

            )

            actual = numeric_values(df, "order_total")

            mismatch = (

                expected.notna() & actual.notna()

                & ((expected - actual).abs() > 0.02)

            )

            if mismatch.any():

                add_issue(

                    issues, "sales_orders", "ORDER_TOTAL_MISMATCH", "ERROR",

                    f"{int(mismatch.sum())} orders fail subtotal + shipping "

                    "+ tax = total.",

                )

    # Available inventory cannot exceed stock on hand.

    if "inventory_snapshots" in tables:

        df = tables["inventory_snapshots"]

        if {"available_stock", "stock_on_hand"}.issubset(df.columns):

            available = numeric_values(df, "available_stock")

            on_hand = numeric_values(df, "stock_on_hand")

            invalid = (

                available.notna() & on_hand.notna() & (available > on_hand)

            )

            if invalid.any():

                add_issue(

                    issues, "inventory_snapshots",

                    "AVAILABLE_EXCEEDS_ON_HAND", "ERROR",

                    f"{int(invalid.sum())} rows have available stock "

                    "greater than stock on hand.",

                )

    # Receipt dates cannot precede purchase-order dates.

    if "purchase_orders" in tables:

        df = tables["purchase_orders"]

        start_col = "purchase_order_date"

        end_col = "expected_receipt_date"

        if {start_col, end_col}.issubset(df.columns):

            start = pd.to_datetime(df[start_col], errors="coerce")

            end = pd.to_datetime(df[end_col], errors="coerce")

            invalid = start.notna() & end.notna() & (end < start)

            if invalid.any():

                add_issue(

                    issues, "purchase_orders", "INVALID_DATE_ORDER", "ERROR",

                    f"{int(invalid.sum())} expected receipt dates precede "

                    "their purchase-order dates.",

                )

def validate_primary_keys(tables, issues):

    for table_name, (_, key, _) in ENTITY_SPECS.items():

        if table_name not in tables:

            continue

        df = tables[table_name]

        if key not in df.columns:

            add_issue(

                issues, table_name, "PK_COLUMN_MISSING", "ERROR",

                f"Primary key '{key}' is missing.",

            )

            continue

        missing = ~nonblank(df[key])

        if missing.any():

            add_issue(

                issues, table_name, "PK_NULL", "ERROR",

                f"{int(missing.sum())} rows have a null primary key.",

            )

        duplicates = df.loc[~missing, key].duplicated(keep=False)

        if duplicates.any():

            add_issue(

                issues, table_name, "PK_DUPLICATE", "ERROR",

                f"{int(duplicates.sum())} rows have duplicate primary keys.",

            )

        if df.empty:

            add_issue(

                issues, table_name, "EMPTY_TABLE", "WARNING",

                "Candidate table contains zero rows.",

            )

# ============================================================

# 8. PII HANDLING

# ============================================================

def apply_pii_policy(tables, issues):

    """

    Remove direct contact details from Silver outputs.

    Raw S3 objects are not modified.

    'review' preserves the columns but emits ERROR issues, so

    the pipeline will not publish those tables under this policy.

    """

    pii_columns = {

        "customers": ["customer_email", "customer_phone"],

        "employees": ["employee_email"],

    }

    for table_name, columns in pii_columns.items():

        if table_name not in tables:

            continue

        existing = [c for c in columns if c in tables[table_name].columns]

        if not existing:

            continue

        if PII_POLICY == "remove":

            tables[table_name] = tables[table_name].drop(

                columns=existing

            )

            add_issue(

                issues, table_name, "PII_REMOVED", "INFO",

                f"Removed direct contact columns from Silver: {existing}.",

            )

        else:

            add_issue(

                issues, table_name, "PII_POLICY_REVIEW", "ERROR",

                f"Sensitive columns remain in candidate output: {existing}.",

            )

# ============================================================

# 9. VERIFY PROCESSED OUTPUTS

# ============================================================

def verify_processed_files(expected_names):

    """

    Verify that each expected file is discoverable through the

    documented processed-data listing API.

    """

    result = api_get(

        "/processed-files",

        {"project": PROJECT, "path": PROCESSED_PREFIX.strip("/") + "/"},

    )

    files = result.get("files", [])

    returned_keys = {

        str(item.get("key", "")) for item in files

        if isinstance(item, dict)

    }

    expected_keys = {

        f"{PROJECT}/{PROCESSED_PREFIX.strip('/')}/{name}"

        for name in expected_names

    }

    missing = expected_keys - returned_keys

    if missing:

        print("WARNING: These files were not found in processed listing:")

        for key in sorted(missing):

            print(f"  - {key}")

        return False

    print(f"Verified {len(expected_keys)} processed files using API.")

    return True

# ============================================================

# 10. MAIN PIPELINE

# ============================================================

def main():

    print("=" * 65)

    print("DECATHLON RAW-TO-SILVER PIPELINE")

    print("=" * 65)

    print(f"Project:       {PROJECT}")

    print(f"Raw prefix:    {SOURCE_PREFIX}")

    print(f"Silver prefix: {PROCESSED_PREFIX}")

    print(f"PII policy:    {PII_POLICY}")

    print()

    source = SOURCE_PREFIX.strip("/")

    silver = PROCESSED_PREFIX.strip("/")

    if not source or not silver:

        raise ValueError("Source and Silver prefixes must not be empty.")

    if source == silver:

        raise ValueError("Raw and Silver prefixes must be different.")

    # Avoid placing Silver inside the raw source prefix.

    if silver.startswith(source + "/"):

        raise ValueError(

            "Silver prefix is nested inside the raw source prefix. "

            "Choose a separate Silver prefix."

        )

    file_infos = api_list_raw_files()

    object_keys = []

    for item in file_infos:

        key = get_object_key(item)

        if not key or not key.lower().endswith(".csv"):

            continue

        if key.startswith(source + "/"):

            object_keys.append(key)

        elif key.startswith(source):

            # A key must be inside the source folder, not merely share

            # its prefix (for example 2025/01/010/).

            continue

    object_keys = sorted(set(object_keys))

    if not object_keys:

        raise ValueError(

            f"No CSV files found under raw prefix '{SOURCE_PREFIX}'."

        )

    print(f"Discovered {len(object_keys)} raw CSV file(s).")

    for key in object_keys:

        print(f"  {key}")

    print()

    issues = []

    source_dfs = {}

    discovered_sources = []

    all_tables = {}

    # --------------------------------------------------------

    # A. Download, clean and construct normalized candidates.

    # --------------------------------------------------------

    for object_key in object_keys:

        filename = PurePosixPath(object_key).name

        source_name = identify_source(filename)

        print(f"Downloading: {object_key}")

        raw_df = download_csv(object_key)

        clean_df = transform_dataframe(raw_df)

        print(f"  Rows: {len(clean_df)} | Columns: {len(clean_df.columns)}")

        print(f"  Source: {source_name or 'UNRECOGNIZED'}")

        if source_name is None:

            add_issue(

                issues, "_source_discovery", "UNRECOGNIZED_SOURCE", "ERROR",

                f"Unrecognized management filename: {filename}", filename,

            )

            continue

        if source_name in source_dfs:

            add_issue(

                issues, "_source_discovery", "DUPLICATE_SOURCE", "ERROR",

                f"More than one file maps to '{source_name}'. "

                "This script expects one file per management source.",

                filename,

            )

            continue

        source_dfs[source_name] = clean_df

        discovered_sources.append(source_name)

        candidates = build_entity_tables(

            source_name, clean_df, issues, filename

        )

        for table_name, table_df in candidates.items():

            if table_name in all_tables:

                add_issue(

                    issues, table_name, "DUPLICATE_TABLE_PRODUCER", "ERROR",

                    "More than one source produced this table.", filename,

                )

            else:

                all_tables[table_name] = table_df

                print(f"  Candidate {table_name}: {len(table_df)} rows")

    # --------------------------------------------------------

    # B. Source discovery and table validation.

    # --------------------------------------------------------

    missing_sources = sorted(EXPECTED_SOURCES - set(discovered_sources))

    if missing_sources:

        add_issue(

            issues, "_source_discovery", "MISSING_SOURCES", "ERROR",

            f"Expected sources not discovered: {missing_sources}",

        )

    validate_required_fields(all_tables, issues)

    validate_primary_keys(all_tables, issues)

    validate_foreign_keys(all_tables, issues)

    validate_numeric_columns(all_tables, issues)

    validate_business_rules(all_tables, issues)

    apply_pii_policy(all_tables, issues)

    quality_columns = [

        "source_file", "table_name", "rule", "severity", "details"

    ]

    report = pd.DataFrame(issues, columns=quality_columns)

    if not report.empty:

        report = report.drop_duplicates().reset_index(drop=True)

    error_count = (

        int(report["severity"].eq("ERROR").sum())

        if not report.empty else 0

    )

    warning_count = (

        int(report["severity"].eq("WARNING").sum())

        if not report.empty else 0

    )

    info_count = (

        int(report["severity"].eq("INFO").sum())

        if not report.empty else 0

    )

    print()

    print("=" * 65)

    print("VALIDATION SUMMARY")

    print("=" * 65)

    print(f"Sources discovered: {len(set(discovered_sources))}/8")

    print(f"Candidate tables:   {len(all_tables)}")

    print(f"Blocking errors:    {error_count}")

    print(f"Warnings:            {warning_count}")

    print(f"Informational items: {info_count}")

    if not report.empty:

        print("\nIssue counts by severity:")

        print(report["severity"].value_counts().to_string())

        print("\nFirst 30 issues:")

        print(report.head(30).to_string(index=False))

    else:

        print("All configured checks passed.")

    # --------------------------------------------------------

    # C. Upload quality report before deciding whether to publish.

    # --------------------------------------------------------

    print("\nUploading Silver quality report...")

    upload_csv_to_s3(report, "data_quality_issues.csv")

    # --------------------------------------------------------

    # D. Block all table uploads if validation errors exist.

    # --------------------------------------------------------

    if error_count:

        print("\nSILVER TABLE UPLOAD BLOCKED.")

        print(

            "Fix the ERROR records in data_quality_issues.csv, "

            "then rerun the pipeline."

        )

        sys.exit(2)

    # --------------------------------------------------------

    # E. Upload validated Silver entity tables.

    # --------------------------------------------------------

    print("\nUploading validated Silver tables...")

    expected_names = ["data_quality_issues.csv"]

    for table_name, df in sorted(all_tables.items()):

        filename = f"{table_name}.csv"

        upload_csv_to_s3(df, filename)

        expected_names.append(filename)

    # --------------------------------------------------------

    # F. Verify through the processed-data API.

    # --------------------------------------------------------

    print("\nVerifying Silver outputs...")

    verified = verify_processed_files(expected_names)

    print("\n" + "=" * 65)

    print("PIPELINE SUMMARY")

    print("=" * 65)

    print(f"Raw CSVs processed: {len(object_keys)}")

    print(f"Silver tables:      {len(all_tables)}")

    print(f"Quality issues:     {len(report)}")

    print(f"Processed verified: {verified}")

    print("Pipeline finished.")

if __name__ == "__main__":

    try:

        main()

    except requests.RequestException as exc:

        print(f"API request failed: {exc}", file=sys.stderr)

        sys.exit(1)

    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
