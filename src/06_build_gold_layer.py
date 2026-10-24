"""
Decathlon Gold Layer Builder (Data Warehouse -> business-ready tables)

Reads the star schema in  decathlon_dw  and (re)builds aggregated, business
friendly tables in  decathlon_gold.  These are the tables Tableau or
Streamlit should connect to: no joins needed, readable column names,
already filtered and aggregated.

GOLD TABLES (grain in brackets)
-------------------------------
    gold_kpi_summary            headline numbers            [1 row]
    gold_sales_daily            sales trend                 [day x channel x category x brand]
    gold_product_performance    revenue / returns / rank    [product]
    gold_customer_360           lifetime value + RFM        [customer]
    gold_customer_cohorts       retention matrix            [first-order month x months since]
    gold_payments_monthly       payment mix                 [month x method x status]
    gold_returns_monthly        return reasons / refunds    [month x category x reason x status]
    gold_delivery_monthly       carrier performance         [dispatch month x carrier]
    gold_inventory_status       stock cover per product     [product]
    gold_inventory_by_warehouse stock health per site       [warehouse]
    gold_supplier_scorecard     fill rate, lead time        [supplier]
    gold_campaign_performance   CTR, conversion, ROAS       [campaign]
    gold_risk_monthly           fraud / risk trend          [month x level x decision]
    gold_service_monthly        after-sales workload        [month x type x priority]
    gold_refresh_log            what was built and when     [table]

Design rules
------------
- Every table is dropped and rebuilt from the warehouse in ONE transaction,
  so Tableau / Streamlit never see a half-built layer.
- "Today" is the latest order date in the data (not CURRENT_DATE), because
  the sample data is historical. Override with GOLD_AS_OF=YYYY-MM-DD.
- Sales metrics exclude cancelled orders. Change the list with
  GOLD_EXCLUDED_ORDER_STATUSES="cancelled,canceled,failed".
- Do not add up "orders" or "avg_order_value" across category / brand rows
  of gold_sales_daily: one order can contain several categories. Use
  gold_kpi_summary for true order totals.

Usage
-----
    python3 src/06_build_gold_layer.py
    python3 src/06_build_gold_layer.py --dry-run

Optional: GOLD_READER_ROLE=bi_reader  grants read-only access to that
PostgreSQL role (create it first), handy for Tableau / Streamlit users.
"""

import os
import re
import sys
import traceback

import psycopg
from psycopg import sql


# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

DATABASE = os.getenv("PGDATABASE", "decathlon_oltp")
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_USER = os.getenv("PGUSER", os.getenv("USER", "apple"))
DB_PASSWORD = os.getenv("PGPASSWORD") or None

DW = os.getenv("DW_SCHEMA", "decathlon_dw")
GOLD = os.getenv("GOLD_SCHEMA", "decathlon_gold")

for _name in (DW, GOLD):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", _name):
        raise ValueError(f"Unsafe schema name: {_name!r}")

DRY_RUN = "--dry-run" in sys.argv
READER_ROLE = os.getenv("GOLD_READER_ROLE", "").strip()
AS_OF_OVERRIDE = os.getenv("GOLD_AS_OF", "").strip()

EXCLUDED_STATUSES = [
    s.strip().lower()
    for s in os.getenv(
        "GOLD_EXCLUDED_ORDER_STATUSES", "cancelled,canceled"
    ).split(",")
    if s.strip()
]


def sales_filter():
    """SQL condition for fact_sales_line (alias f): drop cancelled orders."""
    if not EXCLUDED_STATUSES:
        return "TRUE"
    quoted = ", ".join(
        "'" + status.replace("'", "''") + "'" for status in EXCLUDED_STATUSES
    )
    return f"COALESCE(LOWER(f.order_status), '') NOT IN ({quoted})"


# ---------------------------------------------------------
# GOLD TABLE DEFINITIONS
#   (table name, SELECT statement, [columns to index])
# ---------------------------------------------------------

def build_tables(as_of):
    D = DW
    OK = sales_filter()
    AS_OF = f"DATE '{as_of}'"

    # Latest snapshot per product x warehouse (shared by two tables).
    latest_snapshot = f"""
        latest AS (
            SELECT DISTINCT ON (p.product_id, w.warehouse_id)
                   p.product_id, w.warehouse_id,
                   i.stock_on_hand, i.reserved_stock, i.available_stock,
                   i.damaged_stock, i.in_transit_stock
            FROM {D}.fact_inventory_snapshot i
            JOIN {D}.dim_product   p ON p.product_key   = i.product_key
            JOIN {D}.dim_warehouse w ON w.warehouse_key = i.warehouse_key
            WHERE i.product_key <> -1 AND i.warehouse_key <> -1
            ORDER BY p.product_id, w.warehouse_id,
                     i.snapshot_date_key DESC, i.inventory_id DESC
        )
    """

    tables = []

    # -------------------------------------------------- KPI summary
    tables.append(("gold_kpi_summary", f"""
        SELECT {AS_OF}                                      AS as_of_date,
               s.net_sales, s.gross_sales, s.discount_amount,
               s.orders, s.order_lines, s.units_sold,
               s.buying_customers,
               ROUND(s.net_sales / NULLIF(s.orders, 0), 2)  AS avg_order_value,
               r.return_count, r.refund_amount,
               ROUND(100.0 * r.return_count
                     / NULLIF(s.order_lines, 0), 2)         AS return_rate_pct
        FROM (
            SELECT SUM(f.net_line_amount)    AS net_sales,
                   SUM(f.gross_line_amount)  AS gross_sales,
                   SUM(f.discount_amount)    AS discount_amount,
                   COUNT(DISTINCT f.order_id) AS orders,
                   COUNT(*)                  AS order_lines,
                   SUM(f.quantity)           AS units_sold,
                   COUNT(DISTINCT c.customer_id) AS buying_customers
            FROM {D}.fact_sales_line f
            JOIN {D}.dim_customer c ON c.customer_key = f.customer_key
            WHERE {OK}
        ) s
        CROSS JOIN (
            SELECT COUNT(*) AS return_count,
                   COALESCE(SUM(refund_amount), 0) AS refund_amount
            FROM {D}.fact_return
        ) r
    """, []))

    # -------------------------------------------------- sales daily
    tables.append(("gold_sales_daily", f"""
        SELECT d.full_date                                AS sale_date,
               d.calendar_year, d.calendar_quarter, d.calendar_month,
               d.year_month, d.day_name, d.is_weekend,
               f.sales_channel,
               COALESCE(p.category_name, 'Unknown')       AS category_name,
               COALESCE(p.brand_name, 'Unknown')          AS brand_name,
               COUNT(DISTINCT f.order_id)                 AS orders,
               COUNT(*)                                   AS order_lines,
               SUM(f.quantity)                            AS units_sold,
               SUM(f.gross_line_amount)                   AS gross_sales,
               SUM(f.discount_amount)                     AS discount_amount,
               SUM(f.tax_amount)                          AS tax_amount,
               SUM(f.net_line_amount)                     AS net_sales,
               ROUND(100.0 * SUM(f.discount_amount)
                     / NULLIF(SUM(f.gross_line_amount), 0), 2) AS discount_pct,
               ROUND(SUM(f.net_line_amount)
                     / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_order_value
        FROM {D}.fact_sales_line f
        JOIN {D}.dim_date    d ON d.date_key    = f.order_date_key
        JOIN {D}.dim_product p ON p.product_key = f.product_key
        WHERE f.order_date_key <> -1 AND {OK}
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    """, ["sale_date", "year_month", "category_name"]))

    # -------------------------------------------------- product performance
    tables.append(("gold_product_performance", f"""
        WITH sales_agg AS (
            SELECT p.product_id,
                   COUNT(DISTINCT f.order_id) AS orders,
                   COUNT(*)                   AS order_lines,
                   SUM(f.quantity)            AS units_sold,
                   SUM(f.net_line_amount)     AS net_sales,
                   SUM(f.discount_amount)     AS discount_amount
            FROM {D}.fact_sales_line f
            JOIN {D}.dim_product p ON p.product_key = f.product_key
            WHERE {OK}
            GROUP BY p.product_id
        ),
        return_agg AS (
            SELECT p.product_id,
                   COUNT(*)             AS return_count,
                   SUM(r.refund_amount) AS refund_amount
            FROM {D}.fact_return r
            JOIN {D}.dim_product p ON p.product_key = r.product_key
            GROUP BY p.product_id
        )
        SELECT cur.product_id, cur.sku, cur.product_name, cur.brand_name,
               cur.category_name, cur.product_type, cur.product_status,
               cur.selling_price, cur.mrp,
               COALESCE(s.orders, 0)          AS orders,
               COALESCE(s.order_lines, 0)     AS order_lines,
               COALESCE(s.units_sold, 0)      AS units_sold,
               COALESCE(s.net_sales, 0)       AS net_sales,
               COALESCE(s.discount_amount, 0) AS discount_amount,
               COALESCE(r.return_count, 0)    AS return_count,
               COALESCE(r.refund_amount, 0)   AS refund_amount,
               ROUND(100.0 * COALESCE(r.return_count, 0)
                     / NULLIF(s.order_lines, 0), 2) AS return_rate_pct,
               RANK() OVER (ORDER BY COALESCE(s.net_sales, 0) DESC)
                                              AS revenue_rank,
               RANK() OVER (PARTITION BY cur.category_name
                            ORDER BY COALESCE(s.net_sales, 0) DESC)
                                              AS revenue_rank_in_category
        FROM {D}.dim_product cur
        LEFT JOIN sales_agg  s ON s.product_id = cur.product_id
        LEFT JOIN return_agg r ON r.product_id = cur.product_id
        WHERE cur.is_current AND cur.product_key <> -1
    """, ["product_id", "category_name"]))

    # -------------------------------------------------- customer 360 + RFM
    tables.append(("gold_customer_360", f"""
        WITH order_agg AS (
            SELECT c.customer_id,
                   COUNT(DISTINCT f.order_id)  AS orders,
                   SUM(f.quantity)             AS units,
                   SUM(f.net_line_amount)      AS net_sales,
                   MIN(d.full_date)            AS first_order_date,
                   MAX(d.full_date)            AS last_order_date
            FROM {D}.fact_sales_line f
            JOIN {D}.dim_customer c ON c.customer_key = f.customer_key
            JOIN {D}.dim_date     d ON d.date_key     = f.order_date_key
            WHERE f.order_date_key <> -1 AND {OK}
            GROUP BY c.customer_id
        ),
        return_agg AS (
            SELECT c.customer_id, COUNT(*) AS return_count,
                   SUM(r.refund_amount) AS refund_amount
            FROM {D}.fact_return r
            JOIN {D}.dim_customer c ON c.customer_key = r.customer_key
            GROUP BY c.customer_id
        ),
        service_agg AS (
            SELECT c.customer_id, COUNT(*) AS service_cases,
                   COUNT(*) FILTER (WHERE s.resolution_date_key = -1)
                       AS open_service_cases
            FROM {D}.fact_service_case s
            JOIN {D}.dim_customer c ON c.customer_key = s.customer_key
            GROUP BY c.customer_id
        ),
        risk_agg AS (
            SELECT c.customer_id, COUNT(*) AS risk_cases,
                   SUM(k.is_fraud) AS fraud_cases
            FROM {D}.fact_risk_case k
            JOIN {D}.dim_customer c ON c.customer_key = k.customer_key
            GROUP BY c.customer_id
        ),
        base AS (
            SELECT cu.customer_id, cu.full_name, cu.registration_date,
                   cu.customer_status, cu.preferred_language,
                   cu.country, cu.state, cu.city, cu.loyalty_tier,
                   COALESCE(o.orders, 0)      AS orders,
                   COALESCE(o.units, 0)       AS units,
                   COALESCE(o.net_sales, 0)   AS net_sales,
                   ROUND(o.net_sales / NULLIF(o.orders, 0), 2)
                                              AS avg_order_value,
                   o.first_order_date, o.last_order_date,
                   ({AS_OF} - o.last_order_date) AS recency_days,
                   COALESCE(rt.return_count, 0)       AS return_count,
                   COALESCE(rt.refund_amount, 0)      AS refund_amount,
                   COALESCE(sv.service_cases, 0)      AS service_cases,
                   COALESCE(sv.open_service_cases, 0) AS open_service_cases,
                   COALESCE(rk.risk_cases, 0)         AS risk_cases,
                   COALESCE(rk.fraud_cases, 0)        AS fraud_cases,
                   (COALESCE(o.orders, 0) > 0)        AS has_purchased
            FROM {D}.dim_customer cu
            LEFT JOIN order_agg   o  ON o.customer_id  = cu.customer_id
            LEFT JOIN return_agg  rt ON rt.customer_id = cu.customer_id
            LEFT JOIN service_agg sv ON sv.customer_id = cu.customer_id
            LEFT JOIN risk_agg    rk ON rk.customer_id = cu.customer_id
            WHERE cu.is_current AND cu.customer_key <> -1
        ),
        scored AS (
            SELECT b.*,
                   CASE WHEN has_purchased THEN
                        NTILE(5) OVER (PARTITION BY has_purchased
                                       ORDER BY recency_days DESC) END AS r_score,
                   CASE WHEN has_purchased THEN
                        NTILE(5) OVER (PARTITION BY has_purchased
                                       ORDER BY orders ASC) END        AS f_score,
                   CASE WHEN has_purchased THEN
                        NTILE(5) OVER (PARTITION BY has_purchased
                                       ORDER BY net_sales ASC) END     AS m_score
            FROM base b
        )
        SELECT s.*,
               CASE
                   WHEN NOT s.has_purchased THEN 'No purchase yet'
                   WHEN s.r_score >= 4 AND s.f_score >= 4 AND s.m_score >= 4
                        THEN 'Champions'
                   WHEN s.r_score >= 3 AND s.f_score >= 3 THEN 'Loyal'
                   WHEN s.r_score >= 4 THEN 'Recent / New'
                   WHEN s.r_score <= 2 AND s.f_score >= 3 THEN 'At risk'
                   WHEN s.r_score <= 2 THEN 'Lapsed'
                   ELSE 'Needs attention'
               END AS rfm_segment
        FROM scored s
    """, ["customer_id", "rfm_segment", "country"]))

    # -------------------------------------------------- cohort retention
    tables.append(("gold_customer_cohorts", f"""
        WITH purchases AS (
            SELECT DISTINCT c.customer_id,
                   DATE_TRUNC('month', d.full_date)::date AS order_month
            FROM {D}.fact_sales_line f
            JOIN {D}.dim_customer c ON c.customer_key = f.customer_key
            JOIN {D}.dim_date     d ON d.date_key     = f.order_date_key
            WHERE f.order_date_key <> -1 AND {OK}
        ),
        first_purchase AS (
            SELECT customer_id, MIN(order_month) AS cohort_month
            FROM purchases GROUP BY customer_id
        ),
        cohort_size AS (
            SELECT cohort_month, COUNT(*) AS cohort_customers
            FROM first_purchase GROUP BY cohort_month
        )
        SELECT fp.cohort_month,
               ((DATE_PART('year', p.order_month)
                 - DATE_PART('year', fp.cohort_month)) * 12
                + (DATE_PART('month', p.order_month)
                   - DATE_PART('month', fp.cohort_month)))::integer
                                              AS months_since_first_order,
               cs.cohort_customers,
               COUNT(DISTINCT p.customer_id)  AS active_customers,
               ROUND(100.0 * COUNT(DISTINCT p.customer_id)
                     / cs.cohort_customers, 2) AS retention_pct
        FROM purchases p
        JOIN first_purchase fp ON fp.customer_id  = p.customer_id
        JOIN cohort_size    cs ON cs.cohort_month = fp.cohort_month
        GROUP BY fp.cohort_month, 2, cs.cohort_customers
    """, ["cohort_month"]))

    # -------------------------------------------------- payments monthly
    tables.append(("gold_payments_monthly", f"""
        SELECT d.year_month, d.calendar_year, d.calendar_month,
               COALESCE(p.payment_method, 'Unknown')  AS payment_method,
               COALESCE(p.payment_status, 'Unknown')  AS payment_status,
               COUNT(*)                               AS payments,
               SUM(p.payment_amount)                  AS payment_amount,
               ROUND(AVG(p.payment_amount), 2)        AS avg_payment_amount
        FROM {D}.fact_payment p
        JOIN {D}.dim_date d ON d.date_key = p.payment_date_key
        WHERE p.payment_date_key <> -1
        GROUP BY 1, 2, 3, 4, 5
    """, ["year_month"]))

    # -------------------------------------------------- returns monthly
    tables.append(("gold_returns_monthly", f"""
        SELECT d.year_month, d.calendar_year, d.calendar_month,
               COALESCE(p.category_name, 'Unknown')   AS category_name,
               COALESCE(r.return_reason, 'Unknown')   AS return_reason,
               COALESCE(r.return_status, 'Unknown')   AS return_status,
               COUNT(*)                               AS return_count,
               SUM(r.refund_amount)                   AS refund_amount,
               ROUND(AVG(r.days_since_order), 1)      AS avg_days_since_order
        FROM {D}.fact_return r
        JOIN {D}.dim_date    d ON d.date_key    = r.return_date_key
        JOIN {D}.dim_product p ON p.product_key = r.product_key
        WHERE r.return_date_key <> -1
        GROUP BY 1, 2, 3, 4, 5, 6
    """, ["year_month", "category_name"]))

    # -------------------------------------------------- delivery monthly
    tables.append(("gold_delivery_monthly", f"""
        SELECT d.year_month, d.calendar_year, d.calendar_month,
               COALESCE(s.carrier_name, 'Unknown')    AS carrier_name,
               COUNT(*)                               AS shipments,
               SUM(s.is_delivered)                    AS delivered,
               SUM(s.is_late)                         AS late_deliveries,
               ROUND(100.0 * (SUM(s.is_delivered) - SUM(s.is_late))
                     / NULLIF(SUM(s.is_delivered), 0), 2) AS on_time_pct,
               ROUND(AVG(s.delivery_days), 2)         AS avg_delivery_days,
               ROUND(AVG(s.delay_days) FILTER (WHERE s.is_late = 1), 2)
                                                      AS avg_delay_days_when_late
        FROM {D}.fact_shipment s
        JOIN {D}.dim_date d ON d.date_key = s.dispatch_date_key
        WHERE s.dispatch_date_key <> -1
        GROUP BY 1, 2, 3, 4
    """, ["year_month", "carrier_name"]))

    # -------------------------------------------------- inventory by product
    tables.append(("gold_inventory_status", f"""
        WITH {latest_snapshot},
        stock AS (
            SELECT product_id,
                   COUNT(*)              AS warehouses_stocked,
                   SUM(stock_on_hand)    AS stock_on_hand,
                   SUM(reserved_stock)   AS reserved_stock,
                   SUM(available_stock)  AS available_stock,
                   SUM(damaged_stock)    AS damaged_stock,
                   SUM(in_transit_stock) AS in_transit_stock
            FROM latest GROUP BY product_id
        ),
        velocity AS (
            SELECT p.product_id, SUM(f.quantity) AS units_last_30d
            FROM {D}.fact_sales_line f
            JOIN {D}.dim_product p ON p.product_key = f.product_key
            JOIN {D}.dim_date    d ON d.date_key    = f.order_date_key
            WHERE {OK}
              AND d.full_date >  {AS_OF} - 30
              AND d.full_date <= {AS_OF}
            GROUP BY p.product_id
        )
        SELECT cur.product_id, cur.sku, cur.product_name, cur.brand_name,
               cur.category_name,
               s.warehouses_stocked, s.stock_on_hand, s.reserved_stock,
               s.available_stock, s.damaged_stock, s.in_transit_stock,
               COALESCE(v.units_last_30d, 0)                AS units_sold_last_30d,
               ROUND(COALESCE(v.units_last_30d, 0) / 30.0, 2) AS avg_daily_units,
               ROUND(s.available_stock
                     / NULLIF(v.units_last_30d / 30.0, 0), 1) AS days_of_cover,
               CASE
                   WHEN s.available_stock <= 0 THEN 'Out of stock'
                   WHEN COALESCE(v.units_last_30d, 0) = 0 THEN 'No recent sales'
                   WHEN s.available_stock / (v.units_last_30d / 30.0) < 14
                        THEN 'Low stock'
                   WHEN s.available_stock / (v.units_last_30d / 30.0) > 180
                        THEN 'Overstock'
                   ELSE 'Healthy'
               END AS stock_status
        FROM stock s
        JOIN {D}.dim_product cur
          ON cur.product_id = s.product_id AND cur.is_current
        LEFT JOIN velocity v ON v.product_id = s.product_id
    """, ["product_id", "stock_status"]))

    # -------------------------------------------------- inventory by warehouse
    tables.append(("gold_inventory_by_warehouse", f"""
        WITH {latest_snapshot}
        SELECT w.warehouse_id, w.warehouse_name, w.city, w.state,
               COUNT(*)                         AS products_stocked,
               SUM(l.stock_on_hand)             AS stock_on_hand,
               SUM(l.reserved_stock)            AS reserved_stock,
               SUM(l.available_stock)           AS available_stock,
               SUM(l.damaged_stock)             AS damaged_stock,
               SUM(l.in_transit_stock)          AS in_transit_stock,
               ROUND(100.0 * SUM(l.damaged_stock)
                     / NULLIF(SUM(l.stock_on_hand), 0), 2) AS damaged_pct,
               COUNT(*) FILTER (WHERE l.available_stock <= 0)
                                                AS out_of_stock_products
        FROM latest l
        JOIN {D}.dim_warehouse w ON w.warehouse_id = l.warehouse_id
        GROUP BY w.warehouse_id, w.warehouse_name, w.city, w.state
    """, ["warehouse_id"]))

    # -------------------------------------------------- supplier scorecard
    tables.append(("gold_supplier_scorecard", f"""
        WITH po AS (
            SELECT s.supplier_id,
                   COUNT(DISTINCT f.purchase_order_id) AS purchase_orders,
                   SUM(f.quantity_ordered)             AS units_ordered,
                   SUM(f.quantity_received)            AS units_received,
                   SUM(f.line_amount)                  AS purchase_value,
                   ROUND(AVG(dx.full_date - dd.full_date), 1)
                                                       AS avg_planned_lead_days
            FROM {D}.fact_purchase_order_line f
            JOIN {D}.dim_supplier s  ON s.supplier_key = f.supplier_key
            JOIN {D}.dim_date     dd ON dd.date_key = f.purchase_order_date_key
            JOIN {D}.dim_date     dx ON dx.date_key = f.expected_receipt_date_key
            GROUP BY s.supplier_id
        )
        SELECT cur.supplier_id, cur.supplier_name, cur.supplier_type,
               cur.supplier_country, cur.supplier_status,
               cur.supplier_rating, cur.lead_days AS contracted_lead_days,
               cur.moq, cur.risk_score, cur.compliance_status,
               COALESCE(po.purchase_orders, 0) AS purchase_orders,
               COALESCE(po.units_ordered, 0)   AS units_ordered,
               COALESCE(po.units_received, 0)  AS units_received,
               COALESCE(po.purchase_value, 0)  AS purchase_value,
               po.avg_planned_lead_days,
               ROUND(100.0 * po.units_received
                     / NULLIF(po.units_ordered, 0), 2) AS fill_rate_pct
        FROM {D}.dim_supplier cur
        LEFT JOIN po ON po.supplier_id = cur.supplier_id
        WHERE cur.is_current AND cur.supplier_key <> -1
    """, ["supplier_id"]))

    # -------------------------------------------------- campaign performance
    tables.append(("gold_campaign_performance", f"""
        WITH ev AS (
            SELECT campaign_key,
                   COUNT(*)              AS events,
                   SUM(impressions)      AS impressions,
                   SUM(clicks)           AS clicks,
                   SUM(conversions)      AS conversions,
                   SUM(conversion_value) AS conversion_value
            FROM {D}.fact_ad_event
            GROUP BY campaign_key
        )
        SELECT c.campaign_id, c.campaign_name, c.campaign_type,
               c.campaign_objective, c.campaign_status,
               c.start_date, c.end_date, c.budget,
               COALESCE(ev.events, 0)           AS events,
               COALESCE(ev.impressions, 0)      AS impressions,
               COALESCE(ev.clicks, 0)           AS clicks,
               COALESCE(ev.conversions, 0)      AS conversions,
               COALESCE(ev.conversion_value, 0) AS conversion_value,
               ROUND(100.0 * ev.clicks / NULLIF(ev.impressions, 0), 2)
                                                AS ctr_pct,
               ROUND(100.0 * ev.conversions / NULLIF(ev.clicks, 0), 2)
                                                AS conversion_rate_pct,
               ROUND(c.budget / NULLIF(ev.conversions, 0), 2)
                                                AS cost_per_conversion,
               ROUND(ev.conversion_value / NULLIF(c.budget, 0), 2)
                                                AS roas
        FROM {D}.dim_campaign c
        LEFT JOIN ev ON ev.campaign_key = c.campaign_key
        WHERE c.campaign_key <> -1
    """, ["campaign_id"]))

    # -------------------------------------------------- risk monthly
    tables.append(("gold_risk_monthly", f"""
        SELECT d.year_month,
               COALESCE(k.risk_level, 'Unknown')    AS risk_level,
               COALESCE(k.risk_decision, 'Unknown') AS risk_decision,
               COUNT(*)                             AS risk_cases,
               SUM(k.is_fraud)                      AS fraud_cases,
               ROUND(100.0 * SUM(k.is_fraud) / COUNT(*), 2) AS fraud_rate_pct,
               ROUND(AVG(k.risk_score), 2)          AS avg_risk_score
        FROM {D}.fact_risk_case k
        JOIN {D}.dim_date d ON d.date_key = k.order_date_key
        GROUP BY 1, 2, 3
    """, ["year_month"]))

    # -------------------------------------------------- service monthly
    tables.append(("gold_service_monthly", f"""
        SELECT d.year_month,
               COALESCE(s.service_type, 'Unknown')   AS service_type,
               COALESCE(s.case_priority, 'Unknown')  AS case_priority,
               COUNT(*)                              AS cases,
               COUNT(*) FILTER (WHERE s.resolution_date_key <> -1)
                                                     AS resolved_cases,
               COUNT(*) FILTER (WHERE s.resolution_date_key = -1)
                                                     AS open_cases,
               ROUND(AVG(s.resolution_days), 1)      AS avg_resolution_days
        FROM {D}.fact_service_case s
        JOIN {D}.dim_date d ON d.date_key = s.open_date_key
        GROUP BY 1, 2, 3
    """, ["year_month"]))

    return tables


# ---------------------------------------------------------
# RUNNER
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


def require_warehouse(conn):
    needed = [
        "dim_date", "dim_customer", "dim_product", "dim_supplier",
        "dim_warehouse", "dim_campaign", "fact_sales_line", "fact_payment",
        "fact_shipment", "fact_return", "fact_inventory_snapshot",
        "fact_purchase_order_line", "fact_ad_event", "fact_service_case",
        "fact_risk_case",
    ]
    missing = []
    with conn.cursor() as cur:
        for name in needed:
            cur.execute("SELECT to_regclass(%s)", (f"{DW}.{name}",))
            if cur.fetchone()[0] is None:
                missing.append(name)
    if missing:
        raise RuntimeError(
            "Warehouse tables missing: " + ", ".join(missing)
            + ". Run 04_build_star_schema.py first."
        )


def find_as_of(cur):
    if AS_OF_OVERRIDE:
        return AS_OF_OVERRIDE

    cur.execute(f"""
        SELECT MAX(d.full_date)
        FROM {DW}.fact_sales_line f
        JOIN {DW}.dim_date d ON d.date_key = f.order_date_key
        WHERE f.order_date_key <> -1
    """)
    value = cur.fetchone()[0]

    if value is None:
        raise RuntimeError(
            "fact_sales_line is empty, cannot determine the as-of date."
        )
    return value.isoformat()


def main():
    print("Connecting to PostgreSQL...")
    refreshed = []

    with connect_postgres() as conn:
        require_warehouse(conn)

        with conn.transaction(force_rollback=DRY_RUN):
            with conn.cursor() as cur:
                as_of = find_as_of(cur)
                print(f"As-of date (latest order in data): {as_of}")
                print(f"Excluded order statuses: {EXCLUDED_STATUSES or 'none'}")

                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {GOLD}.gold_refresh_log (
                        table_name TEXT PRIMARY KEY,
                        row_count BIGINT NOT NULL,
                        as_of_date DATE NOT NULL,
                        refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)

                for name, select_sql, index_cols in build_tables(as_of):
                    cur.execute(f"DROP TABLE IF EXISTS {GOLD}.{name}")
                    cur.execute(
                        f"CREATE TABLE {GOLD}.{name} AS {select_sql}"
                    )

                    for column in index_cols:
                        cur.execute(
                            f"CREATE INDEX ix_{name}_{column} "
                            f"ON {GOLD}.{name} ({column})"
                        )

                    cur.execute(f"SELECT COUNT(*) FROM {GOLD}.{name}")
                    rows = cur.fetchone()[0]
                    refreshed.append((name, rows))
                    print(f"  built {name}: {rows} rows")

                    cur.execute(
                        f"""
                        INSERT INTO {GOLD}.gold_refresh_log
                            (table_name, row_count, as_of_date, refreshed_at)
                        VALUES (%s, %s, %s::date, NOW())
                        ON CONFLICT (table_name) DO UPDATE SET
                            row_count = EXCLUDED.row_count,
                            as_of_date = EXCLUDED.as_of_date,
                            refreshed_at = NOW()
                        """,
                        (name, rows, as_of),
                    )

                if READER_ROLE:
                    role = sql.Identifier(READER_ROLE)
                    schema = sql.Identifier(GOLD)
                    cur.execute(
                        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}")
                        .format(schema, role)
                    )
                    cur.execute(
                        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}")
                        .format(schema, role)
                    )
                    print(f"Granted read-only access to role {READER_ROLE}.")

    print(f"\nGold tables built: {len(refreshed)}")
    print(f"Total rows: {sum(r for _, r in refreshed)}")

    if DRY_RUN:
        print("\nDRY RUN: everything was rolled back.")
    else:
        print(f"\nSUCCESS: connect Tableau / Streamlit to schema '{GOLD}'.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nGold layer build failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
