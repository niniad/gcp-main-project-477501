import sys
sys.stdout.reconfigure(encoding='utf-8')

from google.cloud import bigquery_datatransfer_v1
from google.protobuf import struct_pb2
import google.auth

credentials, project = google.auth.default()
client = bigquery_datatransfer_v1.DataTransferServiceClient(credentials=credentials)
parent = f"projects/{project}/locations/us-central1"

# --- fact_daily_asin + fact_daily_parent_asin (02:00 JST = 17:00 UTC) ---
fact_asin_query = """
CREATE OR REPLACE TABLE `main-project-477501.analytics.fact_daily_asin` AS
WITH
traffic AS (
  SELECT report_date, child_asin, parent_asin,
    units_ordered, ordered_product_sales, total_order_items, sessions, page_views
  FROM `main-project-477501.analytics.stg_sp_traffic_child_asin`
),
products AS (
  SELECT asin, amazon_sku, name AS product_name
  FROM `main-project-477501.nocodb.product_master`
  WHERE asin IS NOT NULL
),
ads AS (
  SELECT report_date, advertised_asin AS child_asin,
    SUM(impressions) AS ad_impressions, SUM(clicks) AS ad_clicks,
    SUM(ad_cost) AS ad_cost, SUM(ad_sales_7d) AS ad_sales,
    SUM(ad_purchases_7d) AS ad_purchases, SUM(ad_units_sold_7d) AS ad_units_sold
  FROM `main-project-477501.analytics.stg_ads_product_daily`
  GROUP BY report_date, advertised_asin
),
costs AS (
  SELECT asin, standard_cost, effective_start_date,
    COALESCE(DATE_SUB(LEAD(effective_start_date) OVER (PARTITION BY asin ORDER BY effective_start_date), INTERVAL 1 DAY), DATE '9999-12-31') AS effective_end_date
  FROM `main-project-477501.analytics.stg_cost_standard`
  WHERE asin IS NOT NULL
),
latest_inventory AS (
  SELECT asin, fulfillable_quantity AS inventory_level
  FROM (SELECT asin, fulfillable_quantity, ROW_NUMBER() OVER (PARTITION BY asin ORDER BY fetched_at DESC) AS rn FROM `main-project-477501.analytics.stg_sp_inventory`)
  WHERE rn = 1
)
SELECT
  t.report_date, t.child_asin, t.parent_asin,
  COALESCE(p.product_name, '') AS product_name,
  COALESCE(p.amazon_sku, '') AS sku,
  t.sessions, t.page_views, t.units_ordered,
  t.ordered_product_sales AS total_sales,
  COALESCE(a.ad_impressions, 0) AS ad_impressions,
  COALESCE(a.ad_clicks, 0) AS ad_clicks,
  COALESCE(a.ad_cost, 0) AS ad_cost,
  COALESCE(a.ad_sales, 0) AS ad_sales,
  COALESCE(a.ad_units_sold, 0) AS ad_units_sold,
  GREATEST(t.sessions - COALESCE(a.ad_clicks, 0), 0) AS organic_sessions,
  GREATEST(t.units_ordered - COALESCE(a.ad_units_sold, 0), 0) AS organic_units,
  GREATEST(t.ordered_product_sales - COALESCE(a.ad_sales, 0), 0) AS organic_sales,
  c.standard_cost AS standard_cost_per_unit,
  t.units_ordered * COALESCE(c.standard_cost, 0) AS estimated_cogs,
  t.ordered_product_sales - t.units_ordered * COALESCE(c.standard_cost, 0) - COALESCE(a.ad_cost, 0) AS estimated_profit,
  COALESCE(inv.inventory_level, 0) AS inventory_level,
  SAFE_DIVIDE(COALESCE(a.ad_cost, 0), t.ordered_product_sales) AS tacos,
  SAFE_DIVIDE(COALESCE(a.ad_cost, 0), NULLIF(COALESCE(a.ad_sales, 0), 0)) AS acos,
  SAFE_DIVIDE(t.units_ordered, t.sessions) AS total_cvr,
  SAFE_DIVIDE(COALESCE(a.ad_units_sold, 0), NULLIF(COALESCE(a.ad_clicks, 0), 0)) AS ad_cvr,
  SAFE_DIVIDE(GREATEST(t.units_ordered - COALESCE(a.ad_units_sold, 0), 0), NULLIF(GREATEST(t.sessions - COALESCE(a.ad_clicks, 0), 0), 0)) AS organic_cvr,
  SAFE_DIVIDE(COALESCE(a.ad_cost, 0), NULLIF(COALESCE(a.ad_clicks, 0), 0)) AS cpc
FROM traffic t
LEFT JOIN products p ON t.child_asin = p.asin
LEFT JOIN ads a ON t.report_date = a.report_date AND t.child_asin = a.child_asin
LEFT JOIN costs c ON t.child_asin = c.asin AND t.report_date BETWEEN c.effective_start_date AND c.effective_end_date
LEFT JOIN latest_inventory inv ON t.child_asin = inv.asin;

CREATE OR REPLACE TABLE `main-project-477501.analytics.fact_daily_parent_asin` AS
SELECT
  report_date, parent_asin, MIN(product_name) AS product_name,
  SUM(sessions) AS sessions, SUM(page_views) AS page_views,
  SUM(units_ordered) AS units_ordered, SUM(total_sales) AS total_sales,
  SUM(ad_impressions) AS ad_impressions, SUM(ad_clicks) AS ad_clicks,
  SUM(ad_cost) AS ad_cost, SUM(ad_sales) AS ad_sales,
  SUM(ad_units_sold) AS ad_units_sold,
  SUM(organic_sessions) AS organic_sessions, SUM(organic_units) AS organic_units,
  SUM(organic_sales) AS organic_sales,
  SUM(estimated_cogs) AS estimated_cogs, SUM(estimated_profit) AS estimated_profit,
  SUM(inventory_level) AS inventory_level,
  SAFE_DIVIDE(SUM(ad_cost), SUM(total_sales)) AS tacos,
  SAFE_DIVIDE(SUM(ad_cost), NULLIF(SUM(ad_sales), 0)) AS acos,
  SAFE_DIVIDE(SUM(units_ordered), SUM(sessions)) AS total_cvr,
  SAFE_DIVIDE(SUM(ad_units_sold), NULLIF(SUM(ad_clicks), 0)) AS ad_cvr,
  SAFE_DIVIDE(SUM(organic_units), NULLIF(SUM(organic_sessions), 0)) AS organic_cvr,
  SAFE_DIVIDE(SUM(ad_cost), NULLIF(SUM(ad_clicks), 0)) AS cpc
FROM `main-project-477501.analytics.fact_daily_asin`
GROUP BY report_date, parent_asin;
"""

params = struct_pb2.Struct()
params.update({"query": fact_asin_query})

config = bigquery_datatransfer_v1.TransferConfig(
    display_name="EC Analytics: fact_daily_asin + fact_daily_parent_asin",
    data_source_id="scheduled_query",
    destination_dataset_id="analytics",
    schedule="every day 17:00",  # 17:00 UTC = 02:00 JST
    params=params,
)

result = client.create_transfer_config(
    parent=parent,
    transfer_config=config,
)
print(f"Created: {result.name}")
print(f"Schedule: {result.schedule}")
print(f"State: {result.state}")
