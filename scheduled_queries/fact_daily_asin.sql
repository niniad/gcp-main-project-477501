-- Scheduled Query: fact_daily_asin
-- Schedule: 毎日 02:00 JST (17:00 UTC前日)
-- Destination: main-project-477501.analytics.fact_daily_asin
-- Write preference: WRITE_TRUNCATE (全置換)
--
-- 登録コマンド:
-- bq mk --transfer_config \
--   --project_id=main-project-477501 \
--   --data_source=scheduled_query \
--   --target_dataset=analytics \
--   --display_name="fact_daily_asin - Daily ASIN Fact Table" \
--   --schedule="every day 17:00" \
--   --params='{"query":"<このファイルの内容>","destination_table_name_template":"fact_daily_asin","write_disposition":"WRITE_TRUNCATE"}'

CREATE OR REPLACE TABLE `main-project-477501.analytics.fact_daily_asin` AS
WITH
traffic AS (
  SELECT
    report_date, child_asin, parent_asin,
    units_ordered, ordered_product_sales, total_order_items,
    sessions, page_views
  FROM `main-project-477501.analytics.stg_sp_traffic_child_asin`
),
products AS (
  SELECT asin, amazon_sku, name AS product_name
  FROM `main-project-477501.nocodb.product_master`
  WHERE asin IS NOT NULL
),
ads AS (
  SELECT
    report_date,
    advertised_asin AS child_asin,
    SUM(impressions) AS ad_impressions,
    SUM(clicks) AS ad_clicks,
    SUM(ad_cost) AS ad_cost,
    SUM(ad_sales_7d) AS ad_sales,
    SUM(ad_purchases_7d) AS ad_purchases,
    SUM(ad_units_sold_7d) AS ad_units_sold
  FROM `main-project-477501.analytics.stg_ads_product_daily`
  GROUP BY report_date, advertised_asin
),
costs AS (
  SELECT
    asin, standard_cost, effective_start_date,
    COALESCE(
      DATE_SUB(LEAD(effective_start_date) OVER (PARTITION BY asin ORDER BY effective_start_date), INTERVAL 1 DAY),
      DATE '9999-12-31'
    ) AS effective_end_date
  FROM `main-project-477501.analytics.stg_cost_standard`
  WHERE asin IS NOT NULL
),
latest_inventory AS (
  SELECT asin, fulfillable_quantity AS inventory_level
  FROM (
    SELECT asin, fulfillable_quantity,
      ROW_NUMBER() OVER (PARTITION BY asin ORDER BY fetched_at DESC) AS rn
    FROM `main-project-477501.analytics.stg_sp_inventory`
  )
  WHERE rn = 1
)
SELECT
  t.report_date,
  t.child_asin,
  t.parent_asin,
  COALESCE(p.product_name, '') AS product_name,
  COALESCE(p.amazon_sku, '') AS sku,
  t.sessions,
  t.page_views,
  t.units_ordered,
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
  t.ordered_product_sales
    - t.units_ordered * COALESCE(c.standard_cost, 0)
    - COALESCE(a.ad_cost, 0) AS estimated_profit,
  COALESCE(inv.inventory_level, 0) AS inventory_level,
  SAFE_DIVIDE(COALESCE(a.ad_cost, 0), t.ordered_product_sales) AS tacos,
  SAFE_DIVIDE(COALESCE(a.ad_cost, 0), NULLIF(COALESCE(a.ad_sales, 0), 0)) AS acos,
  SAFE_DIVIDE(t.units_ordered, t.sessions) AS total_cvr,
  SAFE_DIVIDE(COALESCE(a.ad_units_sold, 0), NULLIF(COALESCE(a.ad_clicks, 0), 0)) AS ad_cvr,
  SAFE_DIVIDE(
    GREATEST(t.units_ordered - COALESCE(a.ad_units_sold, 0), 0),
    NULLIF(GREATEST(t.sessions - COALESCE(a.ad_clicks, 0), 0), 0)
  ) AS organic_cvr,
  SAFE_DIVIDE(COALESCE(a.ad_cost, 0), NULLIF(COALESCE(a.ad_clicks, 0), 0)) AS cpc
FROM traffic t
LEFT JOIN products p ON t.child_asin = p.asin
LEFT JOIN ads a ON t.report_date = a.report_date AND t.child_asin = a.child_asin
LEFT JOIN costs c ON t.child_asin = c.asin
  AND t.report_date BETWEEN c.effective_start_date AND c.effective_end_date
LEFT JOIN latest_inventory inv ON t.child_asin = inv.asin;

-- fact_daily_parent_asin も同時更新
CREATE OR REPLACE TABLE `main-project-477501.analytics.fact_daily_parent_asin` AS
SELECT
  report_date, parent_asin,
  MIN(product_name) AS product_name,
  SUM(sessions) AS sessions,
  SUM(page_views) AS page_views,
  SUM(units_ordered) AS units_ordered,
  SUM(total_sales) AS total_sales,
  SUM(ad_impressions) AS ad_impressions,
  SUM(ad_clicks) AS ad_clicks,
  SUM(ad_cost) AS ad_cost,
  SUM(ad_sales) AS ad_sales,
  SUM(ad_units_sold) AS ad_units_sold,
  SUM(organic_sessions) AS organic_sessions,
  SUM(organic_units) AS organic_units,
  SUM(organic_sales) AS organic_sales,
  SUM(estimated_cogs) AS estimated_cogs,
  SUM(estimated_profit) AS estimated_profit,
  SUM(inventory_level) AS inventory_level,
  SAFE_DIVIDE(SUM(ad_cost), SUM(total_sales)) AS tacos,
  SAFE_DIVIDE(SUM(ad_cost), NULLIF(SUM(ad_sales), 0)) AS acos,
  SAFE_DIVIDE(SUM(units_ordered), SUM(sessions)) AS total_cvr,
  SAFE_DIVIDE(SUM(ad_units_sold), NULLIF(SUM(ad_clicks), 0)) AS ad_cvr,
  SAFE_DIVIDE(SUM(organic_units), NULLIF(SUM(organic_sessions), 0)) AS organic_cvr,
  SAFE_DIVIDE(SUM(ad_cost), NULLIF(SUM(ad_clicks), 0)) AS cpc
FROM `main-project-477501.analytics.fact_daily_asin`
GROUP BY report_date, parent_asin;
