-- Scheduled Query: fact_monthly_settlement_sku
-- Schedule: 毎日 03:00 JST (18:00 UTC前日)
-- Destination: main-project-477501.analytics.fact_monthly_settlement_sku
-- Write preference: WRITE_TRUNCATE (全置換)
-- Transfer Config: projects/850116866513/locations/us-central1/transferConfigs/69a14225-0000-2699-8504-14223bb1fd6e

CREATE OR REPLACE TABLE `main-project-477501.analytics.fact_monthly_settlement_sku` AS
WITH
settlement_agg AS (
  SELECT FORMAT_DATE('%Y-%m', posted_date) AS year_month, sku,
    SUM(CASE WHEN transaction_type='Order' AND amount_type='ItemPrice' AND amount_description='Principal' THEN amount ELSE 0 END) AS settlement_sales,
    SUM(CASE WHEN transaction_type='Order' AND amount_type='ItemPrice' AND amount_description='Tax' THEN amount ELSE 0 END) AS settlement_tax,
    SUM(CASE WHEN transaction_type='Order' AND amount_type='ItemFees' THEN amount ELSE 0 END) AS amazon_fees,
    SUM(CASE WHEN transaction_type='Order' AND amount_type='Points' THEN amount ELSE 0 END) AS points_granted,
    SUM(CASE WHEN transaction_type='Order' AND amount_type='Promotion' THEN amount ELSE 0 END) AS promotions,
    SUM(CASE WHEN transaction_type='Refund' THEN amount ELSE 0 END) AS refund_total,
    SUM(CASE WHEN transaction_type='Order' AND amount_type='ItemPrice' AND amount_description='Principal' THEN quantity_purchased ELSE 0 END) AS settlement_qty
  FROM `main-project-477501.analytics.stg_sp_settlement`
  WHERE sku IS NOT NULL AND posted_date IS NOT NULL
  GROUP BY 1, 2
),
ad_cost_monthly AS (
  SELECT FORMAT_DATE('%Y-%m', a.report_date) AS year_month,
    COALESCE(pm.amazon_sku, a.advertised_sku) AS sku,
    SUM(a.ad_cost) AS ad_cost_allocated
  FROM `main-project-477501.analytics.stg_ads_product_daily` a
  LEFT JOIN `main-project-477501.nocodb.product_master` pm ON a.advertised_asin = pm.asin
  GROUP BY 1, 2
),
cost_lookup AS (
  SELECT amazon_sku, standard_cost, effective_start_date,
    COALESCE(DATE_SUB(LEAD(effective_start_date) OVER (PARTITION BY amazon_sku ORDER BY effective_start_date), INTERVAL 1 DAY), DATE '9999-12-31') AS effective_end_date
  FROM `main-project-477501.analytics.stg_cost_standard`
  WHERE amazon_sku IS NOT NULL
),
settlement_with_cost AS (
  SELECT s.*, c.standard_cost
  FROM settlement_agg s
  LEFT JOIN cost_lookup c ON s.sku = c.amazon_sku
    AND PARSE_DATE('%Y-%m', s.year_month) BETWEEN DATE_TRUNC(c.effective_start_date, MONTH) AND DATE_TRUNC(c.effective_end_date, MONTH)
)
SELECT
  sc.year_month, sc.sku, pm.asin, pm.name AS product_name,
  sc.settlement_sales, sc.settlement_tax, sc.amazon_fees, sc.points_granted, sc.promotions, sc.refund_total, sc.settlement_qty,
  COALESCE(ac.ad_cost_allocated, 0) AS ad_cost_allocated,
  sc.standard_cost AS standard_cost_per_unit,
  sc.settlement_qty * COALESCE(sc.standard_cost, 0) AS standard_cogs,
  sc.settlement_sales - sc.settlement_qty * COALESCE(sc.standard_cost, 0) AS gross_profit,
  sc.settlement_sales - sc.settlement_qty * COALESCE(sc.standard_cost, 0) + sc.amazon_fees AS gross_profit_after_fees,
  sc.settlement_sales - sc.settlement_qty * COALESCE(sc.standard_cost, 0) + sc.amazon_fees + sc.points_granted + sc.promotions - COALESCE(ac.ad_cost_allocated, 0) AS net_profit,
  sc.settlement_sales - sc.settlement_qty * COALESCE(sc.standard_cost, 0) + sc.amazon_fees + sc.points_granted + sc.promotions + sc.refund_total - COALESCE(ac.ad_cost_allocated, 0) AS net_profit_after_refund
FROM settlement_with_cost sc
LEFT JOIN ad_cost_monthly ac ON sc.year_month = ac.year_month AND sc.sku = ac.sku
LEFT JOIN `main-project-477501.nocodb.product_master` pm ON sc.sku = pm.amazon_sku
WHERE sc.settlement_qty > 0 OR sc.settlement_sales != 0
ORDER BY sc.year_month DESC, sc.settlement_sales DESC;
