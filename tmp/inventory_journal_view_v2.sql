-- inventory_journal_view v2: 月次COGS（販売数量×標準原価）
-- FY2023/2024: months 1-11 = shipments × standard_cost, Dec = 年次調整（三分法一致）
-- FY2025+完了年: 同上（Opening/Closing は SP-API データから自動計算）
-- 当年度（未完了）: 月次COGS のみ（12月調整なし）

WITH
-- ① 月次販売数量 × 標準原価
monthly_cogs_by_sku AS (
  SELECT
    CAST(SUBSTR(l.Date, STRPOS(l.Date,'/')+1) AS INT64) AS year,
    CAST(SUBSTR(l.Date, 1, STRPOS(l.Date,'/')-1) AS INT64) AS month,
    pm.nocodb_id AS products_id,
    ABS(SUM(CAST(l.Customer_Shipments AS INT64))) AS shipment_qty
  FROM `main-project-477501.sp_api_external.ledger-summary-view-data` l
  JOIN `main-project-477501.nocodb.product_master` pm ON l.MSKU = pm.amazon_sku
  WHERE l.Disposition = 'SELLABLE'
  GROUP BY 1, 2, 3
  HAVING shipment_qty > 0
),

monthly_totals AS (
  SELECT
    mc.year, mc.month,
    SUM(mc.shipment_qty * CAST(sch.standard_cost AS INT64)) AS cogs_amount
  FROM monthly_cogs_by_sku mc
  JOIN `main-project-477501.nocodb.standard_cost_history` sch
    ON mc.products_id = sch.products_id
    AND SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_start_date) <= DATE(mc.year, mc.month, 1)
    AND (SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_end_date) >= LAST_DAY(DATE(mc.year, mc.month, 1))
         OR sch.effective_end_date IS NULL)
  WHERE sch.standard_cost IS NOT NULL
  GROUP BY 1, 2
),

-- ② 年末在庫スナップショット（12月データ）
inventory_snapshot AS (
  SELECT
    CAST(SUBSTR(l.Date, 4, 4) AS INT64) AS snapshot_year,
    pm.nocodb_id AS products_id,
    SUM(CAST(l.Ending_Warehouse_Balance AS INT64)) AS qty
  FROM `main-project-477501.sp_api_external.ledger-summary-view-data` l
  JOIN `main-project-477501.nocodb.product_master` pm ON l.MSKU = pm.amazon_sku
  WHERE l.Date LIKE '12/%'
    AND l.Disposition = 'SELLABLE'
  GROUP BY 1, 2
),

closing_values AS (
  SELECT
    snap.snapshot_year AS fiscal_year,
    SUM(snap.qty * CAST(sch.standard_cost AS INT64)) AS closing_value
  FROM inventory_snapshot snap
  JOIN `main-project-477501.nocodb.standard_cost_history` sch
    ON snap.products_id = sch.products_id
    AND SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_start_date) <= DATE(snap.snapshot_year, 12, 31)
    AND (SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_end_date) >= DATE(snap.snapshot_year, 12, 31)
         OR sch.effective_end_date IS NULL)
  WHERE sch.standard_cost IS NOT NULL
  GROUP BY 1
),

opening_values AS (
  SELECT
    snap.snapshot_year + 1 AS fiscal_year,
    SUM(snap.qty * CAST(sch.standard_cost AS INT64)) AS opening_value
  FROM inventory_snapshot snap
  JOIN `main-project-477501.nocodb.standard_cost_history` sch
    ON snap.products_id = sch.products_id
    AND SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_start_date) <= DATE(snap.snapshot_year + 1, 1, 1)
    AND (SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_end_date) >= DATE(snap.snapshot_year + 1, 1, 1)
         OR sch.effective_end_date IS NULL)
  WHERE sch.standard_cost IS NOT NULL
  GROUP BY 1
),

-- ③ 三分法 net = Opening - Closing（年間の在庫調整額）
-- FY2023/2024: MF確定申告値をハードコード
-- FY2025+: SP-APIデータから自動計算（閉じた年度のみ）
sanpunpo_net AS (
  SELECT 2023 AS fiscal_year, CAST(0 - 93389 AS INT64) AS net_adjustment
  UNION ALL
  SELECT 2024, CAST(93389 - 0 AS INT64)
  UNION ALL
  SELECT
    COALESCE(o.fiscal_year, c.fiscal_year) AS fiscal_year,
    CAST(COALESCE(o.opening_value, 0) - COALESCE(c.closing_value, 0) AS INT64)
  FROM opening_values o
  FULL OUTER JOIN closing_values c ON o.fiscal_year = c.fiscal_year
  WHERE COALESCE(o.fiscal_year, c.fiscal_year) >= 2025
    AND c.fiscal_year IS NOT NULL
),

-- ④ 1-11月合計
jan_nov_totals AS (
  SELECT year, SUM(cogs_amount) AS total_cogs
  FROM monthly_totals
  WHERE month <= 11
  GROUP BY year
),

-- ⑤ 12月エントリ = 三分法net - 1-11月COGS
dec_entries AS (
  SELECT
    s.fiscal_year AS year,
    s.net_adjustment - COALESCE(jn.total_cogs, 0) AS dec_amount
  FROM sanpunpo_net s
  LEFT JOIN jan_nov_totals jn ON jn.year = s.fiscal_year
)

-- ============================
-- A: 完了年度 1-11月 月次COGS
-- ============================
SELECT
  CONCAT('inv_', CAST(mt.year AS STRING), '_', LPAD(CAST(mt.month AS STRING), 2, '0'), '_cogs') AS source_id,
  LAST_DAY(DATE(mt.year, mt.month, 1)) AS journal_date,
  mt.year AS fiscal_year,
  'debit' AS entry_side,
  '仕入高' AS account_name,
  mt.cogs_amount AS amount_jpy,
  CAST(NULL AS INT64) AS tax_code,
  CONCAT(CAST(mt.year AS STRING), '年', CAST(mt.month AS STRING), '月 月次売上原価') AS description,
  'inventory_adjustment' AS source_table
FROM monthly_totals mt
INNER JOIN sanpunpo_net s ON mt.year = s.fiscal_year
WHERE mt.month <= 11 AND mt.cogs_amount > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(mt.year AS STRING), '_', LPAD(CAST(mt.month AS STRING), 2, '0'), '_cogs'),
  LAST_DAY(DATE(mt.year, mt.month, 1)),
  mt.year,
  'credit', '商品', mt.cogs_amount,
  CAST(NULL AS INT64),
  CONCAT(CAST(mt.year AS STRING), '年', CAST(mt.month AS STRING), '月 月次売上原価'),
  'inventory_adjustment'
FROM monthly_totals mt
INNER JOIN sanpunpo_net s ON mt.year = s.fiscal_year
WHERE mt.month <= 11 AND mt.cogs_amount > 0

UNION ALL

-- ============================
-- B: 完了年度 12月（年次調整込）
-- ============================
SELECT
  CONCAT('inv_', CAST(de.year AS STRING), '_12_cogs') AS source_id,
  DATE(de.year, 12, 31) AS journal_date,
  de.year AS fiscal_year,
  'debit' AS entry_side,
  CASE WHEN de.dec_amount >= 0 THEN '仕入高' ELSE '商品' END AS account_name,
  ABS(de.dec_amount) AS amount_jpy,
  CAST(NULL AS INT64) AS tax_code,
  CONCAT(CAST(de.year AS STRING), '年12月 売上原価（年次調整込）') AS description,
  'inventory_adjustment' AS source_table
FROM dec_entries de
WHERE de.dec_amount != 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(de.year AS STRING), '_12_cogs'),
  DATE(de.year, 12, 31),
  de.year,
  'credit',
  CASE WHEN de.dec_amount >= 0 THEN '商品' ELSE '仕入高' END,
  ABS(de.dec_amount),
  CAST(NULL AS INT64),
  CONCAT(CAST(de.year AS STRING), '年12月 売上原価（年次調整込）'),
  'inventory_adjustment'
FROM dec_entries de
WHERE de.dec_amount != 0

UNION ALL

-- ============================
-- C: 当年度（未完了）月次COGS
-- ============================
SELECT
  CONCAT('inv_', CAST(mt.year AS STRING), '_', LPAD(CAST(mt.month AS STRING), 2, '0'), '_cogs') AS source_id,
  LAST_DAY(DATE(mt.year, mt.month, 1)) AS journal_date,
  mt.year AS fiscal_year,
  'debit' AS entry_side,
  '仕入高' AS account_name,
  mt.cogs_amount AS amount_jpy,
  CAST(NULL AS INT64) AS tax_code,
  CONCAT(CAST(mt.year AS STRING), '年', CAST(mt.month AS STRING), '月 月次売上原価') AS description,
  'inventory_adjustment' AS source_table
FROM monthly_totals mt
WHERE mt.year NOT IN (SELECT fiscal_year FROM sanpunpo_net)
  AND mt.cogs_amount > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(mt.year AS STRING), '_', LPAD(CAST(mt.month AS STRING), 2, '0'), '_cogs'),
  LAST_DAY(DATE(mt.year, mt.month, 1)),
  mt.year,
  'credit', '商品', mt.cogs_amount,
  CAST(NULL AS INT64),
  CONCAT(CAST(mt.year AS STRING), '年', CAST(mt.month AS STRING), '月 月次売上原価'),
  'inventory_adjustment'
FROM monthly_totals mt
WHERE mt.year NOT IN (SELECT fiscal_year FROM sanpunpo_net)
  AND mt.cogs_amount > 0
