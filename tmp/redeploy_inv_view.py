"""inventory_journal_view 再デプロイ（商品計上方式）"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

INV_VIEW_ID = 'main-project-477501.accounting.inventory_journal_view'

inv_view_sql = """
WITH
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
  SELECT mc.year, mc.month,
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
inventory_snapshot AS (
  SELECT
    CAST(SUBSTR(l.Date, 4, 4) AS INT64) AS snapshot_year,
    pm.nocodb_id AS products_id,
    SUM(CAST(l.Ending_Warehouse_Balance AS INT64)) AS qty
  FROM `main-project-477501.sp_api_external.ledger-summary-view-data` l
  JOIN `main-project-477501.nocodb.product_master` pm ON l.MSKU = pm.amazon_sku
  WHERE l.Date LIKE '12/%' AND l.Disposition = 'SELLABLE'
  GROUP BY 1, 2
),
closing_values AS (
  SELECT snap.snapshot_year AS fiscal_year,
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
  SELECT snap.snapshot_year + 1 AS fiscal_year,
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
-- COGS = purchase_net + opening - closing
-- FY2023: (296970-30597) + 0 - 93389 = 172984
-- FY2024: (818330-650) + 93389 - 0 = 911069
sanpunpo_net AS (
  SELECT 2023 AS fiscal_year, CAST(172984 AS INT64) AS net_adjustment
  UNION ALL
  SELECT 2024, CAST(911069 AS INT64)
  UNION ALL
  SELECT
    COALESCE(o.fiscal_year, c.fiscal_year) AS fiscal_year,
    CAST(COALESCE(o.opening_value, 0) - COALESCE(c.closing_value, 0) AS INT64) AS net_adjustment
  FROM opening_values o
  FULL OUTER JOIN closing_values c ON o.fiscal_year = c.fiscal_year
  WHERE COALESCE(o.fiscal_year, c.fiscal_year) >= 2025
    AND c.fiscal_year IS NOT NULL
    AND FALSE
),
jan_nov_totals AS (
  SELECT year, SUM(cogs_amount) AS total_cogs
  FROM monthly_totals WHERE month <= 11
  GROUP BY year
),
dec_entries AS (
  SELECT s.fiscal_year AS year,
    s.net_adjustment - COALESCE(jn.total_cogs, 0) AS dec_amount
  FROM sanpunpo_net s
  LEFT JOIN jan_nov_totals jn ON jn.year = s.fiscal_year
)

SELECT
  CONCAT('inv_', CAST(mt.year AS STRING), '_', LPAD(CAST(mt.month AS STRING), 2, '0'), '_cogs') AS source_id,
  LAST_DAY(DATE(mt.year, mt.month, 1)) AS journal_date,
  mt.year AS fiscal_year, 'debit' AS entry_side,
  '\u4ed5\u5165\u9ad8' AS account_name,
  mt.cogs_amount AS amount_jpy, CAST(NULL AS INT64) AS tax_code,
  CONCAT(CAST(mt.year AS STRING), '\u5e74', CAST(mt.month AS STRING), '\u6708 \u6708\u6b21\u58f2\u4e0a\u539f\u4fa1') AS description,
  'inventory_adjustment' AS source_table
FROM monthly_totals mt
INNER JOIN sanpunpo_net s ON mt.year = s.fiscal_year
WHERE mt.month <= 11 AND mt.cogs_amount > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(mt.year AS STRING), '_', LPAD(CAST(mt.month AS STRING), 2, '0'), '_cogs'),
  LAST_DAY(DATE(mt.year, mt.month, 1)), mt.year, 'credit',
  '\u5546\u54c1', mt.cogs_amount, CAST(NULL AS INT64),
  CONCAT(CAST(mt.year AS STRING), '\u5e74', CAST(mt.month AS STRING), '\u6708 \u6708\u6b21\u58f2\u4e0a\u539f\u4fa1'),
  'inventory_adjustment'
FROM monthly_totals mt
INNER JOIN sanpunpo_net s ON mt.year = s.fiscal_year
WHERE mt.month <= 11 AND mt.cogs_amount > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(de.year AS STRING), '_12_cogs') AS source_id,
  DATE(de.year, 12, 31), de.year, 'debit',
  CASE WHEN de.dec_amount >= 0 THEN '\u4ed5\u5165\u9ad8' ELSE '\u5546\u54c1' END,
  ABS(de.dec_amount), CAST(NULL AS INT64),
  CONCAT(CAST(de.year AS STRING), '\u5e74\uff11\uff12\u6708 \u58f2\u4e0a\u539f\u4fa1\uff08\u5e74\u6b21\u8abf\u6574\u8fbc\uff09'),
  'inventory_adjustment'
FROM dec_entries de WHERE de.dec_amount != 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(de.year AS STRING), '_12_cogs'),
  DATE(de.year, 12, 31), de.year, 'credit',
  CASE WHEN de.dec_amount >= 0 THEN '\u5546\u54c1' ELSE '\u4ed5\u5165\u9ad8' END,
  ABS(de.dec_amount), CAST(NULL AS INT64),
  CONCAT(CAST(de.year AS STRING), '\u5e74\uff11\uff12\u6708 \u58f2\u4e0a\u539f\u4fa1\uff08\u5e74\u6b21\u8abf\u6574\u8fbc\uff09'),
  'inventory_adjustment'
FROM dec_entries de WHERE de.dec_amount != 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(mt.year AS STRING), '_', LPAD(CAST(mt.month AS STRING), 2, '0'), '_cogs'),
  LAST_DAY(DATE(mt.year, mt.month, 1)), mt.year, 'debit',
  '\u4ed5\u5165\u9ad8', mt.cogs_amount, CAST(NULL AS INT64),
  CONCAT(CAST(mt.year AS STRING), '\u5e74', CAST(mt.month AS STRING), '\u6708 \u6708\u6b21\u58f2\u4e0a\u539f\u4fa1'),
  'inventory_adjustment'
FROM monthly_totals mt
WHERE mt.year NOT IN (SELECT fiscal_year FROM sanpunpo_net) AND mt.cogs_amount > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(mt.year AS STRING), '_', LPAD(CAST(mt.month AS STRING), 2, '0'), '_cogs'),
  LAST_DAY(DATE(mt.year, mt.month, 1)), mt.year, 'credit',
  '\u5546\u54c1', mt.cogs_amount, CAST(NULL AS INT64),
  CONCAT(CAST(mt.year AS STRING), '\u5e74', CAST(mt.month AS STRING), '\u6708 \u6708\u6b21\u58f2\u4e0a\u539f\u4fa1'),
  'inventory_adjustment'
FROM monthly_totals mt
WHERE mt.year NOT IN (SELECT fiscal_year FROM sanpunpo_net) AND mt.cogs_amount > 0
"""

try:
    client.delete_table(INV_VIEW_ID)
    print('既存VIEW削除')
except Exception:
    pass

v = bigquery.Table(INV_VIEW_ID)
v.view_query = inv_view_sql
client.create_table(v)
print('VIEW作成完了')

# P/L 検証
print('\n=== P/L 検証 ===')
q_pl = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='credit' THEN amount_jpy ELSE 0 END) -
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE 0 END) AS net_pl
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name NOT IN (
  '楽天銀行','PayPay銀行','Amazon出品アカウント','未払金',
  'THE直行便','ESPRIME','YP','セールモンスター','事業主借','開業費','商品'
)
GROUP BY 1 ORDER BY 1
"""
expected = {2023: -1340610, 2024: -1088882}
for r in client.query(q_pl).result():
    exp = expected.get(r.fiscal_year, '?')
    mark = ' OK' if r.net_pl == exp else f' <- {exp}'
    print(f'  FY{r.fiscal_year}: {r.net_pl:>12,.0f}{mark}')

# 商品残高
print('\n=== 商品 年度別残高（累積）===')
q2 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS net
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '商品'
GROUP BY 1 ORDER BY 1
"""
cum = 0
for r in client.query(q2).result():
    cum += r.net
    mark = ''
    if r.fiscal_year == 2023 and abs(cum - 93389) < 5:
        mark = ' OK(93389)'
    elif r.fiscal_year == 2024 and abs(cum) < 5:
        mark = ' OK(0)'
    print(f'  FY{r.fiscal_year}: 当年¥{r.net:,.0f}  累積¥{cum:,.0f}{mark}')
