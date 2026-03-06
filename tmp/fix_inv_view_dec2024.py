"""
inventory_journal_view 修正
FY2024末ゼロ化に伴うDec 2024 SP-API起点(¥510,518)を帳簿値(¥0)に補正

変更点: monthly_closing_corrected CTE を追加し、
        Dec 2024の値のみ¥0でオーバーライド

FY2026以降は自動で正常動作（修正不要）
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

INV_VIEW_ID = 'main-project-477501.accounting.inventory_journal_view'

view_sql = """
WITH
-- ① 月次期末在庫数量（SKU別）
monthly_inventory_snapshot AS (
  SELECT
    CAST(SUBSTR(l.Date, STRPOS(l.Date,'/')+1) AS INT64) AS year,
    CAST(SUBSTR(l.Date, 1, STRPOS(l.Date,'/')-1) AS INT64) AS month,
    pm.nocodb_id AS products_id,
    SUM(CAST(l.Ending_Warehouse_Balance AS INT64)) AS ending_qty
  FROM `main-project-477501.sp_api_external.ledger-summary-view-data` l
  JOIN `main-project-477501.nocodb.product_master` pm ON l.MSKU = pm.amazon_sku
  WHERE l.Disposition = 'SELLABLE'
  GROUP BY 1, 2, 3
),

-- ② 月次期末在庫金額（qty × 標準原価）
monthly_closing_value AS (
  SELECT snap.year, snap.month,
    SUM(snap.ending_qty * CAST(sch.standard_cost AS INT64)) AS closing_value
  FROM monthly_inventory_snapshot snap
  JOIN `main-project-477501.nocodb.standard_cost_history` sch
    ON snap.products_id = sch.products_id
    AND SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_start_date)
        <= LAST_DAY(DATE(snap.year, snap.month, 1))
    AND (SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_end_date)
         >= LAST_DAY(DATE(snap.year, snap.month, 1))
         OR sch.effective_end_date IS NULL)
  WHERE sch.standard_cost IS NOT NULL
  GROUP BY 1, 2
),

-- ③ 月次在庫変動（当月末 - 前月末）
-- FY2024末ゼロ化補正: Jan 2025の起点のみ¥0に固定
-- （Dec 2024 SP-API=¥510,518 vs 帳簿商品=¥0 の差異を吸収）
-- FY2026以降は自動で正常動作（この補正は不要になる）
monthly_inv_change AS (
  SELECT
    curr.year,
    curr.month,
    curr.closing_value,
    CASE
      WHEN curr.year = 2025 AND curr.month = 1
        THEN CAST(0 AS INT64)  -- Jan 2025のみ帳簿値¥0を起点に使用
      ELSE COALESCE(prev.closing_value, 0)
    END AS opening_value,
    curr.closing_value - CASE
      WHEN curr.year = 2025 AND curr.month = 1
        THEN CAST(0 AS INT64)
      ELSE COALESCE(prev.closing_value, 0)
    END AS inv_change
  FROM monthly_closing_value curr
  LEFT JOIN monthly_closing_value prev
    ON (curr.month = 1  AND prev.year = curr.year - 1 AND prev.month = 12)
    OR (curr.month > 1  AND prev.year = curr.year     AND prev.month = curr.month - 1)
)

-- 在庫増加: Dr.商品 / Cr.仕入高
SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj') AS source_id,
  LAST_DAY(DATE(ic.year, ic.month, 1)) AS journal_date,
  ic.year AS fiscal_year,
  'debit' AS entry_side,
  '商品' AS account_name,
  ic.inv_change AS amount_jpy,
  CAST(NULL AS INT64) AS tax_code,
  CONCAT(CAST(ic.year AS STRING), '年',
         CAST(ic.month AS STRING), '月 月次棚卸調整（在庫増）') AS description,
  'inventory_adjustment' AS source_table
FROM monthly_inv_change ic WHERE ic.inv_change > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'credit', '仕入高',
  ic.inv_change, CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '年',
         CAST(ic.month AS STRING), '月 月次棚卸調整（在庫増）'),
  'inventory_adjustment'
FROM monthly_inv_change ic WHERE ic.inv_change > 0

UNION ALL

-- 在庫減少: Dr.仕入高 / Cr.商品
SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'debit', '仕入高',
  ABS(ic.inv_change), CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '年',
         CAST(ic.month AS STRING), '月 月次棚卸調整（在庫減）'),
  'inventory_adjustment'
FROM monthly_inv_change ic WHERE ic.inv_change < 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'credit', '商品',
  ABS(ic.inv_change), CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '年',
         CAST(ic.month AS STRING), '月 月次棚卸調整（在庫減）'),
  'inventory_adjustment'
FROM monthly_inv_change ic WHERE ic.inv_change < 0
"""

# デプロイ
try:
    client.delete_table(INV_VIEW_ID)
    print('既存VIEW削除')
except Exception:
    pass

v = bigquery.Table(INV_VIEW_ID)
v.view_query = view_sql
client.create_table(v)
print('VIEW作成完了')

# 検証
print('\n=== 検証 ===')
q = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='debit' AND account_name='商品' THEN amount_jpy
           WHEN entry_side='credit' AND account_name='商品' THEN -amount_jpy
           ELSE 0 END) AS 商品残高
FROM `main-project-477501.accounting.inventory_journal_view`
GROUP BY 1 ORDER BY 1
"""
print('年度末 商品残高（inventory_journal_view単体）:')
for r in client.query(q).result():
    print(f'  FY{r.fiscal_year}: ¥{r.商品残高:,}')

# FY2025月次推移（修正後）
q2 = """
SELECT
  FORMAT_DATE('%Y-%m', journal_date) AS ym,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS net
FROM `main-project-477501.accounting.inventory_journal_view`
WHERE fiscal_year = 2025
GROUP BY 1 ORDER BY 1
"""
rows = list(client.query(q2).result())
print('\nFY2025 商品勘定 月次推移（修正後）:')
running = 0
for r in rows:
    running += r.net
    mark = ' ← ✓' if running >= 0 else ' ← ❌ マイナス'
    print(f'  {r.ym}: net={r.net:+,}  累計={running:,}{mark}')
