"""
月次三分法 + FY2023/2024 手動仕訳調整

フロー:
1. 代行会社 → 仕入高(109) に変更（NocoDB）
2. inventory_journal_view を月次三分法にデプロイ（BQ）
3. BQ sync は外部で実行（このスクリプトの前後に呼ぶ）
4. BQ から FY2023/2024 の 商品残高差額を計算
5. 手動仕訳として NocoDB に追加
   - FY2023-12-31: Dr.商品 / Cr.仕入高 （MF確定値93,389に合わせる）
   - FY2024-12-31: Dr.仕入高 / Cr.商品 （MF確定値 期末0 に合わせる）

三分法の月次COGS:
  月次COGS = 代行会社仕入高 + (月初在庫 - 月末在庫)
  年間COGS（自動）= 期首棚卸 + 仕入 - 期末棚卸 + 手動調整
"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
INV_VIEW_ID = 'main-project-477501.accounting.inventory_journal_view'

# =========================================
# Step 1: 代行会社 → 仕入高(109)
# =========================================
print('=== Step 1: 代行会社 → 仕入高(109) ===')
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM "nc_opau___代行会社" WHERE "nc_opau___freee勘定科目_id" = 17')
before = cur.fetchone()[0]
if before > 0:
    cur.execute('UPDATE "nc_opau___代行会社" SET "nc_opau___freee勘定科目_id" = 109 WHERE "nc_opau___freee勘定科目_id" = 17')
    conn.commit()
    print(f'  {cur.rowcount}件 更新（商品17 → 仕入高109）')
else:
    cur.execute('SELECT COUNT(*) FROM "nc_opau___代行会社" WHERE "nc_opau___freee勘定科目_id" = 109')
    already = cur.fetchone()[0]
    print(f'  既に仕入高(109): {already}件（変更不要）')
conn.close()

# =========================================
# Step 2: inventory_journal_view を月次三分法にデプロイ
# =========================================
print('\n=== Step 2: inventory_journal_view デプロイ（月次三分法）===')

inv_view_sql = """
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
monthly_inv_change AS (
  SELECT
    curr.year,
    curr.month,
    curr.closing_value,
    COALESCE(prev.closing_value, 0) AS opening_value,
    curr.closing_value - COALESCE(prev.closing_value, 0) AS inv_change
  FROM monthly_closing_value curr
  LEFT JOIN monthly_closing_value prev
    ON (curr.month = 1  AND prev.year = curr.year - 1 AND prev.month = 12)
    OR (curr.month > 1  AND prev.year = curr.year     AND prev.month = curr.month - 1)
)

-- 在庫増加: Dr.\u5546\u54c1 / Cr.\u4ed5\u5165\u9ad8
SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj') AS source_id,
  LAST_DAY(DATE(ic.year, ic.month, 1)) AS journal_date,
  ic.year AS fiscal_year,
  'debit' AS entry_side,
  '\u5546\u54c1' AS account_name,
  ic.inv_change AS amount_jpy,
  CAST(NULL AS INT64) AS tax_code,
  CONCAT(CAST(ic.year AS STRING), '\u5e74',
         CAST(ic.month AS STRING), '\u6708 \u6708\u6b21\u68da\u5378\u8abf\u6574\uff08\u5728\u5eab\u5897\uff09') AS description,
  'inventory_adjustment' AS source_table
FROM monthly_inv_change ic WHERE ic.inv_change > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'credit', '\u4ed5\u5165\u9ad8',
  ic.inv_change, CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '\u5e74',
         CAST(ic.month AS STRING), '\u6708 \u6708\u6b21\u68da\u5378\u8abf\u6574\uff08\u5728\u5eab\u5897\uff09'),
  'inventory_adjustment'
FROM monthly_inv_change ic WHERE ic.inv_change > 0

UNION ALL

-- 在庫減少: Dr.\u4ed5\u5165\u9ad8 / Cr.\u5546\u54c1
SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'debit', '\u4ed5\u5165\u9ad8',
  ABS(ic.inv_change), CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '\u5e74',
         CAST(ic.month AS STRING), '\u6708 \u6708\u6b21\u68da\u5378\u8abf\u6574\uff08\u5728\u5eab\u6e1b\uff09'),
  'inventory_adjustment'
FROM monthly_inv_change ic WHERE ic.inv_change < 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'credit', '\u5546\u54c1',
  ABS(ic.inv_change), CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '\u5e74',
         CAST(ic.month AS STRING), '\u6708 \u6708\u6b21\u68da\u5378\u8abf\u6574\uff08\u5728\u5eab\u6e1b\uff09'),
  'inventory_adjustment'
FROM monthly_inv_change ic WHERE ic.inv_change < 0
"""

try:
    client.delete_table(INV_VIEW_ID)
    print('  既存VIEW削除')
except Exception:
    pass
v = bigquery.Table(INV_VIEW_ID)
v.view_query = inv_view_sql
client.create_table(v)
print('  VIEW作成完了')

print('\n>>> ここで BQ sync を実行してください <<<')
print('    cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py')
print('    完了後、このスクリプトの続き (step3_add_adjustments.py) を実行してください')
