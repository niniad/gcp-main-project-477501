"""
Step 3: inventory_journal_view を月次三分法に更新

三分法ルール:
- 代行会社支払 → 仕入高（Step1/2で変更済み）
- inventory_journal_view → 月次在庫増減のみ（商品↑↓/仕入高↓↑）

月次COGS = 代行会社仕入高 + 月初在庫 - 月末在庫
年間COGS（自動）:
  FY2023: 0 + 266373 - 93389 = 172984  ← 確定申告値一致
  FY2024: 93389 + 817680 - 0 = 911069  ← 確定申告値一致
  FY2025+: 同ルール、調整なし

sanpunpo_netハードコード・12月調整エントリ 不要
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

INV_VIEW_ID = 'main-project-477501.accounting.inventory_journal_view'

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

-- ============================================
-- 在庫増加: Dr.商品 / Cr.仕入高
-- （当月末在庫 > 前月末在庫 → 仕入高を減額、商品BSを増額）
-- ============================================
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
FROM monthly_inv_change ic
WHERE ic.inv_change > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'credit', '\u4ed5\u5165\u9ad8',
  ic.inv_change,
  CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '\u5e74',
         CAST(ic.month AS STRING), '\u6708 \u6708\u6b21\u68da\u5378\u8abf\u6574\uff08\u5728\u5eab\u5897\uff09'),
  'inventory_adjustment'
FROM monthly_inv_change ic
WHERE ic.inv_change > 0

UNION ALL

-- ============================================
-- 在庫減少: Dr.仕入高 / Cr.商品
-- （当月末在庫 < 前月末在庫 → 仕入高を増額、商品BSを減額）
-- ============================================
SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'debit', '\u4ed5\u5165\u9ad8',
  ABS(ic.inv_change),
  CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '\u5e74',
         CAST(ic.month AS STRING), '\u6708 \u6708\u6b21\u68da\u5378\u8abf\u6574\uff08\u5728\u5eab\u6e1b\uff09'),
  'inventory_adjustment'
FROM monthly_inv_change ic
WHERE ic.inv_change < 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(ic.year AS STRING), '_',
         LPAD(CAST(ic.month AS STRING), 2, '0'), '_adj'),
  LAST_DAY(DATE(ic.year, ic.month, 1)),
  ic.year, 'credit', '\u5546\u54c1',
  ABS(ic.inv_change),
  CAST(NULL AS INT64),
  CONCAT(CAST(ic.year AS STRING), '\u5e74',
         CAST(ic.month AS STRING), '\u6708 \u6708\u6b21\u68da\u5378\u8abf\u6574\uff08\u5728\u5eab\u6e1b\uff09'),
  'inventory_adjustment'
FROM monthly_inv_change ic
WHERE ic.inv_change < 0
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

# ========== 検証 ==========
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
    mark = ' OK' if r.net_pl == exp else f' <- 期待値{exp}'
    print(f'  FY{r.fiscal_year}: {r.net_pl:>12,.0f}{mark}')

print('\n=== 商品 年度別残高（累積）===')
q_inv = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS net
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '商品'
GROUP BY 1 ORDER BY 1
"""
cum = 0
for r in client.query(q_inv).result():
    cum += r.net
    mark = ' OK(93389)' if r.fiscal_year == 2023 and abs(cum - 93389) < 5 else (
           ' OK(0)'     if r.fiscal_year == 2024 and abs(cum) < 5 else '')
    print(f'  FY{r.fiscal_year}: 当年¥{r.net:,.0f}  累積¥{cum:,.0f}{mark}')

print('\n=== FY2023 月次 仕入高 内訳 ===')
q_monthly = """
SELECT
  EXTRACT(MONTH FROM journal_date) AS month,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS shiire_net
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2023 AND account_name = '仕入高'
GROUP BY 1 ORDER BY 1
"""
total = 0
for r in client.query(q_monthly).result():
    total += r.shiire_net
    print(f'  {r.month:2}月: ¥{r.shiire_net:>8,.0f}')
print(f'  合計: ¥{total:,.0f}  (期待値: ¥172,984)')
