"""
inventory_journal_view を月次棚卸法に変更
COGS = 月初在庫金額 + 当月仕入 - 月末在庫金額

変更前: 出荷数 × 標準原価
変更後: 月次在庫残高の変動から算出（より正確）

年間合計:
  FY2023/2024: hard-code確定値（変わらず）
  FY2025+: SP-APIデータから自動計算（当年度は月次のみ）
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

INV_VIEW_ID = 'main-project-477501.accounting.inventory_journal_view'

inv_view_sql = """
WITH
-- ① 月次期末在庫金額（全月）
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

-- ② 月次仕入金額（代行会社 商品エントリ）
monthly_purchases AS (
  SELECT
    EXTRACT(YEAR  FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) AS year,
    EXTRACT(MONTH FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) AS month,
    CAST(SUM(ABS(ROUND(a.amount_foreign * COALESCE(a.exchange_rate, 1)))) AS INT64)
      AS purchase_amount
  FROM `main-project-477501.nocodb.agency_transactions` a
  JOIN `main-project-477501.nocodb.account_items` ai
    ON CAST(a.`freee勘定科目_id` AS INT64) = ai.nocodb_id
  WHERE ai.account_name = '\u5546\u54c1'  -- 商品
    AND a.`\u632f\u66ff_id` IS NULL       -- 振替_id
    AND a.amount_foreign < 0
  GROUP BY 1, 2
),

-- ③ 月次データ統合（在庫のある月 + 仕入のある月）
all_months AS (
  SELECT
    COALESCE(mv.year, mp.year) AS year,
    COALESCE(mv.month, mp.month) AS month,
    COALESCE(mv.closing_value, 0) AS closing_value,
    COALESCE(mp.purchase_amount, 0) AS purchases
  FROM monthly_closing_value mv
  FULL OUTER JOIN monthly_purchases mp
    ON mv.year = mp.year AND mv.month = mp.month
),

-- ④ 月次COGS = 月初在庫 + 当月仕入 - 月末在庫
-- 月初在庫 = 前月末在庫（self-join）
monthly_cogs_calc AS (
  SELECT
    curr.year, curr.month,
    curr.closing_value,
    curr.purchases,
    COALESCE(prev.closing_value, 0) AS opening_value,
    COALESCE(prev.closing_value, 0) + curr.purchases - curr.closing_value AS cogs_amount
  FROM all_months curr
  LEFT JOIN all_months prev
    ON (curr.month = 1  AND prev.year = curr.year - 1 AND prev.month = 12)
    OR (curr.month > 1  AND prev.year = curr.year     AND prev.month = curr.month - 1)
),

-- ⑤ 年末在庫（12月）スナップショット（完了年度の年次調整用）
closing_values AS (
  SELECT year AS fiscal_year, closing_value
  FROM monthly_closing_value
  WHERE month = 12
),
opening_values AS (
  SELECT year + 1 AS fiscal_year, closing_value AS opening_value
  FROM monthly_closing_value
  WHERE month = 12
),

-- ⑥ 年間COGS目標（商品計上方式: 購入net + 期首 - 期末）
-- FY2023: (296970-30597)+0-93389=172984
-- FY2024: (818330-650)+93389-0=911069
-- FY2025+完了時: 手動追記
sanpunpo_net AS (
  SELECT 2023 AS fiscal_year, CAST(172984 AS INT64) AS net_adjustment
  UNION ALL
  SELECT 2024, CAST(911069 AS INT64)
  UNION ALL
  SELECT
    COALESCE(o.fiscal_year, c.fiscal_year) AS fiscal_year,
    CAST(COALESCE(o.opening_value, 0) - COALESCE(c.closing_value, 0) AS INT64)
  FROM opening_values o
  FULL OUTER JOIN closing_values c ON o.fiscal_year = c.fiscal_year
  WHERE COALESCE(o.fiscal_year, c.fiscal_year) >= 2025
    AND c.fiscal_year IS NOT NULL
    AND FALSE
),

-- ⑦ 1-11月合計（完了年度）
jan_nov_totals AS (
  SELECT year, SUM(GREATEST(cogs_amount, 0)) AS total_cogs
  FROM monthly_cogs_calc
  WHERE month <= 11
  GROUP BY year
),

-- ⑧ 12月調整額 = 年間目標 - 1-11月計算COGS
dec_entries AS (
  SELECT s.fiscal_year AS year,
    s.net_adjustment - COALESCE(jn.total_cogs, 0) AS dec_amount
  FROM sanpunpo_net s
  LEFT JOIN jan_nov_totals jn ON jn.year = s.fiscal_year
)

-- ============================
-- A: 完了年度 1-11月 月次COGS
-- ============================
SELECT
  CONCAT('inv_', CAST(mc.year AS STRING), '_',
         LPAD(CAST(mc.month AS STRING), 2, '0'), '_cogs') AS source_id,
  LAST_DAY(DATE(mc.year, mc.month, 1)) AS journal_date,
  mc.year AS fiscal_year,
  'debit' AS entry_side,
  '\u4ed5\u5165\u9ad8' AS account_name,
  GREATEST(mc.cogs_amount, 0) AS amount_jpy,
  CAST(NULL AS INT64) AS tax_code,
  CONCAT(CAST(mc.year AS STRING), '\u5e74',
         CAST(mc.month AS STRING), '\u6708 \u6708\u6b21\u58f2\u4e0a\u539f\u4fa1') AS description,
  'inventory_adjustment' AS source_table
FROM monthly_cogs_calc mc
INNER JOIN sanpunpo_net s ON mc.year = s.fiscal_year
WHERE mc.month <= 11 AND mc.cogs_amount > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(mc.year AS STRING), '_',
         LPAD(CAST(mc.month AS STRING), 2, '0'), '_cogs'),
  LAST_DAY(DATE(mc.year, mc.month, 1)),
  mc.year, 'credit', '\u5546\u54c1',
  GREATEST(mc.cogs_amount, 0),
  CAST(NULL AS INT64),
  CONCAT(CAST(mc.year AS STRING), '\u5e74',
         CAST(mc.month AS STRING), '\u6708 \u6708\u6b21\u58f2\u4e0a\u539f\u4fa1'),
  'inventory_adjustment'
FROM monthly_cogs_calc mc
INNER JOIN sanpunpo_net s ON mc.year = s.fiscal_year
WHERE mc.month <= 11 AND mc.cogs_amount > 0

UNION ALL

-- ============================
-- B: 完了年度 12月（年次調整込）
-- ============================
SELECT
  CONCAT('inv_', CAST(de.year AS STRING), '_12_cogs') AS source_id,
  DATE(de.year, 12, 31),
  de.year, 'debit',
  CASE WHEN de.dec_amount >= 0 THEN '\u4ed5\u5165\u9ad8' ELSE '\u5546\u54c1' END,
  ABS(de.dec_amount), CAST(NULL AS INT64),
  CONCAT(CAST(de.year AS STRING),
         '\u5e74\uff11\uff12\u6708 \u58f2\u4e0a\u539f\u4fa1\uff08\u5e74\u6b21\u8abf\u6574\u8fbc\uff09'),
  'inventory_adjustment'
FROM dec_entries de WHERE de.dec_amount != 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(de.year AS STRING), '_12_cogs'),
  DATE(de.year, 12, 31),
  de.year, 'credit',
  CASE WHEN de.dec_amount >= 0 THEN '\u5546\u54c1' ELSE '\u4ed5\u5165\u9ad8' END,
  ABS(de.dec_amount), CAST(NULL AS INT64),
  CONCAT(CAST(de.year AS STRING),
         '\u5e74\uff11\uff12\u6708 \u58f2\u4e0a\u539f\u4fa1\uff08\u5e74\u6b21\u8abf\u6574\u8fbc\uff09'),
  'inventory_adjustment'
FROM dec_entries de WHERE de.dec_amount != 0

UNION ALL

-- ============================
-- C: 当年度（未完了）月次COGS
-- ============================
SELECT
  CONCAT('inv_', CAST(mc.year AS STRING), '_',
         LPAD(CAST(mc.month AS STRING), 2, '0'), '_cogs'),
  LAST_DAY(DATE(mc.year, mc.month, 1)),
  mc.year, 'debit', '\u4ed5\u5165\u9ad8',
  GREATEST(mc.cogs_amount, 0),
  CAST(NULL AS INT64),
  CONCAT(CAST(mc.year AS STRING), '\u5e74',
         CAST(mc.month AS STRING), '\u6708 \u6708\u6b21\u58f2\u4e0a\u539f\u4fa1'),
  'inventory_adjustment'
FROM monthly_cogs_calc mc
WHERE mc.year NOT IN (SELECT fiscal_year FROM sanpunpo_net)
  AND mc.cogs_amount > 0

UNION ALL

SELECT
  CONCAT('inv_', CAST(mc.year AS STRING), '_',
         LPAD(CAST(mc.month AS STRING), 2, '0'), '_cogs'),
  LAST_DAY(DATE(mc.year, mc.month, 1)),
  mc.year, 'credit', '\u5546\u54c1',
  GREATEST(mc.cogs_amount, 0),
  CAST(NULL AS INT64),
  CONCAT(CAST(mc.year AS STRING), '\u5e74',
         CAST(mc.month AS STRING), '\u6708 \u6708\u6b21\u58f2\u4e0a\u539f\u4fa1'),
  'inventory_adjustment'
FROM monthly_cogs_calc mc
WHERE mc.year NOT IN (SELECT fiscal_year FROM sanpunpo_net)
  AND mc.cogs_amount > 0
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

# 検証
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

print('\n=== 2023年 月次 粗利（改善後）===')
q2 = """
SELECT
  EXTRACT(MONTH FROM journal_date) AS month,
  SUM(CASE WHEN account_name='売上高' AND entry_side='credit' THEN amount_jpy
           WHEN account_name='売上高' AND entry_side='debit'  THEN -amount_jpy ELSE 0 END)
    - SUM(CASE WHEN account_name IN ('売上値引高','売上戻り高') AND entry_side='debit' THEN amount_jpy
               WHEN account_name IN ('売上値引高','売上戻り高') AND entry_side='credit' THEN -amount_jpy ELSE 0 END)
    AS revenue,
  SUM(CASE WHEN account_name='仕入高' AND entry_side='debit'  THEN amount_jpy
           WHEN account_name='仕入高' AND entry_side='credit' THEN -amount_jpy ELSE 0 END) AS cogs
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2023
GROUP BY 1 ORDER BY 1
"""
for r in client.query(q2).result():
    gross = r.revenue - r.cogs
    ratio = gross / r.revenue * 100 if r.revenue else 0
    print(f'  {r.month:2}月: 売上¥{r.revenue:>7,.0f}  COGS¥{r.cogs:>7,.0f}  粗利{ratio:>6.1f}%')

print('\n=== 商品 年度別残高（累積）===')
q3 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS net
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '商品'
GROUP BY 1 ORDER BY 1
"""
cum = 0
for r in client.query(q3).result():
    cum += r.net
    mark = ' OK(93389)' if r.fiscal_year == 2023 and abs(cum-93389) < 100 else (
           ' OK(0)' if r.fiscal_year == 2024 and abs(cum) < 100 else '')
    print(f'  FY{r.fiscal_year}: 累積¥{cum:,.0f}{mark}')
