"""
商品計上方式への切り替え

変更内容:
1. NocoDB: 代行会社の freee勘定科目_id 109(仕入高) → 17(商品)
2. BQ: inventory_journal_view の sanpunpo_net を COGS全額に更新
   - FY2023: -93389(在庫変動) → 234178(=327567-93389 COGS全額)
   - FY2024: +93389(在庫変動) → 912369(=818980+93389 COGS全額)
3. BQ: inventory_journal_view 再デプロイ
4. BQ: journal_entries VIEW 再デプロイ（inventory_journal_view参照のため再作成）

P/L 影響: ゼロ（代行会社が仕入高→商品になり、inventory_journalで同額COGS計上）
"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'

# ===== Step 1: NocoDB 更新 =====
print('=== Step 1: NocoDB 代行会社 freee勘定科目_id 109→17 ===')
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute('SELECT COUNT(*) FROM "nc_opau___代行会社" WHERE "nc_opau___freee勘定科目_id" = 109')
before_cnt = cur.fetchone()[0]
print(f'  変更前: {before_cnt}件')

cur.execute('UPDATE "nc_opau___代行会社" SET "nc_opau___freee勘定科目_id" = 17 WHERE "nc_opau___freee勘定科目_id" = 109')
updated = cur.rowcount
conn.commit()
print(f'  更新: {updated}件')

cur.execute('SELECT COUNT(*) FROM "nc_opau___代行会社" WHERE "nc_opau___freee勘定科目_id" = 17')
after_cnt = cur.fetchone()[0]
print(f'  変更後(商品): {after_cnt}件')
conn.close()

# ===== Step 2: BQ inventory_journal_view デプロイ =====
print('\n=== Step 2: BQ inventory_journal_view デプロイ ===')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

INV_VIEW_ID = 'main-project-477501.accounting.inventory_journal_view'

inv_view_sql = r"""
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

-- ③ 商品計上方式 COGS全額（購入 + 期首 - 期末）
-- FY2023/2024: MF確定申告値から算出してハードコード
--   FY2023: 購入327567 + 期首0 - 期末93389 = 234178
--   FY2024: 購入818980 + 期首93389 - 期末0 = 912369
-- FY2025+: SP-APIデータから自動計算（閉じた年度のみ）
sanpunpo_net AS (
  -- FY2023: 支出296970 - 返金30597 = 266373; + 期首0 - 期末93389 = 172984
  SELECT 2023 AS fiscal_year, CAST(172984 AS INT64) AS net_adjustment
  UNION ALL
  -- FY2024: 支出818330 - 返金650 = 817680; + 期首93389 - 期末0 = 911069
  SELECT 2024, CAST(911069 AS INT64)
  UNION ALL
  -- FY2025+完了年: 購入額(商品計上) + 期首 - 期末 をハードコード追加
  -- ※ 年度確定時に手動追加。現在の当年度(section C)はSP-API月次で対応。
  SELECT
    COALESCE(o.fiscal_year, c.fiscal_year) AS fiscal_year,
    CAST(COALESCE(o.opening_value, 0) - COALESCE(c.closing_value, 0) AS INT64) AS net_adjustment
  FROM opening_values o
  FULL OUTER JOIN closing_values c ON o.fiscal_year = c.fiscal_year
  WHERE COALESCE(o.fiscal_year, c.fiscal_year) >= 2025
    AND c.fiscal_year IS NOT NULL
    AND FALSE  -- 現時点では無効化。FY2025確定時に除去してハードコード値を追加
),

-- ④ 1-11月合計
jan_nov_totals AS (
  SELECT year, SUM(cogs_amount) AS total_cogs
  FROM monthly_totals
  WHERE month <= 11
  GROUP BY year
),

-- ⑤ 12月エントリ = COGS全額 - 1-11月COGS
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
"""

# inventory_journal_view を削除・再作成
try:
    client.delete_table(INV_VIEW_ID)
    print(f'  既存VIEW削除: {INV_VIEW_ID}')
except Exception:
    pass

inv_view = bigquery.Table(INV_VIEW_ID)
inv_view.view_query = inv_view_sql
client.create_table(inv_view)
print(f'  VIEW作成完了: {INV_VIEW_ID}')

# ===== Step 3: P/L 事前検証（BQ sync前に現在値確認）=====
print('\n=== Step 3: 現在のP/L確認 ===')
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
    mark = ' ✅' if r.net_pl == exp else f' ← 期待値 {exp}'
    print(f'  FY{r.fiscal_year}: {r.net_pl:>12,.0f}{mark}')

print('\n次のステップ: BQ sync を実行してください')
print('  cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py')
print('その後、再度このスクリプトの検証部分のみ再実行するか、別途確認スクリプトを実行')
