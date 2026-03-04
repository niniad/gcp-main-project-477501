"""
journal_entries VIEW を修正
楽天銀行の振替_id付きエントリのうち、相手勘定が保有口座（ESPRIME/THE直行便/YP）の
ものを VIEW に含めるよう変更。

修正理由:
  楽天銀行 → ESPRIME/THE直行便/YP への送金は振替テーブルでリンクされているため
  振替_id IS NOT NULL となり journal_entries から除外されていた。
  しかし代行会社テーブル側の入金（振替_id付き）は既に除外されているため、
  楽天銀行側だけ含めれば二重計上にはならない。

変更箇所:
  ③ 楽天銀行 銀行側: AND `振替_id` IS NULL
    →  AND (`振替_id` IS NULL OR ai.nocodb_id IN (3, 5, 7))
  ③ 楽天銀行 相手勘定側: AND r.`振替_id` IS NULL
    →  AND (r.`振替_id` IS NULL OR ai.nocodb_id IN (3, 5, 7))
    ※ 相手勘定側の JOIN 条件が r.freee勘定科目_id → ai なので条件は同じ

  保有口座 nocodb_id:
    3 = THE直行便
    5 = ESPRIME
    7 = YP（イーウーパスポート）
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')
VIEW_ID = 'main-project-477501.accounting.journal_entries'

view_sql = r"""
SELECT
  CONCAT('amazon_', CAST(a.nocodb_id AS STRING)) AS source_id,
  SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date) AS journal_date,
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) AS fiscal_year,
  CASE WHEN a.amount >= 0 THEN 'debit' ELSE 'credit' END AS entry_side,
  'Amazon出品アカウント' AS account_name,
  ABS(a.amount) AS amount_jpy,
  NULL AS tax_code,
  a.description,
  'amazon_settlement' AS source_table
FROM `main-project-477501.nocodb.amazon_account_statements` a
WHERE a.`振替_id` IS NULL
  AND a.amount IS NOT NULL
  AND a.transaction_date IS NOT NULL

UNION ALL

-- Amazon出品アカウント（相手勘定側: 非振替の収支）
SELECT
  CONCAT('amazon_', CAST(a.nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)),
  CASE WHEN a.amount >= 0 THEN 'credit' ELSE 'debit' END,
  ai.account_name,
  ABS(a.amount),
  NULL,
  a.description,
  'amazon_settlement'
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.`振替_id` IS NULL
  AND a.amount IS NOT NULL
  AND a.transaction_date IS NOT NULL

UNION ALL

-- PayPay 銀行（銀行側）
SELECT CAST(nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', transaction_date)),
  CASE WHEN amount >= 0 THEN 'debit' ELSE 'credit' END,
  'PayPay銀行', ABS(amount), NULL, description, 'paypay_bank'
FROM `main-project-477501.nocodb.paypay_bank_statements`
WHERE amount IS NOT NULL AND `freee勘定科目_id` IS NOT NULL
  AND `振替_id` IS NULL

UNION ALL

-- PayPay 銀行（相手勘定側）
SELECT CAST(p.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', p.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', p.transaction_date)),
  CASE WHEN p.amount >= 0 THEN 'credit' ELSE 'debit' END,
  ai.account_name, ABS(p.amount), NULL, p.description, 'paypay_bank'
FROM `main-project-477501.nocodb.paypay_bank_statements` p
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON p.`freee勘定科目_id` = ai.nocodb_id
WHERE p.amount IS NOT NULL AND p.`freee勘定科目_id` IS NOT NULL
  AND p.`振替_id` IS NULL

UNION ALL

-- 楽天銀行（銀行側: 通常取引 + 保有口座への送金）
-- 保有口座: 3=THE直行便, 5=ESPRIME, 7=YP
SELECT CAST(r.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', r.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', r.transaction_date)),
  CASE WHEN r.amount_jpy >= 0 THEN 'debit' ELSE 'credit' END,
  '楽天銀行', ABS(r.amount_jpy), NULL, r.counterparty_description, 'rakuten_bank'
FROM `main-project-477501.nocodb.rakuten_bank_statements` r
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON r.`freee勘定科目_id` = ai.nocodb_id
WHERE r.amount_jpy IS NOT NULL AND r.`freee勘定科目_id` IS NOT NULL
  AND (r.`振替_id` IS NULL OR ai.nocodb_id IN (3, 5, 7))

UNION ALL

-- 楽天銀行（相手勘定側: 通常取引 + 保有口座への送金）
SELECT CAST(r.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', r.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', r.transaction_date)),
  CASE WHEN r.amount_jpy >= 0 THEN 'credit' ELSE 'debit' END,
  ai.account_name,
  ABS(r.amount_jpy), NULL, r.counterparty_description, 'rakuten_bank'
FROM `main-project-477501.nocodb.rakuten_bank_statements` r
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON r.`freee勘定科目_id` = ai.nocodb_id
WHERE r.amount_jpy IS NOT NULL AND r.`freee勘定科目_id` IS NOT NULL
  AND (r.`振替_id` IS NULL OR ai.nocodb_id IN (3, 5, 7))

UNION ALL

-- NTT Finance（経費側）
-- NOTE: NTT の振替_id は月次支払バッチへのリンクであり振替フラグではない。
-- is_transfer フィルタを維持する。
SELECT CAST(n.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', n.usage_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', n.usage_date)),
  CASE WHEN n.usage_amount < 0 THEN 'debit' ELSE 'credit' END,
  COALESCE(ntr.account_name, ai.account_name) AS account_name,
  ABS(CAST(n.usage_amount AS INT64)),
  NULL,
  CASE WHEN ntr.memo IS NOT NULL
    THEN CONCAT(COALESCE(n.merchant_name, n.description, ''), ' [', ntr.memo, ']')
    ELSE COALESCE(n.merchant_name, n.description) END,
  'ntt_finance'
FROM `main-project-477501.nocodb.ntt_finance_statements` n
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON n.`freee勘定科目_id` = ai.nocodb_id
LEFT JOIN `main-project-477501.accounting.merchant_account_rules` ntr
  ON ntr.source_table = 'ntt_finance' AND ntr.match_type = 'EXACT' AND n.merchant_name = ntr.match_value
WHERE (n.is_transfer IS FALSE OR n.is_transfer IS NULL)
  AND n.usage_amount IS NOT NULL
  AND (n.`freee勘定科目_id` IS NOT NULL OR ntr.account_name IS NOT NULL)

UNION ALL

-- NTT Finance（カード負債側）
SELECT CAST(n.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', n.usage_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', n.usage_date)),
  CASE WHEN n.usage_amount < 0 THEN 'credit' ELSE 'debit' END,
  '未払金',
  ABS(CAST(n.usage_amount AS INT64)),
  NULL,
  CASE WHEN ntr.memo IS NOT NULL
    THEN CONCAT(COALESCE(n.merchant_name, n.description, ''), ' [', ntr.memo, ']')
    ELSE COALESCE(n.merchant_name, n.description) END,
  'ntt_finance'
FROM `main-project-477501.nocodb.ntt_finance_statements` n
LEFT JOIN `main-project-477501.accounting.merchant_account_rules` ntr
  ON ntr.source_table = 'ntt_finance' AND ntr.match_type = 'EXACT' AND n.merchant_name = ntr.match_value
WHERE (n.is_transfer IS FALSE OR n.is_transfer IS NULL)
  AND n.usage_amount IS NOT NULL
  AND (n.`freee勘定科目_id` IS NOT NULL OR ntr.account_name IS NOT NULL)

UNION ALL

-- 代行会社（経費側）
SELECT CAST(a.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)),
  CASE WHEN a.amount_foreign < 0 THEN 'debit' ELSE 'credit' END,
  ai.account_name,
  CAST(ABS(ROUND(a.amount_foreign * COALESCE(a.exchange_rate, 1))) AS INT64),
  NULL,
  TRIM(CONCAT(COALESCE(a.cost_category, ''), ' ', COALESCE(a.memo, ''))),
  'agency_transactions'
FROM `main-project-477501.nocodb.agency_transactions` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON CAST(a.`freee勘定科目_id` AS INT64) = ai.nocodb_id
WHERE a.`振替_id` IS NULL
  AND a.amount_foreign IS NOT NULL AND a.`freee勘定科目_id` IS NOT NULL

UNION ALL

-- 代行会社（口座側）
SELECT CAST(a.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)),
  CASE WHEN a.amount_foreign < 0 THEN 'credit' ELSE 'debit' END,
  COALESCE(ai.account_name, a.payment_account),
  CAST(ABS(ROUND(a.amount_foreign * COALESCE(a.exchange_rate, 1))) AS INT64),
  NULL,
  TRIM(CONCAT(COALESCE(a.cost_category, ''), ' ', COALESCE(a.memo, ''))),
  'agency_transactions'
FROM `main-project-477501.nocodb.agency_transactions` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.payment_account = ai.account_name
WHERE a.`振替_id` IS NULL
  AND a.amount_foreign IS NOT NULL AND a.`freee勘定科目_id` IS NOT NULL

UNION ALL

-- セールモンスター（売上高側）
SELECT CONCAT('sm_', CAST(nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', sale_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', sale_date)),
  CASE WHEN sale_category = '販売売上' THEN 'credit' ELSE 'debit' END,
  '売上高', ABS(total_amount_incl_tax), NULL,
  CONCAT(COALESCE(marketplace, ''), ': ', SUBSTR(COALESCE(detail_description, ''), 1, 60)),
  'sale_monster'
FROM `main-project-477501.nocodb.sale_monster_reports`
WHERE total_amount_incl_tax IS NOT NULL AND sale_date IS NOT NULL

UNION ALL

-- セールモンスター（セールモンスター口座側）
SELECT CONCAT('sm_', CAST(nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', sale_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', sale_date)),
  CASE WHEN sale_category = '販売売上' THEN 'debit' ELSE 'credit' END,
  'セールモンスター', ABS(total_amount_incl_tax), NULL,
  CONCAT(COALESCE(marketplace, ''), ': ', SUBSTR(COALESCE(detail_description, ''), 1, 60)),
  'sale_monster'
FROM `main-project-477501.nocodb.sale_monster_reports`
WHERE total_amount_incl_tax IS NOT NULL AND sale_date IS NOT NULL

UNION ALL

-- 手動仕訳（借方側）
SELECT
  CONCAT('manual_', CAST(m.nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', m.journal_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', m.journal_date)),
  'debit',
  ai_dr.account_name,
  m.amount,
  NULL,
  m.description,
  'manual_journal'
FROM `main-project-477501.nocodb.manual_journal_entries` m
LEFT JOIN `main-project-477501.nocodb.account_items` ai_dr ON m.debit_account_id = ai_dr.nocodb_id
WHERE m.journal_date IS NOT NULL AND m.amount IS NOT NULL

UNION ALL

-- 手動仕訳（貸方側）
SELECT
  CONCAT('manual_', CAST(m.nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', m.journal_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', m.journal_date)),
  'credit',
  ai_cr.account_name,
  m.amount,
  NULL,
  m.description,
  'manual_journal'
FROM `main-project-477501.nocodb.manual_journal_entries` m
LEFT JOIN `main-project-477501.nocodb.account_items` ai_cr ON m.credit_account_id = ai_cr.nocodb_id
WHERE m.journal_date IS NOT NULL AND m.amount IS NOT NULL

UNION ALL

-- 事業主借（借方側）
SELECT
  CONCAT('oc_', CAST(oc.nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', oc.journal_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', oc.journal_date)),
  'debit',
  ai_dr.account_name,
  oc.amount,
  NULL,
  oc.description,
  'owner_contribution'
FROM `main-project-477501.nocodb.owner_contribution_entries` oc
LEFT JOIN `main-project-477501.nocodb.account_items` ai_dr ON oc.debit_account_id = ai_dr.nocodb_id
WHERE oc.journal_date IS NOT NULL AND oc.amount IS NOT NULL

UNION ALL

-- 事業主借（貸方側 - 常に '事業主借'）
SELECT
  CONCAT('oc_', CAST(oc.nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', oc.journal_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', oc.journal_date)),
  'credit',
  '事業主借' AS account_name,
  oc.amount,
  NULL,
  oc.description,
  'owner_contribution'
FROM `main-project-477501.nocodb.owner_contribution_entries` oc
WHERE oc.journal_date IS NOT NULL AND oc.amount IS NOT NULL

UNION ALL

-- 棚卸仕訳
SELECT
  source_id,
  journal_date,
  fiscal_year,
  entry_side,
  account_name,
  amount_jpy,
  tax_code,
  description,
  source_table
FROM `main-project-477501.accounting.inventory_journal_view`
"""

view = bigquery.Table(VIEW_ID)
view.view_query = view_sql

try:
    client.delete_table(VIEW_ID)
    print(f'既存VIEW削除: {VIEW_ID}')
except Exception:
    pass

created = client.create_table(view)
print(f'VIEW作成完了: {created.table_id}')

# P/L検証（FY2023/2024）
print('\n=== P/L 検証 ===')
q_pl = """
SELECT
  fiscal_year,
  SUM(CASE WHEN entry_side='credit' THEN amount_jpy ELSE 0 END) -
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE 0 END) AS net_pl
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name NOT IN (
  '楽天銀行','PayPay銀行','Amazon出品アカウント','未払金',
  'THE直行便','ESPRIME','YP','セールモンスター','事業主借','開業費'
)
GROUP BY 1 ORDER BY 1
"""
for r in client.query(q_pl).result():
    expected = {2023: -1340610, 2024: -1088882}
    exp = expected.get(r.fiscal_year, '?')
    mark = ' ✅' if r.net_pl == exp else f' ← 期待値 {exp}'
    print(f'  FY{r.fiscal_year}: {r.net_pl:>12,.0f}{mark}')

# BS 保有口座残高確認
print('\n=== 保有口座 BS 残高（全期間）===')
q_bs = """
SELECT
  account_name,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name IN ('楽天銀行','PayPay銀行','THE直行便','ESPRIME','YP','Amazon出品アカウント','セールモンスター')
GROUP BY 1 ORDER BY 1
"""
for r in client.query(q_bs).result():
    print(f'  {r.account_name:<20} {r.balance:>12,.0f}')
