"""
BQ accounting.journal_entries VIEW 定義・デプロイスクリプト

VIEW構成（8ソース）:
  ① Amazon: settlement_journal_view → CROSS JOIN UNNEST → 14科目展開
  ② PayPay銀行: paypay_bank_statements (振替_id IS NULL OR 科目IN(5,6))
  ③ 楽天銀行: rakuten_bank_statements (振替_id IS NULL OR 科目IN(3,5,6,7,8,70))
  ④ NTT Finance: ntt_finance_statements (is_transfer IS FALSE)
  ⑤ 代行会社: agency_transactions (振替_id IS NULL)
  ⑥ セールモンスター: sale_monster_reports → 売上高/セールモンスター
  ⑦ 手動仕訳: manual_journal_entries（事業主借も含む）
  ⑧ 棚卸仕訳: inventory_journal_view

実行: uv run --with google-cloud-bigquery python scripts/journal_entries_view.py
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

BQ_PROJECT = "main-project-477501"

NEW_VIEW_SQL = """
WITH amazon_items AS (
  SELECT
    CONCAT('amazon_', CAST(settlement_id AS STRING), '_', t.col) AS source_id,
    DATE(booking_date) AS journal_date,
    EXTRACT(YEAR FROM booking_date) AS fiscal_year,
    t.amount,
    t.counterpart,
    CONCAT('settlement ', CAST(settlement_id AS STRING), ': ', t.label) AS description
  FROM `main-project-477501.accounting.settlement_journal_view`
  CROSS JOIN UNNEST([
    STRUCT('sales_product'        AS col, CAST(sales_product        AS INT64) AS amount, '売上高'     AS counterpart, '商品売上'       AS label),
    STRUCT('sales_shipping',             CAST(sales_shipping        AS INT64),            '売上高',                   '受取配送料'),
    STRUCT('sales_refunds',              CAST(sales_refunds         AS INT64),            '売上戻り高',               '返品'),
    STRUCT('sales_promotions',           CAST(sales_promotions      AS INT64),            '売上値引高',               'プロモーション'),
    STRUCT('income_reimbursement',       CAST(income_reimbursement  AS INT64),            '雑収入',                   '補償'),
    STRUCT('expense_commission',         CAST(expense_commission    AS INT64),            '販売手数料',               '販売手数料'),
    STRUCT('expense_fba_shipping',       CAST(expense_fba_shipping  AS INT64),            '荷造運賃',                 'FBA配送費'),
    STRUCT('expense_points',             CAST(expense_points        AS INT64),            '売上値引高',               'Amazonポイント'),
    STRUCT('expense_advertising',        CAST(expense_advertising   AS INT64),            '広告宣伝費',               '広告費'),
    STRUCT('expense_storage',            CAST(expense_storage       AS INT64),            '地代家賃',                 '保管費'),
    STRUCT('expense_subscription',       CAST(expense_subscription  AS INT64),            '諸会費',                   '月額料'),
    STRUCT('expense_other',              CAST(expense_other         AS INT64),            '雑費',                     'その他'),
    STRUCT('reserve_withheld',           CAST(reserve_withheld      AS INT64),            '仮払金',                   '引当金'),
    STRUCT('reserve_released',           CAST(reserve_released      AS INT64),            '仮払金',                   '引当金解放')
  ]) t
  WHERE t.amount != 0 AND booking_date IS NOT NULL
)

-- ① Amazon出品アカウント側（Dr/Cr）
SELECT
  source_id, journal_date, fiscal_year,
  CASE WHEN amount >= 0 THEN 'debit' ELSE 'credit' END AS entry_side,
  'Amazon出品アカウント' AS account_name,
  ABS(amount) AS amount_jpy,
  NULL AS tax_code,
  description,
  'amazon_settlement' AS source_table
FROM amazon_items

UNION ALL

-- ① Amazon相手勘定側（Cr/Dr）
SELECT
  source_id, journal_date, fiscal_year,
  CASE WHEN amount >= 0 THEN 'credit' ELSE 'debit' END AS entry_side,
  counterpart AS account_name,
  ABS(amount) AS amount_jpy,
  NULL AS tax_code,
  description,
  'amazon_settlement' AS source_table
FROM amazon_items

UNION ALL

-- ② PayPay 銀行（銀行側: 通常取引 + Amazon→PayPay入金 + PayPay→ESPRIME送金）
SELECT CAST(p.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', p.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', p.transaction_date)),
  CASE WHEN p.amount >= 0 THEN 'debit' ELSE 'credit' END,
  'PayPay銀行', ABS(p.amount), NULL, p.description, 'paypay_bank'
FROM `main-project-477501.nocodb.paypay_bank_statements` p
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON p.`freee勘定科目_id` = ai.nocodb_id
WHERE p.amount IS NOT NULL AND p.`freee勘定科目_id` IS NOT NULL
  AND (p.`振替_id` IS NULL OR ai.nocodb_id IN (5, 6))

UNION ALL

-- ② PayPay 銀行（相手勘定側）
SELECT CAST(p.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', p.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', p.transaction_date)),
  CASE WHEN p.amount >= 0 THEN 'credit' ELSE 'debit' END,
  ai.account_name, ABS(p.amount), NULL, p.description, 'paypay_bank'
FROM `main-project-477501.nocodb.paypay_bank_statements` p
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON p.`freee勘定科目_id` = ai.nocodb_id
WHERE p.amount IS NOT NULL AND p.`freee勘定科目_id` IS NOT NULL
  AND (p.`振替_id` IS NULL OR ai.nocodb_id IN (5, 6))

UNION ALL

-- ③ 楽天銀行（銀行側: 通常取引 + 保有口座への送金 + 未払金支払 + Amazon入金）
SELECT CAST(r.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', r.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', r.transaction_date)),
  CASE WHEN r.amount_jpy >= 0 THEN 'debit' ELSE 'credit' END,
  '楽天銀行', ABS(r.amount_jpy), NULL, r.counterparty_description, 'rakuten_bank'
FROM `main-project-477501.nocodb.rakuten_bank_statements` r
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON r.`freee勘定科目_id` = ai.nocodb_id
WHERE r.amount_jpy IS NOT NULL AND r.`freee勘定科目_id` IS NOT NULL
  AND (r.`振替_id` IS NULL OR ai.nocodb_id IN (3, 5, 6, 7, 8, 70))

UNION ALL

-- ③ 楽天銀行（相手勘定側）
SELECT CAST(r.nocodb_id AS STRING),
  SAFE.PARSE_DATE('%Y-%m-%d', r.transaction_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', r.transaction_date)),
  CASE WHEN r.amount_jpy >= 0 THEN 'credit' ELSE 'debit' END,
  ai.account_name,
  ABS(r.amount_jpy), NULL, r.counterparty_description, 'rakuten_bank'
FROM `main-project-477501.nocodb.rakuten_bank_statements` r
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON r.`freee勘定科目_id` = ai.nocodb_id
WHERE r.amount_jpy IS NOT NULL AND r.`freee勘定科目_id` IS NOT NULL
  AND (r.`振替_id` IS NULL OR ai.nocodb_id IN (3, 5, 6, 7, 8, 70))

UNION ALL

-- ④ NTT Finance（経費側: merchant_account_rules で勘定科目上書き）
-- NOTE: NTT の振替_id は月次支払バッチへのリンク（経費→支払の紐付け）であり、
-- 振替フラグではない。そのため is_transfer フィルタを維持する。
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

-- ④ NTT Finance（カード負債側）
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

-- ⑤ 代行会社（経費側）
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

-- ⑤ 代行会社（口座側）
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

-- ⑥ セールモンスター（売上高側）
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

-- ⑥ セールモンスター（セールモンスター口座側）
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

-- ⑦ 手動仕訳（借方側）- 事業主借も含む
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

-- ⑦ 手動仕訳（貸方側）- 事業主借も含む
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

-- ⑧ 棚卸仕訳
SELECT
  source_id, journal_date, fiscal_year,
  entry_side, account_name, amount_jpy, tax_code, description, source_table
FROM `main-project-477501.accounting.inventory_journal_view`
"""

def main():
    client = bigquery.Client(project=BQ_PROJECT)

    # First, get current P/L for comparison
    print('=== 更新前 P/L ===')
    q_before = """
    SELECT fiscal_year,
      SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) AS total_debit,
      SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS total_credit,
      COUNT(*) as cnt
    FROM `main-project-477501.accounting.journal_entries`
    GROUP BY fiscal_year
    ORDER BY fiscal_year
    """
    before = {}
    for row in client.query(q_before).result():
        before[row.fiscal_year] = (row.total_debit, row.total_credit, row.cnt)
        print(f'  FY{row.fiscal_year}: debit={row.total_debit:,} credit={row.total_credit:,} cnt={row.cnt}')

    # Update the VIEW
    print()
    print('=== VIEW 更新 ===')
    view_ref = f"{BQ_PROJECT}.accounting.journal_entries"

    # Use DDL to update the view
    ddl = f"CREATE OR REPLACE VIEW `{view_ref}` AS\n{NEW_VIEW_SQL}"

    job = client.query(ddl)
    job.result()
    print('  VIEW 更新完了')

    # Verify after update
    print()
    print('=== 更新後 P/L ===')
    after = {}
    for row in client.query(q_before).result():
        after[row.fiscal_year] = (row.total_debit, row.total_credit, row.cnt)
        print(f'  FY{row.fiscal_year}: debit={row.total_debit:,} credit={row.total_credit:,} cnt={row.cnt}')

    # Compare
    print()
    print('=== 差異分析 ===')
    all_years = sorted(set(list(before.keys()) + list(after.keys())))
    has_diff = False
    for fy in all_years:
        b = before.get(fy, (0, 0, 0))
        a = after.get(fy, (0, 0, 0))
        if b != a:
            has_diff = True
            print(f'  FY{fy}: debit {b[0]:,} → {a[0]:,} (diff={a[0]-b[0]:+,})')
            print(f'         credit {b[1]:,} → {a[1]:,} (diff={a[1]-b[1]:+,})')
            print(f'         count {b[2]} → {a[2]} (diff={a[2]-b[2]:+d})')
            print(f'         balance: {a[0]-a[1]:,}')

    if not has_diff:
        print('  差異なし ✓')

    # Balance check
    print()
    print('=== 全年度バランスチェック ===')
    all_balanced = True
    for fy in all_years:
        a = after.get(fy, (0, 0, 0))
        balance = a[0] - a[1]
        if balance != 0:
            all_balanced = False
            print(f'  FY{fy}: UNBALANCED! diff={balance:,}')
    if all_balanced:
        print('  全年度 バランス=0 ✓')

    # Source table comparison
    print()
    print('=== ソーステーブル別件数 ===')
    q_source = """
    SELECT source_table, COUNT(*) as cnt,
      SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit,
      SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit
    FROM `main-project-477501.accounting.journal_entries`
    GROUP BY source_table
    ORDER BY source_table
    """
    for row in client.query(q_source).result():
        print(f'  {row.source_table}: {row.cnt} entries, debit={row.debit:,}, credit={row.credit:,}')


if __name__ == '__main__':
    main()
