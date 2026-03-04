"""
accounting.amazon_account_balance_view を作成/更新
Amazon出品アカウントの正しい残高推移を追えるVIEW

設計:
- REVENUE / EXPENSE / ADJUSTMENT: 常に残高計算に含める
- DEPOSIT: 振替_id IS NOT NULL (銀行リンク済み) のみ含める
  ← 負値精算のDEPOSITは振替なし→除外（二重計上防止）
- 全レコードはVIEWに表示、balance_contribution列で寄与度を示す
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

VIEW_ID = 'main-project-477501.accounting.amazon_account_balance_view'

view_sql = r"""
/*
  Amazon出品アカウント 残高推移 VIEW
  =============================================
  balance_contribution:
    REVENUE / EXPENSE / ADJUSTMENT     → amount (常に計上)
    DEPOSIT (振替_id IS NOT NULL)      → amount (銀行送金済み、計上)
    DEPOSIT (振替_id IS NULL)          → 0 (負値精算または銀行データ欠損、計上せず)

  running_balance:
    累積合計。正=Amazonが我々に支払い義務あり、負=我々がAmazonに支払い義務あり
*/
SELECT
  a.nocodb_id,
  SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date) AS transaction_date,
  a.entry_type,
  a.amount,
  a.description,
  ai.account_name             AS counterpart_account,
  a.`振替_id`                 AS transfer_id,
  -- 残高への寄与
  CASE
    WHEN a.entry_type IN ('REVENUE', 'EXPENSE', 'ADJUSTMENT') THEN a.amount
    WHEN a.entry_type = 'DEPOSIT' AND a.`振替_id` IS NOT NULL  THEN a.amount
    ELSE 0
  END                         AS balance_contribution,
  -- 除外理由（DEPOSITが除外された場合）
  CASE
    WHEN a.entry_type = 'DEPOSIT' AND a.`振替_id` IS NULL AND a.amount < 0
         THEN CASE
                WHEN s.total_amount < 0 THEN 'Amazon回収（負値精算）'
                WHEN s.total_amount > 0 THEN '銀行データ未取込'
                ELSE '精算額ゼロ'
              END
    ELSE NULL
  END                         AS exclusion_reason,
  s.total_amount              AS settlement_total,
  -- 累積残高（正しい計算）
  SUM(
    CASE
      WHEN a.entry_type IN ('REVENUE', 'EXPENSE', 'ADJUSTMENT') THEN a.amount
      WHEN a.entry_type = 'DEPOSIT' AND a.`振替_id` IS NOT NULL  THEN a.amount
      ELSE 0
    END
  ) OVER (
    ORDER BY SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date),
             a.nocodb_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                           AS running_balance
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai
  ON a.`freee勘定科目_id` = ai.nocodb_id
LEFT JOIN (
  -- settlement_summary: 各精算の振込方向（正=銀行送金、負=Amazon回収）
  SELECT DISTINCT
    CAST(settlement_id AS STRING) AS settlement_id_str,
    total_amount
  FROM `main-project-477501.sp_api_external.sp_api_settlement_summary_view`
) s ON REGEXP_EXTRACT(a.description, r'settlement (\d+):') = s.settlement_id_str
ORDER BY SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date), a.nocodb_id
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

# 動作確認
print('\n=== VIEW 確認（月次残高サマリー）===')
q_check = """
SELECT
  FORMAT_DATE('%Y-%m', transaction_date) AS ym,
  COUNT(*) AS cnt,
  SUM(balance_contribution) AS monthly_contrib,
  MAX(running_balance) AS month_end_balance,
  COUNTIF(exclusion_reason IS NOT NULL) AS excluded_deposits
FROM `main-project-477501.accounting.amazon_account_balance_view`
GROUP BY 1
ORDER BY 1
"""
print(f'{"年月":7} {"件":>4} {"月次貢献":>10} {"月末残高":>12} {"除外DEPOSIT":>12}')
for r in client.query(q_check).result():
    mark = ' <-- 除外あり' if r.excluded_deposits > 0 else ''
    print(f'{r.ym}  {r.cnt:>3}  {r.monthly_contrib:>10,.0f}  {r.month_end_balance:>12,.0f}  {r.excluded_deposits:>10}{mark}')

# 除外DEPOSITの一覧
print('\n=== 除外されたDEPOSITエントリ（全件）===')
q_excluded = """
SELECT
  nocodb_id,
  transaction_date,
  amount,
  exclusion_reason,
  settlement_total,
  description
FROM `main-project-477501.accounting.amazon_account_balance_view`
WHERE exclusion_reason IS NOT NULL
ORDER BY transaction_date
"""
for r in client.query(q_excluded).result():
    print(f'  id={r.nocodb_id} {r.transaction_date} {r.amount:>9,}  [{r.exclusion_reason}]  settlement_net={r.settlement_total}')
