import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# PayPay の freee勘定科目別（振替_idあり/なし）
sql2 = """
SELECT ai.account_name, p.`振替_id` IS NOT NULL as has_transfer,
  COUNT(*) as cnt,
  SUM(ABS(p.amount)) as total
FROM `main-project-477501.nocodb.paypay_bank_statements` p
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON p.`freee勘定科目_id` = ai.nocodb_id
WHERE p.`freee勘定科目_id` IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2
"""
print("=== PayPay 勘定科目別（振替フラグ別）===")
for row in client.query(sql2).result():
    print(f"  {row.account_name} / transfer={row.has_transfer}: {row.cnt}件 ¥{row.total:,}")

# 事業主借テーブルの借方科目（PayPay銀行があるか）
sql3 = """
SELECT ai.account_name as debit_account, COUNT(*) as cnt, SUM(oc.amount) as total
FROM `main-project-477501.nocodb.owner_contribution_entries` oc
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON oc.debit_account_id = ai.nocodb_id
GROUP BY 1 ORDER BY total DESC
"""
print()
print("=== 事業主借テーブル 借方科目別 ===")
for row in client.query(sql3).result():
    print(f"  Dr.{row.debit_account}: {row.cnt}件 ¥{row.total:,}")

# Amazon出品アカウント FY2025末残高
sql4 = """
SELECT
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = 'Amazon出品アカウント'
AND journal_date <= '2025-12-31'
"""
print()
print("=== Amazon出品アカウント FY2025末残高 ===")
for row in client.query(sql4).result():
    print(f"  残高: ¥{row.balance:,}")

# Amazon 月次推移（FY2025）
sql5 = """
SELECT
  FORMAT_DATE('%Y-%m', journal_date) as month,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS monthly_net
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = 'Amazon出品アカウント'
AND fiscal_year = 2025
GROUP BY 1 ORDER BY 1
"""
print()
print("=== Amazon出品アカウント FY2025 月次 ===")
running = 0
for row in client.query(sql5).result():
    running += row.monthly_net
    print(f"  {row.month}: 純増減¥{row.monthly_net:,} / 累計¥{running:,}")
