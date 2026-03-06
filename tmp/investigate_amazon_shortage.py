import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# 1. Amazon Settlement Net のUNLINKEDエントリの freee科目確認
sql1 = """
SELECT
  a.nocodb_id, a.transaction_date, a.amount, a.settlement_id,
  a.`freee勘定科目_id`, ai.account_name as freee_account
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.description LIKE '%Amazon Settlement Net%'
  AND a.`振替_id` IS NULL
ORDER BY a.transaction_date
"""
print("=== Amazon Settlement Net UNLINKED - freee科目 ===")
for row in client.query(sql1).result():
    print(f"  id={row.nocodb_id} {row.transaction_date} ¥{row.amount:,} freee科目={row['freee勘定科目_id']} ({row.freee_account})")

# 2. Amazon Settlement Net LINKED の freee科目（除外されているもの）
sql2 = """
SELECT
  a.nocodb_id, a.transaction_date, a.amount, a.settlement_id,
  a.`freee勘定科目_id`, ai.account_name as freee_account, a.`振替_id`
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.description LIKE '%Amazon Settlement Net%'
  AND a.`振替_id` IS NOT NULL
ORDER BY a.transaction_date
LIMIT 10
"""
print()
print("=== Amazon Settlement Net LINKED - freee科目（最初10件）===")
for row in client.query(sql2).result():
    print(f"  id={row.nocodb_id} {row.transaction_date} ¥{row.amount:,} freee科目={row['freee勘定科目_id']} ({row.freee_account}) 振替_id={row['振替_id']}")

# 3. NTT id=182 の is_transfer フラグ確認
sql3 = """
SELECT nocodb_id, usage_date, usage_amount, merchant_name, is_transfer, `振替_id`, `freee勘定科目_id`
FROM `main-project-477501.nocodb.ntt_finance_statements`
WHERE nocodb_id IN (182, 187, 164, 16, 36, 50, 60, 80, 90, 102)
ORDER BY nocodb_id
"""
print()
print("=== NTT freee科目=9 の is_transfer フラグ ===")
for row in client.query(sql3).result():
    print(f"  id={row.nocodb_id} {row.usage_date} ¥{row.usage_amount:,} is_transfer={row.is_transfer} 振替_id={row['振替_id']}")

# 4. Amazon出品アカウント 残高の全FY別（2023/2024/2025別）
sql4 = """
SELECT
  fiscal_year,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS net_change,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE 0 END) AS dr_total,
  SUM(CASE WHEN entry_side='credit' THEN amount_jpy ELSE 0 END) AS cr_total,
  source_table,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = 'Amazon出品アカウント'
GROUP BY 1,5
ORDER BY 1,5
"""
print()
print("=== Amazon出品アカウント FY・source別 ===")
for row in client.query(sql4).result():
    print(f"  FY{row.fiscal_year} {row.source_table}: {row.cnt}件 Dr.¥{row.dr_total:,} Cr.¥{row.cr_total:,} Net¥{row.net_change:,}")
