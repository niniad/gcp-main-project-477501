import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# 1. FY2025 UNLINKED Amazon entries で settlement_id=None または調整エントリを確認
sql1 = """
SELECT
  a.nocodb_id, a.transaction_date, a.amount,
  a.description, a.settlement_id,
  ai.account_name as freee_account, a.`freee勘定科目_id`
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.`振替_id` IS NULL
  AND EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) = 2025
  AND (a.settlement_id IS NULL OR a.description LIKE '%調整%' OR a.description LIKE '%残高%')
ORDER BY a.transaction_date
"""
print("=== FY2025 UNLINKED 特殊エントリ（調整・sid=None）===")
for row in client.query(sql1).result():
    side = "Dr." if row.amount > 0 else "Cr."
    print(f"  id={row.nocodb_id} {row.transaction_date} {side}¥{abs(row.amount):,} [{row.freee_account}] {row.description}")

# 2. FY2025 settlement_id別のUNLINKED残高
sql2 = """
SELECT
  a.settlement_id,
  SUM(a.amount) as period_net,
  SUM(CASE WHEN a.`振替_id` IS NULL THEN a.amount ELSE 0 END) as unlinked_net,
  MAX(a.transaction_date) as last_date,
  COUNT(*) as entries
FROM `main-project-477501.nocodb.amazon_account_statements` a
WHERE EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) = 2025
GROUP BY 1
HAVING SUM(CASE WHEN a.`振替_id` IS NULL THEN a.amount ELSE 0 END) <> 0
ORDER BY last_date
"""
print()
print("=== FY2025 精算期間別（UNLINKED残あり） ===")
total_unlinked = 0
for row in client.query(sql2).result():
    total_unlinked += row.unlinked_net
    print(f"  sid={row.settlement_id} {row.last_date}: 期間Net¥{row.period_net:,} / unlinked¥{row.unlinked_net:,} ({row.entries}件)")
print(f"  合計UNLINKED: ¥{total_unlinked:,}")

# 3. FY2025 LINKED Amazon entries（銀行振替済み）
sql3 = """
SELECT
  a.nocodb_id, a.transaction_date, a.amount,
  a.description, a.settlement_id, a.`振替_id`,
  ai.account_name as freee_account
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.`振替_id` IS NOT NULL
  AND EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) = 2025
ORDER BY a.transaction_date
"""
print()
print("=== FY2025 LINKED Amazon entries（振替済み）===")
linked_total = 0
for row in client.query(sql3).result():
    linked_total += abs(row.amount)
    side = "Dr." if row.amount > 0 else "Cr."
    print(f"  id={row.nocodb_id} {row.transaction_date} {side}¥{abs(row.amount):,} 振替_id={row['振替_id']} [{row.freee_account}]")
print(f"  振替済み合計: ¥{linked_total:,}")

# 4. FY2023 amazon_account_statements 特殊エントリ確認
sql4 = """
SELECT
  a.nocodb_id, a.transaction_date, a.amount,
  a.description, a.settlement_id,
  ai.account_name as freee_account
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.`振替_id` IS NULL
  AND EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) = 2023
  AND (a.settlement_id IS NULL OR ai.account_name IN ('事業主借', 'Amazon出品アカウント'))
ORDER BY a.transaction_date
"""
print()
print("=== FY2023 UNLINKED 特殊エントリ（調整・事業主借・sid=None）===")
for row in client.query(sql4).result():
    side = "Dr." if row.amount > 0 else "Cr."
    print(f"  id={row.nocodb_id} {row.transaction_date} {side}¥{abs(row.amount):,} [{row.freee_account}] sid={row.settlement_id} {row.description}")
