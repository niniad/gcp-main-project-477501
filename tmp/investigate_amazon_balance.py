import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# 1. FY2024 amazon_account_statements の UNLINKED エントリ詳細
sql1 = """
SELECT
  a.nocodb_id, a.transaction_date, a.amount,
  a.description, a.settlement_id,
  ai.account_name as freee_account, a.`freee勘定科目_id`
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.`振替_id` IS NULL
  AND EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) = 2024
ORDER BY a.transaction_date
"""
print("=== FY2024 UNLINKED Amazon entries ===")
total_dr = 0
total_cr = 0
for row in client.query(sql1).result():
    side = "Dr." if row.amount > 0 else "Cr."
    if row.amount > 0:
        total_dr += row.amount
    else:
        total_cr += abs(row.amount)
    print(f"  id={row.nocodb_id} {row.transaction_date} {side}¥{abs(row.amount):,} [{row.freee_account}] {row.description} sid={row.settlement_id}")
print(f"  Dr合計=¥{total_dr:,} / Cr合計=¥{total_cr:,} / Net=¥{total_dr-total_cr:,}")

# 2. FY2024 LINKED エントリ（振替_id IS NOT NULL）の確認
sql2 = """
SELECT
  a.nocodb_id, a.transaction_date, a.amount,
  a.description, a.settlement_id, a.`振替_id`,
  ai.account_name as freee_account
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.`振替_id` IS NOT NULL
  AND EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) = 2024
ORDER BY a.transaction_date
"""
print()
print("=== FY2024 LINKED Amazon entries (振替済) ===")
for row in client.query(sql2).result():
    side = "Dr." if row.amount > 0 else "Cr."
    print(f"  id={row.nocodb_id} {row.transaction_date} {side}¥{abs(row.amount):,} 振替_id={row['振替_id']} [{row.freee_account}] {row.description}")

# 3. FY2024 楽天銀行 Amazon関連エントリ（freee科目=9）
sql3 = """
SELECT
  r.nocodb_id, r.transaction_date, r.amount_jpy,
  r.`振替_id`, r.counterparty_description
FROM `main-project-477501.nocodb.rakuten_bank_statements` r
WHERE r.`freee勘定科目_id` = 9
  AND r.transaction_date BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY r.transaction_date
"""
print()
print("=== FY2024 楽天銀行 freee科目=Amazon (id=9) ===")
rakuten_total = 0
for row in client.query(sql3).result():
    rakuten_total += abs(row.amount_jpy)
    print(f"  id={row.nocodb_id} {row.transaction_date} ¥{row.amount_jpy:,} 振替_id={row['振替_id']} | {row.counterparty_description}")
print(f"  合計: ¥{rakuten_total:,}")

# 4. FY2024 settlement_id 別の LINKED/UNLINKED 状況
sql4 = """
SELECT
  a.settlement_id,
  SUM(a.amount) as period_net,
  SUM(CASE WHEN a.`振替_id` IS NULL THEN a.amount ELSE 0 END) as unlinked_net,
  SUM(CASE WHEN a.`振替_id` IS NOT NULL THEN a.amount ELSE 0 END) as linked_net,
  MAX(a.transaction_date) as last_date,
  COUNT(*) as entries
FROM `main-project-477501.nocodb.amazon_account_statements` a
WHERE EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', a.transaction_date)) = 2024
GROUP BY 1
HAVING SUM(CASE WHEN a.`振替_id` IS NULL THEN a.amount ELSE 0 END) <> 0
ORDER BY last_date
"""
print()
print("=== FY2024 精算期間別（UNLINKED残がある） ===")
for row in client.query(sql4).result():
    print(f"  sid={row.settlement_id} {row.last_date}: 期間Net¥{row.period_net:,} / unlinked¥{row.unlinked_net:,} / linked¥{row.linked_net:,} ({row.entries}件)")
