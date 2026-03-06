import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# Amazon DEPOSIT エントリの状況（振替_id あり/なし）
sql1 = """
SELECT
  a.`振替_id` IS NOT NULL as is_linked,
  ai.account_name as counterpart,
  COUNT(*) as cnt,
  SUM(ABS(a.amount)) as total
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.amount < 0  -- DEPOSIT/費用（Amazonから支出）
  AND a.description LIKE '%TRANSFER%'  -- DEPOSIT行
GROUP BY 1,2 ORDER BY 1,2
"""
print("=== Amazon DEPOSIT行（TRANSFER）===")
for row in client.query(sql1).result():
    print(f"  linked={row.is_linked} / {row.counterpart}: {row.cnt}件 ¥{row.total:,}")

# Amazon 全エントリの種類
sql2 = """
SELECT
  a.description,
  a.`振替_id` IS NOT NULL as is_linked,
  ai.account_name as counterpart,
  COUNT(*) as cnt,
  SUM(a.amount) as total_amount
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
GROUP BY 1,2,3
ORDER BY total_amount
"""
print()
print("=== Amazon全エントリ 種類別 ===")
for row in client.query(sql2).result():
    print(f"  '{row.description}' linked={row.is_linked} {row.counterpart}: {row.cnt}件 合計¥{row.total_amount:,}")

# 楽天銀行 2025-03-03 付近の取引確認
sql3 = """
SELECT r.nocodb_id, r.transaction_date, r.amount_jpy, ai.account_name, r.counterparty_description
FROM `main-project-477501.nocodb.rakuten_bank_statements` r
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON r.`freee勘定科目_id` = ai.nocodb_id
WHERE r.transaction_date BETWEEN '2025-02-25' AND '2025-03-15'
ORDER BY r.transaction_date
"""
print()
print("=== 楽天銀行 2025-02-25〜03-15 ===")
for row in client.query(sql3).result():
    print(f"  {row.transaction_date} ¥{row.amount_jpy:,} {row.account_name} | {row.counterparty_description}")
