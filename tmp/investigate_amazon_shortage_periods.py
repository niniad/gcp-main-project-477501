import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# 1. FY2025 Jul 30 と Nov 5 の shortage精算期間の詳細
sql1 = """
SELECT
  a.nocodb_id, a.transaction_date, a.amount,
  a.description, a.settlement_id,
  ai.account_name as freee_account, a.`freee勘定科目_id`, a.`振替_id`
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON a.`freee勘定科目_id` = ai.nocodb_id
WHERE a.settlement_id IN ('12080603173', '12163491473')
ORDER BY a.settlement_id, a.nocodb_id
"""
print("=== FY2025 Shortage精算期間（Jul 30, Nov 5）の全エントリ ===")
for row in client.query(sql1).result():
    side = "Dr." if row.amount > 0 else "Cr."
    linked = f" 振替_id={row['振替_id']}" if row['振替_id'] else " [UNLINKED]"
    print(f"  id={row.nocodb_id} {row.transaction_date} {side}¥{abs(row.amount):,} [{row.freee_account}]{linked}")

# 2. FY2025 NTT Finance の freee科目=9（Amazon）エントリ
sql2 = """
SELECT
  n.nocodb_id, n.usage_date, n.usage_amount, n.merchant_name,
  n.is_transfer, n.`振替_id`, n.`freee勘定科目_id`,
  ai.account_name
FROM `main-project-477501.nocodb.ntt_finance_statements` n
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON n.`freee勘定科目_id` = ai.nocodb_id
WHERE n.`freee勘定科目_id` = 9
  AND n.usage_date BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY n.usage_date
"""
print()
print("=== FY2025 NTT freee科目=Amazon ===")
ntt_total = 0
for row in client.query(sql2).result():
    ntt_total += abs(row.usage_amount)
    print(f"  id={row.nocodb_id} {row.usage_date} ¥{row.usage_amount:,} [{row.account_name}] is_transfer={row.is_transfer} 振替_id={row['振替_id']} | {row.merchant_name}")
print(f"  合計: ¥{ntt_total:,}")

# 3. FY2025 PayPay銀行 freee科目=9（Amazon）エントリ
sql3 = """
SELECT
  p.nocodb_id, p.transaction_date, p.amount, p.description,
  p.`freee勘定科目_id`, ai.account_name, p.`振替_id`
FROM `main-project-477501.nocodb.paypay_bank_statements` p
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON p.`freee勘定科目_id` = ai.nocodb_id
WHERE p.`freee勘定科目_id` = 9
  AND p.transaction_date BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY p.transaction_date
"""
print()
print("=== FY2025 PayPay freee科目=Amazon ===")
paypay_total = 0
for row in client.query(sql3).result():
    paypay_total += abs(row.amount)
    side = "Dr." if row.amount > 0 else "Cr."
    print(f"  id={row.nocodb_id} {row.transaction_date} {side}¥{abs(row.amount):,} [{row.account_name}] 振替_id={row['振替_id']} | {row.description}")
print(f"  合計: ¥{paypay_total:,}")

# 4. FY2024 Amazon balance の構成再確認（id=695, id=696 除外した場合）
sql4 = """
SELECT
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS total_balance,
  SUM(CASE WHEN fiscal_year=2024 THEN (CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) ELSE 0 END) AS fy2024,
  SUM(CASE WHEN fiscal_year=2025 THEN (CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) ELSE 0 END) AS fy2025,
  SUM(CASE WHEN fiscal_year=2026 THEN (CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) ELSE 0 END) AS fy2026
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = 'Amazon出品アカウント'
"""
print()
print("=== Amazon出品アカウント 現在残高（全年度）===")
for row in client.query(sql4).result():
    print(f"  FY2024: ¥{row.fy2024:,}  FY2025: ¥{row.fy2025:,}  FY2026: ¥{row.fy2026:,}  合計: ¥{row.total_balance:,}")

# 5. id=695, 696 の詳細
sql5 = """
SELECT nocodb_id, transaction_date, amount, description, settlement_id,
  `freee勘定科目_id`, `振替_id`
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE nocodb_id IN (695, 696)
"""
print()
print("=== Amazon 調整エントリ id=695, 696 ===")
for row in client.query(sql5).result():
    side = "Dr." if row.amount > 0 else "Cr."
    print(f"  id={row.nocodb_id} {row.transaction_date} {side}¥{abs(row.amount):,} freee科目={row['freee勘定科目_id']} 振替_id={row['振替_id']}")
    print(f"    説明: {row.description}")
