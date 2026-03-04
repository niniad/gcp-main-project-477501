import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# Check NTT id=202 in journal_entries
query = """
SELECT source_table, source_id, entry_side, account_name, amount_jpy, description
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table LIKE '%ntt%' AND source_id = '202'
ORDER BY entry_side
"""
print("=== NTT id=202 in journal_entries ===")
for row in client.query(query).result():
    print(f"  {row.source_table} | {row.source_id} | {row.entry_side} | {row.account_name} | {row.amount_jpy:,} | {row.description}")

# Compare PL by account: current vs what it should be
# Get each PL account's contribution
query2 = """
SELECT account_name,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) AS current_amount
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2023
AND account_name NOT IN (
  'Amazon出品アカウント','ESPRIME','THE直行便','YP','PayPay銀行',
  '事業主借','事業主貸','売掛金','未払金','楽天銀行','商品','開業費','仮払金'
)
GROUP BY account_name
ORDER BY account_name
"""
print()
print("=== FY2023 PL accounts detail ===")
total = 0
for row in client.query(query2).result():
    total += row.current_amount
    print(f"  {row.account_name:20s} {row.current_amount:>10,}")
print(f"  {'TOTAL':20s} {total:>10,}")

# Check if NTT id=202 appears at all
query3 = """
SELECT COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE source_id = '202' AND source_table LIKE '%ntt%'
"""
print()
for row in client.query(query3).result():
    print(f"NTT id=202 entries in journal: {row.cnt}")
