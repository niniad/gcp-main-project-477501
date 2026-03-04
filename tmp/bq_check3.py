import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# Get full journal_entries view definition and find NTT section
table = client.get_table('main-project-477501.accounting.journal_entries')
q = table.view_query
lines = q.split('\n')

# Find NTT section
in_ntt = False
for i, line in enumerate(lines):
    if 'ntt' in line.lower() or 'NTT' in line:
        in_ntt = True
    if in_ntt:
        print(f"{i}: {line}")
    if in_ntt and i > 0 and ('UNION ALL' in line and not 'ntt' in lines[max(0,i-3):i+1][0].lower()):
        # Check if we've gone past the NTT section
        pass
    if in_ntt and line.strip().startswith('UNION ALL') and i > 10:
        # Check next lines to see if still NTT
        if i+2 < len(lines) and 'ntt' not in lines[i+1].lower() and 'ntt' not in lines[i+2].lower():
            break

# Also check id=202 vs id=159 (the existing PP*9491CODE)
print()
print("=== NTT id=159 vs id=202 in journal_entries ===")
query = """
SELECT source_id, entry_side, account_name, amount_jpy
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table LIKE '%ntt%' AND source_id IN ('159', '202')
ORDER BY source_id, entry_side
"""
for row in client.query(query).result():
    print(f"  id={row.source_id} | {row.entry_side} | {row.account_name} | {row.amount_jpy:,}")

# Check raw NTT data for both
print()
print("=== NTT raw data (NocoDB BQ table) ===")
query2 = """
SELECT nocodb_id, usage_date, merchant_name, usage_amount, payment_amount,
       freee_account_item_id, is_transfer
FROM `main-project-477501.nocodb.ntt_finance_statements`
WHERE nocodb_id IN (159, 202)
ORDER BY nocodb_id
"""
for row in client.query(query2).result():
    print(f"  id={row.nocodb_id} | {row.usage_date} | {row.merchant_name} | usage={row.usage_amount} | pay={row.payment_amount} | acct={row.freee_account_item_id} | transfer={row.is_transfer}")
