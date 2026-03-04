"""Step 2 verification"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# P/L check
q = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) AS total_debit,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS total_credit,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) -
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS balance
FROM `main-project-477501.accounting.journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('=== P/L check after Step 2 ===')
for row in client.query(q).result():
    print(f'  FY{row.fiscal_year}: debit={row.total_debit:,} credit={row.total_credit:,} balance={row.balance:,}')

# Source counts
q2 = """
SELECT source_table, COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
GROUP BY source_table
ORDER BY source_table
"""
print('\n=== Source counts ===')
for row in client.query(q2).result():
    print(f'  {row.source_table}: {row.cnt}')

# Verify transfer records
q3 = """
SELECT COUNT(*) as cnt, SUM(amount) as total
FROM `main-project-477501.nocodb.transfer_records`
"""
print('\n=== Transfer records ===')
for row in client.query(q3).result():
    print(f'  Count: {row.cnt}, Total amount: {row.total:,}')
