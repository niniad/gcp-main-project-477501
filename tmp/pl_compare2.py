"""Compare old settlement_journal_payload_view vs new Amazon data"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check settlement_journal_payload_view columns
q0 = """
SELECT column_name, data_type
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'settlement_journal_payload_view'
ORDER BY ordinal_position
"""
print('=== settlement_journal_payload_view columns ===')
for row in client.query(q0).result():
    print(f'  {row.column_name}: {row.data_type}')

# Old settlement payload view totals
q1 = """
SELECT EXTRACT(YEAR FROM journal_date) as yr,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit_total,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit_total,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.settlement_journal_payload_view`
GROUP BY yr
ORDER BY yr
"""
print('\n=== Old settlement_journal_payload_view totals by year ===')
try:
    for row in client.query(q1).result():
        print(f'  {row.yr}: debit={row.debit_total:>10,}  credit={row.credit_total:>10,}  cnt={row.cnt}')
except Exception as e:
    print(f'  Error: {e}')

# P/L from old view
q2 = """
SELECT EXTRACT(YEAR FROM sj.journal_date) as yr,
  SUM(CASE
    WHEN ai.small_category IN ('売上高','雑収入') AND sj.entry_side = 'credit' THEN sj.amount_jpy
    WHEN ai.small_category IN ('売上高','雑収入') AND sj.entry_side = 'debit' THEN -sj.amount_jpy
    WHEN ai.big_category IN ('売上原価','経費') AND sj.entry_side = 'debit' THEN -sj.amount_jpy
    WHEN ai.big_category IN ('売上原価','経費') AND sj.entry_side = 'credit' THEN sj.amount_jpy
    ELSE 0
  END) as pl_contribution
FROM `main-project-477501.accounting.settlement_journal_payload_view` sj
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON sj.account_name = ai.account_name
GROUP BY yr
ORDER BY yr
"""
print('\n=== Old settlement P/L (calculated from payload view) ===')
try:
    for row in client.query(q2).result():
        pl = row.pl_contribution or 0
        print(f'  {row.yr}: {pl:+,}')
except Exception as e:
    print(f'  Error: {e}')

# New Amazon P/L
q3 = """
SELECT fiscal_year,
  SUM(pl_contribution) as pl
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE source_table = 'amazon_settlement'
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n=== New Amazon P/L (from pl_journal_entries) ===')
for row in client.query(q3).result():
    pl = row.pl or 0
    print(f'  FY{row.fiscal_year}: {pl:+,}')

# Compare entry counts
q4 = """
SELECT EXTRACT(YEAR FROM journal_date) as yr, account_name,
  SUM(amount_jpy) as total, COUNT(*) as cnt
FROM `main-project-477501.accounting.settlement_journal_payload_view`
WHERE EXTRACT(YEAR FROM journal_date) = 2024
GROUP BY yr, account_name
ORDER BY account_name
"""
print('\n=== Old FY2024 settlement by account ===')
try:
    for row in client.query(q4).result():
        print(f'  {row.account_name:25} total={row.total:>12,}  cnt={row.cnt}')
except Exception as e:
    print(f'  Error: {e}')

# New
q5 = """
SELECT account_name, entry_side,
  SUM(amount_jpy) as total, COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2024 AND source_table = 'amazon_settlement'
GROUP BY account_name, entry_side
ORDER BY account_name, entry_side
"""
print('\n=== New FY2024 Amazon journal_entries by account ===')
for row in client.query(q5).result():
    print(f'  {row.account_name:25} {row.entry_side:6} total={row.total:>12,}  cnt={row.cnt}')
