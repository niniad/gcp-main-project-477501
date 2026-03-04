"""Compare old settlement VIEW vs new NocoDB Amazon data"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check old settlement_journal_view columns
q0 = """
SELECT column_name, data_type
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'settlement_journal_view'
ORDER BY ordinal_position
"""
print('=== settlement_journal_view columns ===')
for row in client.query(q0).result():
    print(f'  {row.column_name}: {row.data_type}')

# Check old settlement totals by year
q1 = """
SELECT EXTRACT(YEAR FROM journal_date) as yr,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit_total,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit_total,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.settlement_journal_view`
GROUP BY yr
ORDER BY yr
"""
print('\n=== Old settlement_journal_view by year ===')
for row in client.query(q1).result():
    print(f'  {row.yr}: debit={row.debit_total:>10,}  credit={row.credit_total:>10,}  cnt={row.cnt}')

# Check new journal_entries Amazon section by year
q2 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit_total,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit_total,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'amazon_settlement'
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n=== New journal_entries Amazon section by year ===')
for row in client.query(q2).result():
    print(f'  FY{row.fiscal_year}: debit={row.debit_total:>10,}  credit={row.credit_total:>10,}  cnt={row.cnt}')

# P/L from old settlement view
q3 = """
SELECT EXTRACT(YEAR FROM sj.journal_date) as yr,
  SUM(CASE
    WHEN ai.small_category IN ('売上高','雑収入') AND sj.entry_side = 'credit' THEN sj.amount_jpy
    WHEN ai.small_category IN ('売上高','雑収入') AND sj.entry_side = 'debit' THEN -sj.amount_jpy
    WHEN ai.big_category IN ('売上原価','経費') AND sj.entry_side = 'debit' THEN -sj.amount_jpy
    WHEN ai.big_category IN ('売上原価','経費') AND sj.entry_side = 'credit' THEN sj.amount_jpy
    ELSE 0
  END) as pl_contribution
FROM `main-project-477501.accounting.settlement_journal_view` sj
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON sj.account_name = ai.account_name
GROUP BY yr
ORDER BY yr
"""
print('\n=== Old settlement P/L contribution ===')
for row in client.query(q3).result():
    pl = row.pl_contribution or 0
    print(f'  {row.yr}: {pl:+,}')

# New Amazon P/L contribution
q4 = """
SELECT fiscal_year,
  SUM(pl_contribution) as pl
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE source_table = 'amazon_settlement'
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n=== New Amazon P/L from pl_journal_entries ===')
for row in client.query(q4).result():
    pl = row.pl or 0
    print(f'  FY{row.fiscal_year}: {pl:+,}')
