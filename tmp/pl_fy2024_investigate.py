"""Investigate FY2024 P/L discrepancy: -1,088,882 vs expected -296,872"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# FY2024 P/L by source_table
q1 = """
SELECT source_table,
  SUM(pl_contribution) as pl_total,
  COUNT(*) as cnt,
  COUNT(pl_contribution) as pl_non_null
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2024
GROUP BY source_table
ORDER BY source_table
"""
print('=== FY2024 P/L by source_table ===')
total_pl = 0
for row in client.query(q1).result():
    pl = row.pl_total or 0
    total_pl += pl
    print(f'  {row.source_table:25} pl={pl:>+12,} ({row.pl_non_null}/{row.cnt} rows)')
print(f'  {"TOTAL":<25} pl={total_pl:>+12,}')

# Check if there are entries that should be transfers but aren't filtered
# Rakuten bank entries with 振替_id that are still in journal_entries
q2 = """
SELECT source_table, COUNT(*) as cnt,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit_total,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit_total
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2024
GROUP BY source_table
ORDER BY source_table
"""
print('\n=== FY2024 journal_entries by source_table ===')
for row in client.query(q2).result():
    print(f'  {row.source_table:25} cnt={row.cnt:>4}  debit={row.debit_total:>10,}  credit={row.credit_total:>10,}')

# Check settlement_journal_payload_view if it still exists
q3 = """
SELECT table_name
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.VIEWS`
WHERE table_name LIKE '%settlement%'
"""
print('\n=== Settlement-related views ===')
for row in client.query(q3).result():
    print(f'  {row.table_name}')

# Check old Amazon P/L from the settlement views
q4 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side = 'credit' AND account_name NOT IN ('Amazon出品アカウント', '売掛金', '仮払金') THEN amount_jpy ELSE 0 END) as revenue,
  SUM(CASE WHEN entry_side = 'debit' AND account_name NOT IN ('Amazon出品アカウント', '売掛金', '仮払金') THEN amount_jpy ELSE 0 END) as expenses
FROM `main-project-477501.accounting.settlement_journal_view`
WHERE fiscal_year = 2024
GROUP BY fiscal_year
"""
print('\n=== FY2024 old settlement_journal_view P/L ===')
try:
    for row in client.query(q4).result():
        rev = row.revenue or 0
        exp = row.expenses or 0
        print(f'  Revenue: {rev:+,}')
        print(f'  Expenses: {exp:+,}')
        print(f'  Net: {rev - exp:+,}')
except Exception as e:
    print(f'  Error: {e}')

# Check what Amazon entries are in new NocoDB table for FY2024
q5 = """
SELECT entry_type, COUNT(*) as cnt, SUM(amount) as total
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', transaction_date)) = 2024
GROUP BY entry_type
ORDER BY entry_type
"""
print('\n=== FY2024 NocoDB Amazon entries by type ===')
for row in client.query(q5).result():
    total = row.total or 0
    print(f'  {row.entry_type:25} cnt={row.cnt:>4}  total={total:>+12,}')

# Check how many are filtered by 振替_id
q6 = """
SELECT
  COUNT(*) as total,
  COUNTIF(`振替_id` IS NULL) as non_transfer,
  COUNTIF(`振替_id` IS NOT NULL) as transfer
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', transaction_date)) = 2024
"""
print('\n=== FY2024 Amazon transfer filter ===')
for row in client.query(q6).result():
    print(f'  Total: {row.total}, Non-transfer: {row.non_transfer}, Transfer: {row.transfer}')

# OLD settlement approach - what did the old VIEW produce?
q7 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit_total,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit_total,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.settlement_journal_view`
WHERE fiscal_year IN (2023, 2024)
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n=== Old settlement_journal_view totals ===')
try:
    for row in client.query(q7).result():
        print(f'  FY{row.fiscal_year}: debit={row.debit_total:>10,}  credit={row.credit_total:>10,}  cnt={row.cnt}')
except Exception as e:
    print(f'  Error: {e}')
