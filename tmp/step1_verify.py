"""Step 1 verification: Compare BQ amazon_account_statements with settlement_journal_payload_view"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check BQ table
q1 = """
SELECT COUNT(*) as cnt, SUM(amount) as total
FROM `main-project-477501.nocodb.amazon_account_statements`
"""
for row in client.query(q1).result():
    print(f'amazon_account_statements: {row.cnt} rows, total={row.total:,}')

# Check by entry_type
q2 = """
SELECT entry_type, COUNT(*) as cnt, SUM(amount) as total
FROM `main-project-477501.nocodb.amazon_account_statements`
GROUP BY entry_type
ORDER BY entry_type
"""
print('\nBy entry_type:')
for row in client.query(q2).result():
    print(f'  {row.entry_type}: {row.cnt} rows, total={row.total:,}')

# Compare with settlement_journal_payload_view
q3 = """
WITH bq_view AS (
  SELECT
    SUM(CASE WHEN d.entry_side = 'debit' THEN d.amount ELSE 0 END) as total_debit,
    SUM(CASE WHEN d.entry_side = 'credit' THEN d.amount ELSE 0 END) as total_credit,
    COUNT(*) as cnt
  FROM `main-project-477501.accounting.settlement_journal_payload_view` s
  CROSS JOIN UNNEST(s.json_details) AS d
),
nocodb_table AS (
  SELECT
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_positive,
    SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END) as total_negative,
    COUNT(*) as cnt
  FROM `main-project-477501.nocodb.amazon_account_statements`
)
SELECT
  bv.cnt as view_cnt, bv.total_debit as view_debit, bv.total_credit as view_credit,
  nt.cnt as table_cnt, nt.total_positive as table_positive, nt.total_negative as table_negative
FROM bq_view bv, nocodb_table nt
"""
print('\nComparison:')
for row in client.query(q3).result():
    print(f'  settlement_journal_view: {row.view_cnt} rows, debit={row.view_debit:,}, credit={row.view_credit:,}')
    print(f'  amazon_account_statements: {row.table_cnt} rows, positive={row.table_positive:,}, negative={row.table_negative:,}')
    print(f'  Counts match: {row.view_cnt == row.table_cnt}')
    print(f'  Credit (view) = Positive (table): {row.view_credit == row.table_positive}')
    print(f'  Debit (view) = Negative (table): {row.view_debit == row.table_negative}')

# Check column names
q4 = """
SELECT column_name, data_type
FROM `main-project-477501.nocodb.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'amazon_account_statements'
ORDER BY ordinal_position
"""
print('\nBQ columns:')
for row in client.query(q4).result():
    print(f'  {row.column_name}: {row.data_type}')

# P/L check (should be unchanged since new table isn't in VIEW yet)
q5 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) AS total_debit,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS total_credit,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) -
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS balance
FROM `main-project-477501.accounting.journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\nP/L check (should be unchanged):')
for row in client.query(q5).result():
    print(f'  FY{row.fiscal_year}: debit={row.total_debit:,} credit={row.total_credit:,} balance={row.balance:,}')
