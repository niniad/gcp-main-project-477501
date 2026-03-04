"""Analyze P/L difference: old vs new architecture"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check current P/L by fiscal year
q1 = """
SELECT fiscal_year, SUM(pl_contribution) AS net_income, COUNT(*) as cnt
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('=== Current P/L by fiscal year ===')
for row in client.query(q1).result():
    ni = row.net_income or 0
    print(f'  FY{row.fiscal_year}: {ni:+,} ({row.cnt} rows)')

# Check Amazon P/L detail - compare old expected vs new
# Old architecture had settlement_journal_payload_view with different grouping
# New architecture uses NocoDB amazon_account_statements

# Check if Amazon totals match expected
q2 = """
SELECT fiscal_year,
  SUM(CASE WHEN source_table = 'amazon_settlement' THEN pl_contribution ELSE 0 END) as amazon_pl,
  SUM(CASE WHEN source_table != 'amazon_settlement' THEN pl_contribution ELSE 0 END) as other_pl,
  SUM(pl_contribution) as total_pl
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n=== Amazon vs Other P/L ===')
for row in client.query(q2).result():
    amazon = row.amazon_pl or 0
    other = row.other_pl or 0
    total = row.total_pl or 0
    print(f'  FY{row.fiscal_year}: Amazon={amazon:+,}  Other={other:+,}  Total={total:+,}')

# Check MF reconciliation values
print('\n=== Expected P/L from MF reconciliation ===')
print('  FY2023: -1,340,610 (MF confirmed)')
print('  FY2024: -296,872 (MF confirmed)')

# Check FY2023 non-Amazon P/L
q3 = """
SELECT SUM(pl_contribution) as non_amazon_pl
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2023 AND source_table != 'amazon_settlement'
"""
for row in client.query(q3).result():
    print(f'\n  FY2023 non-Amazon P/L: {row.non_amazon_pl:+,}')

# Check FY2023 Amazon net (sales - expenses)
q4 = """
SELECT
  SUM(CASE WHEN entry_side = 'credit' AND account_name NOT IN ('Amazon出品アカウント', '仮払金') THEN amount_jpy ELSE 0 END) as revenue,
  SUM(CASE WHEN entry_side = 'debit' AND account_name NOT IN ('Amazon出品アカウント', '仮払金') THEN amount_jpy ELSE 0 END) as expenses,
  SUM(pl_contribution) as pl_total
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2023 AND source_table = 'amazon_settlement'
"""
print('\n=== FY2023 Amazon P/L breakdown ===')
for row in client.query(q4).result():
    rev = row.revenue or 0
    exp = row.expenses or 0
    pl = row.pl_total or 0
    print(f'  Revenue: {rev:+,}')
    print(f'  Expenses: {exp:+,}')
    print(f'  Net (rev-exp): {rev - exp:+,}')
    print(f'  pl_contribution sum: {pl:+,}')

# Compare with old settlement view if it still exists
q5 = """
SELECT
  SUM(CASE WHEN entry_side = 'credit' AND account_name NOT LIKE '%Amazon%' AND account_name != '仮払金' THEN amount_jpy ELSE 0 END) as revenue,
  SUM(CASE WHEN entry_side = 'debit' AND account_name NOT LIKE '%Amazon%' AND account_name != '仮払金' THEN amount_jpy ELSE 0 END) as expenses
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2023 AND source_table = 'amazon_settlement'
"""
print('\n=== FY2023 Amazon from journal_entries (raw) ===')
for row in client.query(q5).result():
    rev = row.revenue or 0
    exp = row.expenses or 0
    print(f'  Revenue: {rev:+,}')
    print(f'  Expenses: {exp:+,}')
    print(f'  Net: {rev - exp:+,}')
