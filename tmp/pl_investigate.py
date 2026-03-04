"""Investigate P/L discrepancy"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check for duplicate account_names in account_items
q1 = """
SELECT account_name, COUNT(*) as cnt
FROM `main-project-477501.nocodb.account_items`
GROUP BY account_name
HAVING COUNT(*) > 1
ORDER BY cnt DESC
"""
print('=== Duplicate account_names in account_items ===')
dups = list(client.query(q1).result())
for row in dups:
    print(f'  {row.account_name}: {row.cnt} entries')
if not dups:
    print('  None ✓')

# Check pl_journal_entries row count vs journal_entries row count
q2 = """
SELECT
  (SELECT COUNT(*) FROM `main-project-477501.accounting.journal_entries`) as je_count,
  (SELECT COUNT(*) FROM `main-project-477501.accounting.pl_journal_entries`) as pj_count
"""
for row in client.query(q2).result():
    print(f'\n=== Row count comparison ===')
    print(f'  journal_entries: {row.je_count}')
    print(f'  pl_journal_entries: {row.pj_count}')
    print(f'  Ratio: {row.pj_count / row.je_count:.2f}')

# FY2023 P/L breakdown by source_table
q3 = """
SELECT source_table,
  SUM(pl_contribution) as pl_total,
  COUNT(*) as cnt,
  COUNT(pl_contribution) as pl_non_null
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2023
GROUP BY source_table
ORDER BY source_table
"""
print('\n=== FY2023 P/L by source_table ===')
total_pl = 0
for row in client.query(q3).result():
    pl = row.pl_total or 0
    total_pl += pl
    print(f'  {row.source_table:25} pl={pl:>+10,} ({row.pl_non_null}/{row.cnt} rows)')
print(f'  {"TOTAL":<25} pl={total_pl:>+10,}')

# Check specifically amazon_settlement P/L detail
q4 = """
SELECT account_name, entry_side,
  SUM(amount_jpy) as total_amount,
  SUM(pl_contribution) as total_pl,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2023 AND source_table = 'amazon_settlement'
GROUP BY account_name, entry_side
ORDER BY account_name, entry_side
"""
print('\n=== FY2023 amazon_settlement P/L detail ===')
for row in client.query(q4).result():
    pl = row.total_pl or 0
    print(f'  {row.account_name:25} {row.entry_side:6} amount={row.total_amount:>10,} pl={pl:>+10,} ({row.cnt})')
