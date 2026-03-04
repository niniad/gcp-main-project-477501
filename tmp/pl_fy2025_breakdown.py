"""Check FY2025 P/L breakdown and FY2023 deviation"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# FY2025 P/L by source_table
q1 = """
SELECT source_table,
  SUM(pl_contribution) as pl_total,
  COUNT(pl_contribution) as pl_rows,
  COUNT(*) as total_rows
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2025
GROUP BY source_table
ORDER BY source_table
"""
print('=== FY2025 P/L by source_table ===')
total = 0
for row in client.query(q1).result():
    pl = row.pl_total or 0
    total += pl
    print(f'  {row.source_table:25} pl={pl:>+12,} ({row.pl_rows}/{row.total_rows})')
print(f'  {"TOTAL":<25} pl={total:>+12,}')

# FY2025 account-level detail for key categories
q2 = """
SELECT account_name, entry_side,
  SUM(amount_jpy) as total,
  SUM(pl_contribution) as pl,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2025 AND pl_contribution IS NOT NULL
GROUP BY account_name, entry_side
ORDER BY ABS(SUM(pl_contribution)) DESC
LIMIT 20
"""
print('\n=== FY2025 top P/L accounts ===')
for row in client.query(q2).result():
    pl = row.pl or 0
    print(f'  {row.account_name:20} {row.entry_side:6} total={row.total:>10,} pl={pl:>+10,} ({row.cnt})')

# FY2023 manual_journal detail
q3 = """
SELECT je.source_id, je.account_name, je.entry_side, je.amount_jpy,
  pj.pl_contribution, pj.small_category
FROM `main-project-477501.accounting.pl_journal_entries` pj
JOIN `main-project-477501.accounting.journal_entries` je
  ON pj.source_id = je.source_id AND pj.entry_side = je.entry_side AND pj.source_table = je.source_table
WHERE pj.fiscal_year = 2023 AND pj.source_table = 'manual_journal'
ORDER BY je.source_id
"""
print('\n=== FY2023 manual_journal P/L detail ===')
total_pl = 0
for row in client.query(q3).result():
    pl = row.pl_contribution or 0
    total_pl += pl
    cat = row.small_category or '-'
    print(f'  {row.source_id:15} {row.account_name:15} {row.entry_side:6} amt={row.amount_jpy:>8,} pl={pl:>+8,} ({cat})')
print(f'  {"TOTAL":<15} pl={total_pl:>+8,}')

# Overall P/L for all years
q4 = """
SELECT fiscal_year, SUM(pl_contribution) as pl
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n=== All years P/L summary ===')
for row in client.query(q4).result():
    pl = row.pl or 0
    print(f'  FY{row.fiscal_year}: {pl:>+12,}')
