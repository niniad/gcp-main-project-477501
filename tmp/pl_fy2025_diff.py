"""Check FY2025 P/L change causes"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# FY2025 P/L by account (top contributors)
q = """
SELECT account_name,
  SUM(pl_contribution) as pl,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2025 AND pl_contribution IS NOT NULL
GROUP BY account_name
ORDER BY ABS(SUM(pl_contribution)) DESC
"""
print('=== FY2025 P/L by account (current) ===')
print('Old reference total: -550,091')
total = 0
for row in client.query(q).result():
    pl = row.pl or 0
    total += pl
    print(f'  {row.account_name:20} {pl:>+12,} ({row.cnt})')
print(f'  {"TOTAL":<20} {total:>+12,}')
print(f'  Expected old: -550,091, Diff: {total - (-550091):+,}')

# Check owner_contribution FY2025 entries
q2 = """
SELECT source_id, account_name, entry_side, amount_jpy, pl_contribution
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2025 AND source_table = 'owner_contribution' AND pl_contribution IS NOT NULL
ORDER BY source_id
"""
print('\n=== FY2025 owner_contribution P/L entries ===')
for row in client.query(q2).result():
    pl = row.pl_contribution or 0
    print(f'  {row.source_id:15} {row.account_name:20} {row.entry_side:6} amt={row.amount_jpy:>8,} pl={pl:>+8,}')

# Check NTT FY2025 to verify +16
q3 = """
SELECT SUM(pl_contribution) as pl, COUNT(pl_contribution) as cnt
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2025 AND source_table = 'ntt_finance'
"""
print('\n=== FY2025 NTT total ===')
for row in client.query(q3).result():
    print(f'  P/L: {row.pl:+,} ({row.cnt} rows)')

# system_design reference values
print('\n=== Reference values from mf_bq_reconciliation.md ===')
print('  FY2025 old (system_design): -550,091')
print('  FY2025 new (current BQ):    -489,429')
print('  Difference: +60,662')
print()
print('  Possible causes:')
print('  1. NTT fix (Step 0): +16')
print('  2. Owner contribution fix (Step 5): 3 entries with NULL account IDs')
print('  3. New NocoDB data synced (any pending changes)')
