"""Check pl_journal_entries for NULL join issues"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check which account_names have NULL pl_contribution
q = """
SELECT je.account_name, je.source_table, COUNT(*) as cnt,
  SUM(je.amount_jpy) as total,
  COUNT(pj.pl_contribution) as pl_non_null,
  COUNT(*) - COUNT(pj.pl_contribution) as pl_null
FROM `main-project-477501.accounting.journal_entries` je
LEFT JOIN `main-project-477501.accounting.pl_journal_entries` pj
  ON je.source_id = pj.source_id AND je.entry_side = pj.entry_side AND je.source_table = pj.source_table
GROUP BY 1, 2
HAVING COUNT(*) - COUNT(pj.pl_contribution) > 0
ORDER BY pl_null DESC
"""
print('=== account_names with NULL pl_contribution ===')
for row in client.query(q).result():
    print(f'  {row.account_name:25} [{row.source_table:20}] {row.cnt} entries, {row.pl_null} NULL')

# Check P/L with COALESCE
q2 = """
SELECT fiscal_year, COALESCE(SUM(pl_contribution), 0) AS net_income
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print()
print('=== P/L (with COALESCE) ===')
for row in client.query(q2).result():
    print(f'  FY{row.fiscal_year}: {row.net_income:+,}')

# Check if Amazon account names are in account_items
q3 = """
SELECT DISTINCT je.account_name,
  CASE WHEN ai.account_name IS NOT NULL THEN 'found' ELSE 'MISSING' END as status
FROM `main-project-477501.accounting.journal_entries` je
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON je.account_name = ai.account_name
WHERE je.source_table = 'amazon_settlement'
ORDER BY je.account_name
"""
print()
print('=== Amazon account_names in account_items ===')
for row in client.query(q3).result():
    print(f'  {row.account_name:25} {row.status}')
