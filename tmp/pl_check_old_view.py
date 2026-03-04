"""Check old journal_entries VIEW approach for Amazon section"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Get the full current journal_entries VIEW SQL to understand the structure
q0 = """
SELECT view_definition
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.VIEWS`
WHERE table_name = 'journal_entries'
"""
print('=== Full journal_entries VIEW SQL ===')
for row in client.query(q0).result():
    # Print full SQL
    print(row.view_definition)

print('\n\n=== account_map columns ===')
q1 = """
SELECT column_name FROM `main-project-477501.accounting.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'account_map'
"""
for row in client.query(q1).result():
    print(f'  {row.column_name}')

# Check account_map data
print('\n=== account_map full data ===')
q2 = """SELECT * FROM `main-project-477501.accounting.account_map`"""
for row in client.query(q2).result():
    print(f'  {dict(row)}')
