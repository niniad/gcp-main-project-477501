"""Check account_map and account_items relationship"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check account_map table
q1 = """
SELECT * FROM `main-project-477501.accounting.account_map`
ORDER BY logical_key
"""
print('=== account_map ===')
for row in client.query(q1).result():
    print(f'  {row.logical_key:30} acct_id={row.account_item_id} tax={row.tax_code}')

# Check account_items columns
q2 = """
SELECT column_name, data_type
FROM `main-project-477501.nocodb.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'account_items'
ORDER BY ordinal_position
"""
print('\n=== account_items columns ===')
for row in client.query(q2).result():
    print(f'  {row.column_name}: {row.data_type}')

# Check if account_items has freee IDs
q3 = """
SELECT nocodb_id, account_name, big_category, small_category
FROM `main-project-477501.nocodb.account_items`
WHERE nocodb_id IN (9, 99, 100, 101, 104, 119, 125, 126, 146, 148, 156, 32)
ORDER BY nocodb_id
"""
print('\n=== Relevant account_items (NocoDB IDs) ===')
for row in client.query(q3).result():
    big = row.big_category or ''
    small = row.small_category or ''
    print(f'  id={row.nocodb_id:>3} {row.account_name:20} big={big:10} small={small}')

# Check how old journal_entries VIEW resolved Amazon accounts
# The old VIEW SQL was replaced, but let's check the old approach
# Old approach: d.account_item_id was freee ID → need freee account table
q4 = """
SELECT view_definition
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.VIEWS`
WHERE table_name = 'journal_entries'
"""
print('\n=== Current journal_entries VIEW SQL (first 500 chars of Amazon section) ===')
for row in client.query(q4).result():
    # Find the Amazon section
    sql = row.view_definition
    idx = sql.find('Amazon')
    if idx >= 0:
        start = max(0, idx - 200)
        print(sql[start:start+800])

# Check the OLD settlement journal approach - how did it resolve account names?
# The old journal_entries VIEW used freee IDs directly in the UNNEST
# and joined with a different lookup
q5 = """
SELECT DISTINCT d.account_item_id,
  am.logical_key,
  ai_nocodb.account_name as nocodb_name
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) d
LEFT JOIN `main-project-477501.accounting.account_map` am ON d.account_item_id = am.account_item_id
LEFT JOIN `main-project-477501.nocodb.account_items` ai_nocodb ON d.account_item_id = ai_nocodb.nocodb_id
ORDER BY d.account_item_id
"""
print('\n=== Old VIEW account_item_id resolution ===')
for row in client.query(q5).result():
    name = row.nocodb_name or 'NULL'
    key = row.logical_key or 'NULL'
    print(f'  freee_id={row.account_item_id:>12} logical_key={key:30} nocodb_name={name}')
