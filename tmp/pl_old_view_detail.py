"""Check old settlement_journal_payload_view structure in detail"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check the VIEW SQL
q0 = """
SELECT view_definition
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.VIEWS`
WHERE table_name = 'settlement_journal_payload_view'
"""
print('=== settlement_journal_payload_view SQL ===')
for row in client.query(q0).result():
    print(row.view_definition[:3000])

# Check actual data sample
q1 = """
SELECT s.settlement_id, s.issue_date,
  EXTRACT(YEAR FROM s.issue_date) as yr,
  d.entry_side, d.account_item_id, d.amount, d.description
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) d
ORDER BY s.settlement_id
LIMIT 20
"""
print('\n\n=== Sample data from old view ===')
for row in client.query(q1).result():
    print(f'  sid={row.settlement_id} date={row.issue_date} yr={row.yr} {row.entry_side:6} acct_id={row.account_item_id} amt={row.amount:>10,} {row.description[:30]}')

# Check distinct account_item_ids
q2 = """
SELECT DISTINCT d.account_item_id, ai.account_name
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) d
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON d.account_item_id = ai.nocodb_id
ORDER BY d.account_item_id
"""
print('\n=== Distinct account_item_ids in old view ===')
for row in client.query(q2).result():
    acct = row.account_name or 'NULL'
    print(f'  id={row.account_item_id} -> {acct}')
