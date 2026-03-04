# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# First check the schema
table = client.get_table('main-project-477501.nocodb.owner_contribution_entries')
print('=== owner_contribution_entries columns ===')
for field in table.schema:
    print(f'  {field.name} ({field.field_type})')

print()
print('=== 事業主借 (owner_contribution) FY2023 entries ===')

# Use backticks for Japanese column name
q4 = """SELECT nocodb_id, journal_date, amount, description, account_name
FROM (
  SELECT oc.nocodb_id, oc.journal_date, oc.amount, oc.description, ai.account_name
  FROM nocodb.owner_contribution_entries oc
  LEFT JOIN nocodb.account_items ai ON oc.`freee勘定科目_id` = ai.nocodb_id
  WHERE oc.journal_date >= '2023-01-01' AND oc.journal_date <= '2023-12-31'
)
ORDER BY journal_date, nocodb_id"""

total = 0
for row in client.query(q4).result():
    total += row.amount
    print(f'  oc_{row.nocodb_id} | {row.journal_date} | {row.amount:,} | {row.account_name} | {row.description}')
print(f'  FY2023 Total: {total:,}')
