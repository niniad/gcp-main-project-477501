"""Check Amazon account statements in BQ for FK data"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Check columns
q1 = """
SELECT column_name, data_type
FROM `main-project-477501.nocodb.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'amazon_account_statements'
ORDER BY ordinal_position
"""
print('=== BQ amazon_account_statements columns ===')
for row in client.query(q1).result():
    print(f'  {row.column_name}: {row.data_type}')

# Check FK data
q2 = """
SELECT 
  COUNT(*) as total,
  COUNTIF(`振替_id` IS NOT NULL) as has_transfer,
  COUNTIF(`freee勘定科目_id` IS NOT NULL) as has_account
FROM `main-project-477501.nocodb.amazon_account_statements`
"""
print('\n=== FK data coverage ===')
for row in client.query(q2).result():
    print(f'  Total: {row.total}')
    print(f'  With 振替_id: {row.has_transfer}')
    print(f'  With freee勘定科目_id: {row.has_account}')

# Sample FK values
q3 = """
SELECT `振替_id`, `freee勘定科目_id`, COUNT(*) as cnt
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE `振替_id` IS NOT NULL OR `freee勘定科目_id` IS NOT NULL
GROUP BY `振替_id`, `freee勘定科目_id`
ORDER BY cnt DESC
LIMIT 20
"""
print('\n=== FK value distribution ===')
for row in client.query(q3).result():
    print(f'  振替_id={row.振替_id}, freee勘定科目_id={row.freee勘定科目_id}: {row.cnt}件')

