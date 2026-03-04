"""Check filter impact: is_transfer vs 振替_id for each table"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# First list all nocodb tables
q0 = """
SELECT table_name FROM `main-project-477501.nocodb.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name
"""
print('=== NocoDB BQ tables ===')
for row in client.query(q0).result():
    print(f'  {row.table_name}')
