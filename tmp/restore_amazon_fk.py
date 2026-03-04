"""Restore FK data (振替_id, freee勘定科目_id) from BQ to NocoDB Amazon table"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
from google.cloud import bigquery

bq_client = bigquery.Client(project='main-project-477501')

# Get FK data from BQ
q = """
SELECT nocodb_id, CAST(`振替_id` AS INT64) as transfer_id, `freee勘定科目_id` as account_id
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE `振替_id` IS NOT NULL OR `freee勘定科目_id` IS NOT NULL
"""
print('=== Fetching FK data from BQ ===')
rows = list(bq_client.query(q).result())
print(f'  {len(rows)} rows with FK data')

# Update NocoDB
db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

updated_transfer = 0
updated_account = 0

for row in rows:
    if row.transfer_id is not None:
        cur.execute("""
          UPDATE "nc_opau___Amazon出品アカウント明細"
          SET "nc_opau___振替_id" = ?
          WHERE id = ?
        """, (row.transfer_id, row.nocodb_id))
        updated_transfer += cur.rowcount

    if row.account_id is not None:
        cur.execute("""
          UPDATE "nc_opau___Amazon出品アカウント明細"
          SET "nc_opau___freee勘定科目_id" = ?
          WHERE id = ?
        """, (row.account_id, row.nocodb_id))
        updated_account += cur.rowcount

conn.commit()

print(f'\n=== Results ===')
print(f'  Updated 振替_id: {updated_transfer} rows')
print(f'  Updated freee勘定科目_id: {updated_account} rows')

# Verify
cur.execute("""
  SELECT COUNT(*) as total,
    SUM(CASE WHEN "nc_opau___振替_id" IS NOT NULL THEN 1 ELSE 0 END) as has_transfer,
    SUM(CASE WHEN "nc_opau___freee勘定科目_id" IS NOT NULL THEN 1 ELSE 0 END) as has_account
  FROM "nc_opau___Amazon出品アカウント明細"
""")
row = cur.fetchone()
print(f'\n=== Verification ===')
print(f'  Total rows: {row[0]}')
print(f'  With 振替_id: {row[1]}')
print(f'  With freee勘定科目_id: {row[2]}')

conn.close()
print('\nDone!')
