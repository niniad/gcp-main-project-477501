"""Register Amazon出品アカウント明細 in NocoDB via API, then restore data from existing SQLite table"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
import json
import urllib.request

# Get token
from google.cloud import secretmanager
sm_client = secretmanager.SecretManagerServiceClient()
resp = sm_client.access_secret_version(name='projects/main-project-477501/secrets/NOCODB_API_TOKEN/versions/latest')
TOKEN = resp.payload.data.decode('utf-8').strip()

BASE_URL = 'http://localhost:8080/api/v2'
BASE_ID = 'pbvdkr5cvkj4n2e'  # EC base

def api_call(method, path, data=None):
    url = f'{BASE_URL}{path}'
    body = json.dumps(data).encode('utf-8') if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header('xc-token', TOKEN)
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode('utf-8'))

# Step 1: Check if table already exists in NocoDB API
print('=== Step 1: Check existing tables ===')
tables_resp = api_call('GET', f'/meta/bases/{BASE_ID}/tables')
existing_titles = [t['title'] for t in tables_resp.get('list', [])]
if 'Amazon出品アカウント明細' in existing_titles:
    print('  Table already registered in NocoDB!')
    sys.exit(0)
else:
    print(f'  Not found. {len(existing_titles)} tables registered.')

# Step 2: Rename existing SQLite table to backup
print('\n=== Step 2: Rename existing SQLite table ===')
db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute('SELECT COUNT(*) FROM "nc_opau___Amazon出品アカウント明細"')
count = cur.fetchone()[0]
print(f'  Existing table has {count} rows')

cur.execute('ALTER TABLE "nc_opau___Amazon出品アカウント明細" RENAME TO "_backup_Amazon出品アカウント明細"')
conn.commit()
print('  Renamed to _backup')
conn.close()

# Step 3: Create table via NocoDB API
print('\n=== Step 3: Create table via NocoDB API ===')
table_def = {
    "title": "Amazon出品アカウント明細",
    "columns": [
        {"title": "取引日", "uidt": "Date"},
        {"title": "金額", "uidt": "Number"},
        {"title": "摘要", "uidt": "LongText"},
        {"title": "settlement_id", "uidt": "SingleLineText"},
        {"title": "entry_type", "uidt": "SingleLineText"},
        {"title": "品目", "uidt": "SingleLineText"},
        {"title": "税区分", "uidt": "SingleLineText"},
    ]
}
try:
    result = api_call('POST', f'/meta/bases/{BASE_ID}/tables', table_def)
    table_id = result['id']
    table_name = result['table_name']
    print(f'  Created: id={table_id}, table_name={table_name}')
except Exception as e:
    print(f'  Error creating table: {e}')
    # Restore backup
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('ALTER TABLE "_backup_Amazon出品アカウント明細" RENAME TO "nc_opau___Amazon出品アカウント明細"')
    conn.commit()
    conn.close()
    print('  Restored backup table')
    sys.exit(1)

# Step 4: Add link columns
print('\n=== Step 4: Add link columns ===')
tables_resp = api_call('GET', f'/meta/bases/{BASE_ID}/tables')
target_ids = {}
for t in tables_resp.get('list', []):
    if t['title'] in ('振替', 'freee勘定科目'):
        target_ids[t['title']] = t['id']
        print(f'  {t["title"]}: id={t["id"]}')

for link_title, target_title in [('振替', '振替'), ('freee勘定科目', 'freee勘定科目')]:
    if target_title in target_ids:
        link_def = {
            "title": link_title,
            "uidt": "LinkToAnotherRecord",
            "parentId": table_id,
            "childId": target_ids[target_title],
            "type": "bt"
        }
        try:
            api_call('POST', f'/meta/tables/{table_id}/columns', link_def)
            print(f'  Added link: {link_title}')
        except Exception as e:
            print(f'  Link {link_title} failed: {e}')

# Step 5: Copy data from backup
print('\n=== Step 5: Copy data from backup ===')
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Get new table column names
cur.execute(f'PRAGMA table_info("{table_name}")')
new_cols_info = cur.fetchall()
new_col_names = [c[1] for c in new_cols_info]
print(f'  New table columns: {new_col_names}')

# Get backup columns
cur.execute('PRAGMA table_info("_backup_Amazon出品アカウント明細")')
backup_cols_info = cur.fetchall()
backup_col_names = [c[1] for c in backup_cols_info]
print(f'  Backup columns: {backup_col_names}')

# Read all backup data
cur.execute('SELECT * FROM "_backup_Amazon出品アカウント明細"')
backup_data = cur.fetchall()
print(f'  Backup rows: {len(backup_data)}')

# Map columns: find matching columns between backup and new
col_mapping = []  # (new_col_name, backup_col_index)
backup_col_idx = {c: i for i, c in enumerate(backup_col_names)}

for nc in new_col_names:
    if nc in backup_col_idx:
        col_mapping.append((nc, backup_col_idx[nc]))
    else:
        # Check for link column mapping
        for bc in backup_col_names:
            if '振替' in nc and '振替' in bc and nc != bc:
                col_mapping.append((nc, backup_col_idx[bc]))
                break
            elif 'freee' in nc.lower() and 'freee' in bc.lower() and nc != bc:
                col_mapping.append((nc, backup_col_idx[bc]))
                break

print(f'  Column mapping ({len(col_mapping)} columns):')
for nc, bi in col_mapping:
    print(f'    {backup_col_names[bi]} -> {nc}')

# Insert data
insert_cols = [f'"{nc}"' for nc, _ in col_mapping]
placeholders = ', '.join(['?'] * len(col_mapping))
sql = f'INSERT INTO "{table_name}" ({", ".join(insert_cols)}) VALUES ({placeholders})'

batch_size = 100
inserted = 0
for i in range(0, len(backup_data), batch_size):
    batch = backup_data[i:i+batch_size]
    rows_to_insert = []
    for row in batch:
        vals = [row[bi] for _, bi in col_mapping]
        rows_to_insert.append(vals)
    cur.executemany(sql, rows_to_insert)
    inserted += len(batch)

conn.commit()
print(f'  Inserted {inserted} rows')

# Verify
cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
verify_count = cur.fetchone()[0]
print(f'  Verification: {verify_count} rows in new table')

# Step 6: Drop backup
print('\n=== Step 6: Drop backup table ===')
cur.execute('DROP TABLE "_backup_Amazon出品アカウント明細"')
conn.commit()
print('  Dropped _backup table')

conn.close()
print('\nDone! Amazon出品アカウント明細 registered in NocoDB.')
