"""Fix Amazon出品アカウント明細 link columns: remove wrong hm links, create correct bt links"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
import uuid
import datetime

db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Constants
AMAZON_MODEL_ID = 'mwaoi5cfvolp1fu'
AMAZON_TABLE_NAME = 'nc_opau___Amazon出品アカウント明細'
FURIKAE_MODEL_ID = 'm6qm3ca7r4deu4y'
ACCOUNT_MODEL_ID = 'mvvmdn559d8sejw'
FURIKAE_ID_COL_ID = 'cdjvdrbest9h26g'  # 振替.Id column
ACCOUNT_ID_COL_ID = 'c1oj4o6qd4qkkud'  # freee勘定科目.Id column
BASE_ID = 'pbvdkr5cvkj4n2e'
WORKSPACE_ID = 'w75fvxkr'

def gen_id(prefix='c'):
    return prefix + uuid.uuid4().hex[:15]

now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')

print('=' * 60)
print('  Fix Amazon出品アカウント明細 Link Columns')
print('=' * 60)

# Step 1: Remove wrong hm relations
print('\n=== Step 1: Remove wrong hm relations ===')
cur.execute("DELETE FROM nc_col_relations_v2 WHERE id IN ('le1ukx5yqhhzwms', 'li6pok34903t9wz')")
print(f'  Deleted {cur.rowcount} relations')

# Step 2: Remove wrong Links columns from Amazon table
print('\n=== Step 2: Remove wrong Links columns ===')
cur.execute("DELETE FROM nc_columns_v2 WHERE id IN ('c9swsp48ktjufhq', 'c5ob552x3fsy6vi')")
print(f'  Deleted {cur.rowcount} Links columns')

# Step 3: Remove wrong FK columns from 振替 and freee勘定科目 tables
print('\n=== Step 3: Remove wrong FK columns from related tables ===')
cur.execute("DELETE FROM nc_columns_v2 WHERE id IN ('cukzo4jfrex3vej', 'cewca6m454dwscc')")
print(f'  Deleted {cur.rowcount} FK column registrations')

# Also remove physical columns from related tables
# SQLite doesn't support DROP COLUMN in older versions, so we check if it works
try:
    cur.execute('ALTER TABLE "nc_opau___振替" DROP COLUMN "nc_opau___Amazon出品アカウント明細_id"')
    print('  Dropped wrong FK column from 振替 physical table')
except Exception as e:
    print(f'  Warning: Could not drop column from 振替: {e}')

try:
    cur.execute('ALTER TABLE "nc_opau___freee勘定科目" DROP COLUMN "nc_opau___Amazon出品アカウント明細_id"')
    print('  Dropped wrong FK column from freee勘定科目 physical table')
except Exception as e:
    print(f'  Warning: Could not drop column from freee勘定科目: {e}')

conn.commit()

# Step 4: Add correct FK columns to Amazon physical table
print('\n=== Step 4: Add FK columns to Amazon table ===')
cur.execute(f'ALTER TABLE "{AMAZON_TABLE_NAME}" ADD COLUMN "nc_opau___振替_id" INTEGER')
print('  Added nc_opau___振替_id')
cur.execute(f'ALTER TABLE "{AMAZON_TABLE_NAME}" ADD COLUMN "nc_opau___freee勘定科目_id" INTEGER')
print('  Added nc_opau___freee勘定科目_id')
conn.commit()

# Step 5: Register FK columns in nc_columns_v2
print('\n=== Step 5: Register FK columns in metadata ===')
furikae_fk_col_id = gen_id()
account_fk_col_id = gen_id()

# Get max order for Amazon columns
cur.execute("""
  SELECT MAX("order") FROM nc_columns_v2
  WHERE fk_model_id = ?
""", (AMAZON_MODEL_ID,))
max_order = cur.fetchone()[0] or 13

# FK column for 振替
cur.execute("""
  INSERT INTO nc_columns_v2 (id, fk_model_id, title, column_name, uidt, dt, np, ns,
    clen, cop, pk, rqd, un, ai, "unique", cdf, cc, csn, dtx, dtxp, dtxs,
    au, system, "order", created_at, updated_at, deleted, base_id, fk_workspace_id)
  VALUES (?, ?, 'nc_opau___振替_id', 'nc_opau___振替_id', 'ForeignKey', 'integer',
    NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL,
    0, 0, ?, ?, ?, NULL, ?, ?)
""", (furikae_fk_col_id, AMAZON_MODEL_ID, max_order + 1, now, now, BASE_ID, WORKSPACE_ID))
print(f'  Registered 振替 FK column: {furikae_fk_col_id}')

# FK column for freee勘定科目
cur.execute("""
  INSERT INTO nc_columns_v2 (id, fk_model_id, title, column_name, uidt, dt, np, ns,
    clen, cop, pk, rqd, un, ai, "unique", cdf, cc, csn, dtx, dtxp, dtxs,
    au, system, "order", created_at, updated_at, deleted, base_id, fk_workspace_id)
  VALUES (?, ?, 'nc_opau___freee勘定科目_id', 'nc_opau___freee勘定科目_id', 'ForeignKey', 'integer',
    NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL,
    0, 0, ?, ?, ?, NULL, ?, ?)
""", (account_fk_col_id, AMAZON_MODEL_ID, max_order + 2, now, now, BASE_ID, WORKSPACE_ID))
print(f'  Registered freee勘定科目 FK column: {account_fk_col_id}')

conn.commit()

# Step 6: Register bt LinkToAnotherRecord columns in nc_columns_v2
print('\n=== Step 6: Register Link columns ===')
furikae_link_col_id = gen_id()
account_link_col_id = gen_id()

# Link column for 振替
cur.execute("""
  INSERT INTO nc_columns_v2 (id, fk_model_id, title, column_name, uidt, dt, np, ns,
    clen, cop, pk, rqd, un, ai, "unique", cdf, cc, csn, dtx, dtxp, dtxs,
    au, system, "order", created_at, updated_at, deleted, base_id, fk_workspace_id)
  VALUES (?, ?, '振替', NULL, 'LinkToAnotherRecord', NULL,
    NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL,
    0, 0, ?, ?, ?, NULL, ?, ?)
""", (furikae_link_col_id, AMAZON_MODEL_ID, max_order + 3, now, now, BASE_ID, WORKSPACE_ID))
print(f'  Registered 振替 Link column: {furikae_link_col_id}')

# Link column for freee勘定科目
cur.execute("""
  INSERT INTO nc_columns_v2 (id, fk_model_id, title, column_name, uidt, dt, np, ns,
    clen, cop, pk, rqd, un, ai, "unique", cdf, cc, csn, dtx, dtxp, dtxs,
    au, system, "order", created_at, updated_at, deleted, base_id, fk_workspace_id)
  VALUES (?, ?, 'freee勘定科目', NULL, 'LinkToAnotherRecord', NULL,
    NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL,
    0, 0, ?, ?, ?, NULL, ?, ?)
""", (account_link_col_id, AMAZON_MODEL_ID, max_order + 4, now, now, BASE_ID, WORKSPACE_ID))
print(f'  Registered freee勘定科目 Link column: {account_link_col_id}')

conn.commit()

# Step 7: Register bt relations in nc_col_relations_v2
print('\n=== Step 7: Register relations ===')
furikae_rel_id = gen_id('l')
account_rel_id = gen_id('l')

# bt relation for 振替: Amazon.振替_id → 振替.Id
cur.execute("""
  INSERT INTO nc_col_relations_v2 (id, type, virtual,
    fk_column_id, fk_related_model_id, fk_child_column_id, fk_parent_column_id,
    base_id, fk_workspace_id, created_at, updated_at)
  VALUES (?, 'bt', 1, ?, ?, ?, ?, ?, ?, ?, ?)
""", (furikae_rel_id, furikae_link_col_id, FURIKAE_MODEL_ID,
      furikae_fk_col_id, FURIKAE_ID_COL_ID,
      BASE_ID, WORKSPACE_ID, now, now))
print(f'  Registered 振替 bt relation: {furikae_rel_id}')

# bt relation for freee勘定科目: Amazon.freee勘定科目_id → freee勘定科目.Id
cur.execute("""
  INSERT INTO nc_col_relations_v2 (id, type, virtual,
    fk_column_id, fk_related_model_id, fk_child_column_id, fk_parent_column_id,
    base_id, fk_workspace_id, created_at, updated_at)
  VALUES (?, 'bt', 1, ?, ?, ?, ?, ?, ?, ?, ?)
""", (account_rel_id, account_link_col_id, ACCOUNT_MODEL_ID,
      account_fk_col_id, ACCOUNT_ID_COL_ID,
      BASE_ID, WORKSPACE_ID, now, now))
print(f'  Registered freee勘定科目 bt relation: {account_rel_id}')

conn.commit()

# Step 8: Verify
print('\n=== Step 8: Verify ===')
cur.execute(f'PRAGMA table_info("{AMAZON_TABLE_NAME}")')
cols = cur.fetchall()
print(f'  Physical columns ({len(cols)}):')
for c in cols:
    print(f'    {c[1]} ({c[2]})')

cur.execute("""
  SELECT c.title, c.uidt, r.type
  FROM nc_columns_v2 c
  JOIN nc_models_v2 m ON c.fk_model_id = m.id
  LEFT JOIN nc_col_relations_v2 r ON r.fk_column_id = c.id
  WHERE m.id = ?
    AND c.uidt IN ('LinkToAnotherRecord', 'ForeignKey')
    AND (c.deleted IS NULL OR c.deleted = 0)
""", (AMAZON_MODEL_ID,))
print('  Metadata link/FK columns:')
for row in cur.fetchall():
    print(f'    {row[0]} ({row[1]}) rel={row[2]}')

conn.close()
print('\nDone! Link columns fixed.')
