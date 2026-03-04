"""Fix reverse links: remove orphaned bt links on parent tables, create correct hm reverse links"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
import uuid
import datetime

db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

def gen_id(prefix='c'):
    return prefix + uuid.uuid4().hex[:15]

now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')

BASE_ID = 'pbvdkr5cvkj4n2e'
WORKSPACE_ID = 'w75fvxkr'
AMAZON_MODEL_ID = 'mwaoi5cfvolp1fu'
FURIKAE_MODEL_ID = 'm6qm3ca7r4deu4y'
ACCOUNT_MODEL_ID = 'mvvmdn559d8sejw'

# New FK column IDs on Amazon table
AMAZON_FURIKAE_FK_COL = 'ca893c8ed1d69481'
AMAZON_ACCOUNT_FK_COL = 'c3d59650daa4144e'

# Parent Id column IDs
FURIKAE_ID_COL = 'cdjvdrbest9h26g'
ACCOUNT_ID_COL = 'c1oj4o6qd4qkkud'

print('=' * 60)
print('  Fix Reverse Links on Parent Tables')
print('=' * 60)

# Step 1: Delete orphaned bt columns and relations on parent tables
print('\n=== Step 1: Delete orphaned bt columns on parent tables ===')

# Orphaned columns: cl63ziq8s7qkjd3 (振替.振替) and c01auqi1hlfxtjy (freee勘定科目.freee勘定科目)
orphan_cols = ['cl63ziq8s7qkjd3', 'c01auqi1hlfxtjy']

# Delete their relations first
cur.execute("""
  DELETE FROM nc_col_relations_v2
  WHERE fk_column_id IN ('cl63ziq8s7qkjd3', 'c01auqi1hlfxtjy')
""")
print(f'  Deleted {cur.rowcount} orphaned relations')

# Delete the columns
cur.execute("""
  DELETE FROM nc_columns_v2
  WHERE id IN ('cl63ziq8s7qkjd3', 'c01auqi1hlfxtjy')
""")
print(f'  Deleted {cur.rowcount} orphaned columns')

conn.commit()

# Step 2: Create correct hm reverse columns on parent tables
print('\n=== Step 2: Create hm reverse columns ===')

# Get max order for 振替 columns
cur.execute("""
  SELECT MAX("order") FROM nc_columns_v2 WHERE fk_model_id = ?
""", (FURIKAE_MODEL_ID,))
furikae_max_order = cur.fetchone()[0] or 10

# Get max order for freee勘定科目 columns
cur.execute("""
  SELECT MAX("order") FROM nc_columns_v2 WHERE fk_model_id = ?
""", (ACCOUNT_MODEL_ID,))
account_max_order = cur.fetchone()[0] or 10

# hm column on 振替 table → "Amazon出品アカウント明細s"
furikae_hm_col_id = gen_id()
cur.execute("""
  INSERT INTO nc_columns_v2 (id, fk_model_id, title, column_name, uidt, dt,
    pk, rqd, un, ai, "unique", au, system, "order",
    created_at, updated_at, deleted, base_id, fk_workspace_id)
  VALUES (?, ?, 'Amazon出品アカウント明細s', NULL, 'Links', NULL,
    0, 0, 0, 0, 0, 0, 0, ?, ?, ?, NULL, ?, ?)
""", (furikae_hm_col_id, FURIKAE_MODEL_ID, furikae_max_order + 1, now, now, BASE_ID, WORKSPACE_ID))
print(f'  Created hm column on 振替: {furikae_hm_col_id}')

# hm relation: 振替 → Amazon (via Amazon.振替_id)
furikae_hm_rel_id = gen_id('l')
cur.execute("""
  INSERT INTO nc_col_relations_v2 (id, type, virtual,
    fk_column_id, fk_related_model_id, fk_child_column_id, fk_parent_column_id,
    base_id, fk_workspace_id, created_at, updated_at)
  VALUES (?, 'hm', 1, ?, ?, ?, ?, ?, ?, ?, ?)
""", (furikae_hm_rel_id, furikae_hm_col_id, AMAZON_MODEL_ID,
      AMAZON_FURIKAE_FK_COL, FURIKAE_ID_COL,
      BASE_ID, WORKSPACE_ID, now, now))
print(f'  Created hm relation: {furikae_hm_rel_id}')

# hm column on freee勘定科目 table → "Amazon出品アカウント明細s"
account_hm_col_id = gen_id()
cur.execute("""
  INSERT INTO nc_columns_v2 (id, fk_model_id, title, column_name, uidt, dt,
    pk, rqd, un, ai, "unique", au, system, "order",
    created_at, updated_at, deleted, base_id, fk_workspace_id)
  VALUES (?, ?, 'Amazon出品アカウント明細s', NULL, 'Links', NULL,
    0, 0, 0, 0, 0, 0, 0, ?, ?, ?, NULL, ?, ?)
""", (account_hm_col_id, ACCOUNT_MODEL_ID, account_max_order + 1, now, now, BASE_ID, WORKSPACE_ID))
print(f'  Created hm column on freee勘定科目: {account_hm_col_id}')

# hm relation: freee勘定科目 → Amazon (via Amazon.freee勘定科目_id)
account_hm_rel_id = gen_id('l')
cur.execute("""
  INSERT INTO nc_col_relations_v2 (id, type, virtual,
    fk_column_id, fk_related_model_id, fk_child_column_id, fk_parent_column_id,
    base_id, fk_workspace_id, created_at, updated_at)
  VALUES (?, 'hm', 1, ?, ?, ?, ?, ?, ?, ?, ?)
""", (account_hm_rel_id, account_hm_col_id, AMAZON_MODEL_ID,
      AMAZON_ACCOUNT_FK_COL, ACCOUNT_ID_COL,
      BASE_ID, WORKSPACE_ID, now, now))
print(f'  Created hm relation: {account_hm_rel_id}')

conn.commit()

# Step 3: Verify
print('\n=== Step 3: Verify ===')

# Check Amazon table links
cur.execute("""
  SELECT c.title, c.uidt, r.type
  FROM nc_columns_v2 c
  LEFT JOIN nc_col_relations_v2 r ON r.fk_column_id = c.id
  WHERE c.fk_model_id = ?
    AND c.uidt IN ('LinkToAnotherRecord', 'ForeignKey', 'Links')
    AND (c.deleted IS NULL OR c.deleted = 0)
""", (AMAZON_MODEL_ID,))
print('  Amazon table:')
for row in cur.fetchall():
    print(f'    {row[0]:40} {row[1]:25} rel={row[2]}')

# Check 振替 table reverse links for Amazon
cur.execute("""
  SELECT c.title, c.uidt, r.type, r.fk_related_model_id
  FROM nc_columns_v2 c
  LEFT JOIN nc_col_relations_v2 r ON r.fk_column_id = c.id
  WHERE c.fk_model_id = ?
    AND c.uidt IN ('LinkToAnotherRecord', 'Links')
    AND (c.deleted IS NULL OR c.deleted = 0)
    AND r.fk_related_model_id = ?
""", (FURIKAE_MODEL_ID, AMAZON_MODEL_ID))
print('  振替 table (Amazon reverse):')
for row in cur.fetchall():
    print(f'    {row[0]:40} {row[1]:25} rel={row[2]}')

# Check freee勘定科目 table reverse links for Amazon
cur.execute("""
  SELECT c.title, c.uidt, r.type, r.fk_related_model_id
  FROM nc_columns_v2 c
  LEFT JOIN nc_col_relations_v2 r ON r.fk_column_id = c.id
  WHERE c.fk_model_id = ?
    AND c.uidt IN ('LinkToAnotherRecord', 'Links')
    AND (c.deleted IS NULL OR c.deleted = 0)
    AND r.fk_related_model_id = ?
""", (ACCOUNT_MODEL_ID, AMAZON_MODEL_ID))
print('  freee勘定科目 table (Amazon reverse):')
for row in cur.fetchall():
    print(f'    {row[0]:40} {row[1]:25} rel={row[2]}')

conn.close()
print('\nDone!')
