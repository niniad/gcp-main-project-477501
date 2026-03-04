"""Strip all link metadata from Amazon table to test basic record access.
Physical FK columns remain for nocodb-to-bq sync."""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3

db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

AMAZON_MODEL_ID = 'mwaoi5cfvolp1fu'

# Find all Link/FK/LinkToAnotherRecord columns for Amazon
cur.execute("""
  SELECT c.id, c.title, c.uidt
  FROM nc_columns_v2 c
  WHERE c.fk_model_id = ?
    AND c.uidt IN ('LinkToAnotherRecord', 'ForeignKey', 'Links')
    AND (c.deleted IS NULL OR c.deleted = 0)
""", (AMAZON_MODEL_ID,))
cols = cur.fetchall()
print('Columns to remove from metadata:')
col_ids = []
for c in cols:
    print(f'  {c[0]}: {c[1]} ({c[2]})')
    col_ids.append(c[0])

# Find reverse hm columns on parent tables
cur.execute("""
  SELECT c.id, c.title, m.title as table_title
  FROM nc_columns_v2 c
  JOIN nc_models_v2 m ON c.fk_model_id = m.id
  JOIN nc_col_relations_v2 r ON r.fk_column_id = c.id
  WHERE r.fk_related_model_id = ?
    AND (c.deleted IS NULL OR c.deleted = 0)
""", (AMAZON_MODEL_ID,))
reverse_cols = cur.fetchall()
print('\nReverse columns to remove:')
for c in reverse_cols:
    print(f'  {c[0]}: {c[1]} on {c[2]}')
    col_ids.append(c[0])

# Delete relations
if col_ids:
    placeholders = ','.join(['?'] * len(col_ids))
    cur.execute(f"DELETE FROM nc_col_relations_v2 WHERE fk_column_id IN ({placeholders})", col_ids)
    print(f'\nDeleted {cur.rowcount} relations')

    cur.execute(f"DELETE FROM nc_columns_v2 WHERE id IN ({placeholders})", col_ids)
    print(f'Deleted {cur.rowcount} columns from metadata')

conn.commit()

# Verify what's left
cur.execute("""
  SELECT c.title, c.uidt
  FROM nc_columns_v2 c
  WHERE c.fk_model_id = ?
    AND (c.deleted IS NULL OR c.deleted = 0)
  ORDER BY c."order"
""", (AMAZON_MODEL_ID,))
print('\nRemaining columns:')
for c in cur.fetchall():
    print(f'  {c[0]:35} {c[1]}')

# Physical table check
cur.execute('PRAGMA table_info("nc_opau___Amazon出品アカウント明細")')
print('\nPhysical table columns:')
for c in cur.fetchall():
    print(f'  {c[1]:35} {c[2]}')

conn.close()
print('\nDone! Link metadata stripped. Physical FK columns remain.')
