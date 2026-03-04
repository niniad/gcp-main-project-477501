"""Check manual_journal id=190 column names"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3

db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Check table structure
cur.execute("PRAGMA table_info('nc_opau___手動仕訳')")
print('=== Column info ===')
for col in cur.fetchall():
    print(f'  {col}')

# Check actual data
cur.execute("SELECT * FROM 'nc_opau___手動仕訳' WHERE id = 190")
cols = [desc[0] for desc in cur.description]
row = cur.fetchone()
print('\n=== id=190 data ===')
for c, v in zip(cols, row):
    print(f'  {c}: {v}')

conn.close()
