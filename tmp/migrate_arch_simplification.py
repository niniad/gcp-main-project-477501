# -*- coding: utf-8 -*-
"""
Architecture simplification migration:
1. Clear Amazon-related transfer links in PayPay/Rakuten bank tables
2. Delete Amazon-bank transfer records (id=49-114, 115, 117)
3. Migrate owner_contribution (127 rows) -> manual_journal
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'

# Table and column names as raw strings built from Unicode escapes
TBL_PAYPAY  = 'nc_opau___PayPay\u9280\u884c\u5165\u51fa\u91d1\u660e\u7d30'
TBL_RAKUTEN = 'nc_opau___\u697d\u5929\u9280\u884c\u30d3\u30b8\u30cd\u30b9\u53e3\u5ea7\u5165\u51fa\u91d1\u660e\u7d30'
TBL_FURIKAE = 'nc_opau___\u632f\u66ff'
TBL_OC      = 'nc_opau___\u4e8b\u696d\u4e3b\u501f'
TBL_MANUAL  = 'nc_opau___\u624b\u52d5\u4ed5\u8a33'

# FK column names in SQLite (with nc_opau___ prefix)
COL_FURIKAE_ID = 'nc_opau___\u632f\u66ff_id'   # 振替_id
COL_FREEE_ID   = 'nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id'  # freee勘定科目_id

# 事業主借 columns
COL_DATE = '\u4ed5\u8a33\u65e5'      # 仕訳日
COL_AMT  = '\u91d1\u984d'            # 金額
COL_DESC = '\u6458\u8981'            # 摘要
COL_DR   = '\u501f\u65b9\u79d1\u76ee_id'  # 借方科目_id
COL_CR   = '\u8cb8\u65b9\u79d1\u76ee_id'  # 貸方科目_id


def q(tbl, cols='*', where='', order=''):
    sql = f'SELECT {cols} FROM "{tbl}"'
    if where: sql += f' WHERE {where}'
    if order: sql += f' ORDER BY {order}'
    return sql


conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== Step 1: Confirmation ===")

# PayPay: freee科目=9 AND 振替_id IS NOT NULL
cur.execute(q(TBL_PAYPAY, 'id', f'"{COL_FREEE_ID}" = 9 AND "{COL_FURIKAE_ID}" IS NOT NULL', 'id'))
paypay_rows = cur.fetchall()
print(f"PayPay Amazon entries with transfer link: {len(paypay_rows)}")
print(f"  ids: {[r['id'] for r in paypay_rows]}")

# Rakuten: freee科目=9 AND 振替_id IS NOT NULL
cur.execute(q(TBL_RAKUTEN, 'id', f'"{COL_FREEE_ID}" = 9 AND "{COL_FURIKAE_ID}" IS NOT NULL', 'id'))
rakuten_rows = cur.fetchall()
print(f"Rakuten Amazon entries with transfer link: {len(rakuten_rows)}")
print(f"  ids: {[r['id'] for r in rakuten_rows[:10]]}{'...' if len(rakuten_rows)>10 else ''}")

# Transfer records to delete
cur.execute(f'SELECT id FROM "{TBL_FURIKAE}" WHERE (id BETWEEN 49 AND 114) OR id IN (115, 117) ORDER BY id')
transfer_ids = [r['id'] for r in cur.fetchall()]
print(f"Transfer records to delete: {len(transfer_ids)} (id={min(transfer_ids) if transfer_ids else '-'} to {max(transfer_ids) if transfer_ids else '-'})")

# Owner contribution count
cur.execute(f'SELECT COUNT(*) FROM "{TBL_OC}"')
oc_count = cur.fetchone()[0]
print(f"Owner contribution rows to migrate: {oc_count}")

confirm = input("\nProceed? (y/n): ").strip().lower()
if confirm != 'y':
    print("Aborted.")
    conn.close()
    sys.exit(0)

# Step 2a: Clear PayPay transfer links for Amazon
if paypay_rows:
    ids = [r['id'] for r in paypay_rows]
    cur.execute(f'UPDATE "{TBL_PAYPAY}" SET "{COL_FURIKAE_ID}"=NULL WHERE id IN ({",".join("?"*len(ids))})', ids)
    print(f"PayPay: cleared {len(ids)} transfer links")

# Step 2b: Clear Rakuten transfer links for Amazon
if rakuten_rows:
    ids = [r['id'] for r in rakuten_rows]
    cur.execute(f'UPDATE "{TBL_RAKUTEN}" SET "{COL_FURIKAE_ID}"=NULL WHERE id IN ({",".join("?"*len(ids))})', ids)
    print(f"Rakuten: cleared {len(ids)} transfer links")

# Step 2c: Delete Amazon-bank transfer records
cur.execute(f'DELETE FROM "{TBL_FURIKAE}" WHERE (id BETWEEN 49 AND 114) OR id IN (115, 117)')
print(f"Transfer table: deleted {cur.rowcount} records")

# Step 2d: Migrate owner_contribution -> manual_journal
cur.execute(f'SELECT "{COL_DATE}", "{COL_AMT}", "{COL_DESC}", "{COL_DR}", "{COL_CR}" FROM "{TBL_OC}" ORDER BY id')
oc_rows = cur.fetchall()

cur.execute(f'SELECT MAX(id) FROM "{TBL_MANUAL}"')
max_id = cur.fetchone()[0] or 0
first_new_id = max_id + 1

for row in oc_rows:
    max_id += 1
    cur.execute(
        f'INSERT INTO "{TBL_MANUAL}" (id, "{COL_DATE}", "{COL_AMT}", "{COL_DESC}", "{COL_DR}", "{COL_CR}") VALUES (?,?,?,?,?,?)',
        (max_id, row[COL_DATE], row[COL_AMT], row[COL_DESC], row[COL_DR], row[COL_CR])
    )

print(f"Manual journal: inserted {len(oc_rows)} rows (id={first_new_id}-{max_id})")

conn.commit()
print("\n=== Committed ===")

# Verification
cur.execute(f'SELECT COUNT(*) FROM "{TBL_PAYPAY}" WHERE "{COL_FREEE_ID}"=9 AND "{COL_FURIKAE_ID}" IS NOT NULL')
print(f"PayPay Amazon with transfer link (expect 0): {cur.fetchone()[0]}")

cur.execute(f'SELECT COUNT(*) FROM "{TBL_RAKUTEN}" WHERE "{COL_FREEE_ID}"=9 AND "{COL_FURIKAE_ID}" IS NOT NULL')
print(f"Rakuten Amazon with transfer link (expect 0): {cur.fetchone()[0]}")

cur.execute(f'SELECT COUNT(*) FROM "{TBL_FURIKAE}" WHERE (id BETWEEN 49 AND 114) OR id IN (115, 117)')
print(f"Deleted transfer records remaining (expect 0): {cur.fetchone()[0]}")

cur.execute(f'SELECT COUNT(*) FROM "{TBL_FURIKAE}"')
print(f"Transfer table total remaining: {cur.fetchone()[0]}")

cur.execute(f'SELECT COUNT(*) FROM "{TBL_MANUAL}"')
print(f"Manual journal total rows: {cur.fetchone()[0]}")

conn.close()
print("\nDone. Next: BQ sync -> rewrite journal_entries VIEW -> verify P/L")
