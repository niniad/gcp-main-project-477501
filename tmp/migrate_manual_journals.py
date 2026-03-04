"""Migrate manual journal entries to appropriate tables.

Move plan:
- id=190,192,196,199 → 事業主借
- id=191,193 → Amazon出品アカウント明細
- id=197,198 → stay (fix FK on 198)
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
import datetime

db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()
now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')

print('=' * 60)
print('  手動仕訳エントリの移動')
print('=' * 60)

# ============================================================
# Step 1: Move 4 entries to 事業主借
# ============================================================
print('\n=== Step 1: 事業主借へ移動 (4件) ===')

# Get source data
cur.execute("""
  SELECT id, 仕訳日, 金額, 摘要, 借方科目_id, 貸方科目_id
  FROM "nc_opau___手動仕訳"
  WHERE id IN (190, 192, 196, 199)
  ORDER BY id
""")
manual_rows = cur.fetchall()

# Get current max id and order for 事業主借
cur.execute('SELECT MAX(id), MAX(nc_order) FROM "nc_opau___事業主借"')
max_oc_id, max_oc_order = cur.fetchone()

for i, row in enumerate(manual_rows):
    mid, journal_date, amount, desc, debit_id, credit_id = row
    new_id = max_oc_id + 1 + i
    new_order = max_oc_order + 1 + i

    if mid == 199:
        # Reverse entry: Dr.事業主借(85) / Cr.支払手数料(148)
        # → 事業主借: 借方=支払手数料(148), 金額=-13240
        debit_account = credit_id  # 148 (支払手数料)
        final_amount = -amount  # -13240
    elif mid == 192:
        # BQ integer columns are swapped: 借方科目_id=85, 貸方科目_id=162
        # But actual intent is Dr.雑費(162) / Cr.事業主借(85)
        # → 事業主借: 借方=雑費(162), 金額=85177
        debit_account = credit_id  # 162 (雑費) -- 貸方科目_id column has the debit value
        final_amount = amount
    else:
        # Normal: Dr.X / Cr.事業主借
        debit_account = debit_id
        final_amount = amount

    cur.execute("""
      INSERT INTO "nc_opau___事業主借"
        (id, created_at, updated_at, nc_order, 仕訳日, 金額, 摘要, ソース,
         借方科目_id, 貸方科目_id, "nc_opau___freee勘定科目_id", "nc_opau___freee勘定科目_id1")
      VALUES (?, ?, ?, ?, ?, ?, ?, '手動仕訳から移動',
              ?, 85, ?, 85)
    """, (new_id, now, now, new_order, journal_date, final_amount,
          desc, debit_account, debit_account))

    print(f'  id={mid} → 事業主借 id={new_id}: 借方={debit_account}, 金額={final_amount}')

conn.commit()

# ============================================================
# Step 2: Move 2 entries to Amazon出品アカウント明細
# ============================================================
print('\n=== Step 2: Amazon出品アカウント明細へ移動 (2件) ===')

cur.execute("""
  SELECT id, 仕訳日, 金額, 摘要, 借方科目_id, 貸方科目_id
  FROM "nc_opau___手動仕訳"
  WHERE id IN (191, 193)
  ORDER BY id
""")
amazon_rows = cur.fetchall()

cur.execute('SELECT MAX(id), MAX(nc_order) FROM "nc_opau___Amazon出品アカウント明細"')
max_az_id, max_az_order = cur.fetchone()

for i, row in enumerate(amazon_rows):
    mid, journal_date, amount, desc, debit_id, credit_id = row
    new_id = max_az_id + 1 + i
    new_order = max_az_order + 1 + i

    # Dr.Amazon出品アカウント(9) / Cr.事業主借(85)
    # Amazon明細: positive amount, freee勘定科目_id = credit_id (85=事業主借)
    cur.execute("""
      INSERT INTO "nc_opau___Amazon出品アカウント明細"
        (id, created_at, updated_at, nc_order, 取引日, 金額, 摘要,
         entry_type, "nc_opau___freee勘定科目_id")
      VALUES (?, ?, ?, ?, ?, ?, ?, 'ADJUSTMENT', ?)
    """, (new_id, now, now, new_order, journal_date, amount,
          desc, credit_id))  # credit_id=85 (事業主借)

    print(f'  id={mid} → Amazon明細 id={new_id}: 金額={amount}, freee勘定科目_id={credit_id}')

conn.commit()

# ============================================================
# Step 3: Fix FK on id=198 (credit FK is NULL, should be 105)
# ============================================================
print('\n=== Step 3: id=198 の貸方FK修正 ===')

cur.execute("""
  UPDATE "nc_opau___手動仕訳"
  SET "nc_opau___freee勘定科目_id1" = 105
  WHERE id = 198
""")
print(f'  id=198: credit FK → 105 (為替差損益)')

conn.commit()

# ============================================================
# Step 4: Delete moved entries from 手動仕訳
# ============================================================
print('\n=== Step 4: 手動仕訳から移動済エントリ削除 ===')

cur.execute("""
  DELETE FROM "nc_opau___手動仕訳"
  WHERE id IN (190, 191, 192, 193, 196, 199)
""")
print(f'  削除: {cur.rowcount}件')

conn.commit()

# ============================================================
# Verify
# ============================================================
print('\n=== 検証 ===')

cur.execute('SELECT COUNT(*) FROM "nc_opau___手動仕訳"')
print(f'  手動仕訳: {cur.fetchone()[0]}件 (2件残留予定)')

cur.execute('SELECT id, 仕訳日, 金額, 借方科目_id, 摘要 FROM "nc_opau___手動仕訳" ORDER BY id')
for row in cur.fetchall():
    print(f'    id={row[0]}: {row[1]} ¥{row[2]:,} Dr.{row[3]} {row[4][:40]}...')

cur.execute('SELECT COUNT(*) FROM "nc_opau___事業主借"')
print(f'  事業主借: {cur.fetchone()[0]}件')

cur.execute("""
  SELECT id, 仕訳日, 金額, 借方科目_id, 摘要
  FROM "nc_opau___事業主借"
  WHERE id > 124 ORDER BY id
""")
for row in cur.fetchall():
    print(f'    id={row[0]}: {row[1]} ¥{row[2]:,} Dr.{row[3]} {row[4][:40]}...')

cur.execute('SELECT COUNT(*) FROM "nc_opau___Amazon出品アカウント明細"')
print(f'  Amazon明細: {cur.fetchone()[0]}件')

cur.execute("""
  SELECT id, 取引日, 金額, entry_type, "nc_opau___freee勘定科目_id"
  FROM "nc_opau___Amazon出品アカウント明細"
  WHERE id > 694 ORDER BY id
""")
for row in cur.fetchall():
    print(f'    id={row[0]}: {row[1]} ¥{row[2]:,} type={row[3]} fk={row[4]}')

conn.close()
print('\nDone!')
