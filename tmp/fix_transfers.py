"""Fix transfer table issues:
1. Merge duplicate transfers: 47→12, 48→13
2. Remove transfer 1 (pre-business, 事業主借)
3. Remove transfers 20,21 (事業主借→PayPay, not inter-account)
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3

db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('=' * 60)
print('  振替テーブル修正')
print('=' * 60)

# ============================================================
# 1. Merge duplicate: transfer 47 → 12 (PayPay id=51)
# ============================================================
print('\n=== 1. 重複統合: 振替47→12 (¥300,000 PayPay→ESPRIME) ===')

# Move PayPay link from transfer 47 to transfer 12
cur.execute("""
  UPDATE "nc_opau___PayPay銀行入出金明細"
  SET "nc_opau___振替_id" = 12
  WHERE "nc_opau___振替_id" = 47
""")
print(f'  PayPay id=51: 振替_id 47→12 ({cur.rowcount} rows)')

# Delete transfer 47
cur.execute('DELETE FROM "nc_opau___振替" WHERE id = 47')
print(f'  振替47 削除')

# ============================================================
# 2. Merge duplicate: transfer 48 → 13 (PayPay id=63)
# ============================================================
print('\n=== 2. 重複統合: 振替48→13 (¥100,000 PayPay→ESPRIME) ===')

cur.execute("""
  UPDATE "nc_opau___PayPay銀行入出金明細"
  SET "nc_opau___振替_id" = 13
  WHERE "nc_opau___振替_id" = 48
""")
print(f'  PayPay id=63: 振替_id 48→13 ({cur.rowcount} rows)')

cur.execute('DELETE FROM "nc_opau___振替" WHERE id = 48')
print(f'  振替48 削除')

# ============================================================
# 3. Remove transfer 1 (2022-06-23, pre-business 事業主借)
# ============================================================
print('\n=== 3. 振替1 解除 (2022-06-23 事業開始前=事業主借) ===')

cur.execute("""
  UPDATE "nc_opau___代行会社"
  SET "nc_opau___振替_id" = NULL
  WHERE "nc_opau___振替_id" = 1
""")
print(f'  代行会社 id=1: 振替_id→NULL ({cur.rowcount} rows)')

cur.execute('DELETE FROM "nc_opau___振替" WHERE id = 1')
print(f'  振替1 削除')

# ============================================================
# 4. Remove transfers 20,21 (事業主借→PayPay, not transfer)
# ============================================================
print('\n=== 4. 振替20,21 解除 (事業主借→PayPay入金) ===')

cur.execute("""
  UPDATE "nc_opau___PayPay銀行入出金明細"
  SET "nc_opau___振替_id" = NULL
  WHERE "nc_opau___振替_id" IN (20, 21)
""")
print(f'  PayPay: 振替_id→NULL ({cur.rowcount} rows)')

cur.execute('DELETE FROM "nc_opau___振替" WHERE id IN (20, 21)')
print(f'  振替20,21 削除 ({cur.rowcount} records)')

conn.commit()

# ============================================================
# Verify
# ============================================================
print('\n=== 検証 ===')

cur.execute('SELECT COUNT(*) FROM "nc_opau___振替"')
print(f'  振替テーブル: {cur.fetchone()[0]}件 (93-5=88)')

# Re-check transfers with < 2 links
cur.execute("""
  SELECT t.id, t.振替日, t.金額, SUBSTR(t.メモ, 1, 35) as memo,
    (SELECT COUNT(*) FROM "nc_opau___楽天銀行ビジネス口座入出金明細" x WHERE x."nc_opau___振替_id" = t.id) as rakuten,
    (SELECT COUNT(*) FROM "nc_opau___PayPay銀行入出金明細" x WHERE x."nc_opau___振替_id" = t.id) as paypay,
    (SELECT COUNT(*) FROM "nc_opau___Amazon出品アカウント明細" x WHERE x."nc_opau___振替_id" = t.id) as amazon,
    (SELECT COUNT(*) FROM "nc_opau___代行会社" x WHERE x."nc_opau___振替_id" = t.id) as agency
  FROM "nc_opau___振替" t
  WHERE (
    (SELECT COUNT(*) FROM "nc_opau___楽天銀行ビジネス口座入出金明細" x WHERE x."nc_opau___振替_id" = t.id) +
    (SELECT COUNT(*) FROM "nc_opau___PayPay銀行入出金明細" x WHERE x."nc_opau___振替_id" = t.id) +
    (SELECT COUNT(*) FROM "nc_opau___Amazon出品アカウント明細" x WHERE x."nc_opau___振替_id" = t.id) +
    (SELECT COUNT(*) FROM "nc_opau___代行会社" x WHERE x."nc_opau___振替_id" = t.id)
  ) < 2
  ORDER BY t.id
""")
print('\n  片側のみの振替:')
for row in cur.fetchall():
    sides = []
    if row[4]: sides.append(f'楽天:{row[4]}')
    if row[5]: sides.append(f'PayPay:{row[5]}')
    if row[6]: sides.append(f'Amazon:{row[6]}')
    if row[7]: sides.append(f'代行:{row[7]}')
    print(f'    id={row[0]:3} {row[1]} ¥{row[2]:>8,} {", ".join(sides):20} {row[3]}')

# Verify merged transfers now have both sides
print('\n  統合後の振替12,13:')
for tid in [12, 13]:
    cur.execute("""
      SELECT t.id, t.振替日, t.金額,
        (SELECT COUNT(*) FROM "nc_opau___代行会社" x WHERE x."nc_opau___振替_id" = t.id) as agency,
        (SELECT COUNT(*) FROM "nc_opau___PayPay銀行入出金明細" x WHERE x."nc_opau___振替_id" = t.id) as paypay
      FROM "nc_opau___振替" t WHERE t.id = ?
    """, (tid,))
    r = cur.fetchone()
    print(f'    id={r[0]}: {r[1]} ¥{r[2]:,} 代行会社={r[3]} PayPay={r[4]} {"✅" if r[3] and r[4] else "❌"}')

conn.close()
print('\nDone!')
