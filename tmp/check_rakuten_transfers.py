"""楽天銀行 振替_id付き・非保有口座エントリの確認"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# freee勘定科目テーブルのカラムを確認
cur.execute('PRAGMA table_info("nc_opau___freee勘定科目")')
cols = [r[1] for r in cur.fetchall()]
print(f'freee勘定科目 columns: {cols[:8]}')

# 楽天銀行 振替_id IS NOT NULL かつ 相手勘定が(3,5,7)以外
cur.execute('''
SELECT r.id, r."取引日", r."入出金_円_", r."入出金先内容",
       r."nc_opau___振替_id", fi."勘定科目", fi.id AS acct_id
FROM "nc_opau___楽天銀行ビジネス口座入出金明細" r
LEFT JOIN "nc_opau___freee勘定科目" fi ON r."nc_opau___freee勘定科目_id" = fi.id
WHERE r."nc_opau___振替_id" IS NOT NULL
  AND (fi.id IS NULL OR fi.id NOT IN (3, 5, 7))
ORDER BY r."取引日"
''')
rows = cur.fetchall()
print(f'\n楽天銀行 振替付き（保有口座以外）: {len(rows)}件')
total = 0
for r in rows:
    print(f'  id={r[0]:>4} {r[1]} ¥{(r[2] or 0):>10,} acct_id={r[6]} [{r[5]}] [{(r[3] or "")[:25]}]')
    total += (r[2] or 0)
print(f'  合計: ¥{total:,}')

# 未払金(id=70)の確認
print('\n--- 未払金エントリのみ ---')
cur.execute('''
SELECT r.id, r."取引日", r."入出金_円_", r."入出金先内容",
       r."nc_opau___振替_id", fi."勘定科目", fi.id
FROM "nc_opau___楽天銀行ビジネス口座入出金明細" r
LEFT JOIN "nc_opau___freee勘定科目" fi ON r."nc_opau___freee勘定科目_id" = fi.id
WHERE r."nc_opau___振替_id" IS NOT NULL
  AND fi.id = 70
ORDER BY r."取引日"
''')
ntt_rows = cur.fetchall()
ntt_total = 0
for r in ntt_rows:
    print(f'  id={r[0]:>4} {r[1]} ¥{(r[2] or 0):>10,} [{(r[3] or "")[:30]}]')
    ntt_total += (r[2] or 0)
print(f'  未払金合計: ¥{ntt_total:,}')

conn.close()
print('\n完了')
