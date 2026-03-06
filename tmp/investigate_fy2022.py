"""
FY2022以前エントリの調査
- どのテーブルに何件あるか
- 楽天銀行の振替_id付き・未払金エントリ
"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print('=' * 60)
print('1. 事業主借テーブル（owner_contribution_entries）< 2023-02-07')
print('=' * 60)
cur.execute('''
SELECT id, "仕訳日", "金額", "摘要",
       "借方科目_id", "貸方科目_id"
FROM "nc_opau___事業主借"
WHERE "仕訳日" < '2023-02-07'
ORDER BY "仕訳日"
''')
rows = cur.fetchall()
print(f'  {len(rows)}件')
for r in rows:
    print(f'  id={r[0]:>4} {r[1]} ¥{r[2]:>10,} dr={r[4]} cr={r[5]} [{(r[3] or "")[:40]}]')

print()
print('=' * 60)
print('2. 手動仕訳テーブル（manual_journal_entries）< 2023-02-07')
print('=' * 60)
cur.execute('''
SELECT id, "仕訳日", "金額", "摘要",
       "借方科目_id", "貸方科目_id"
FROM "nc_opau___手動仕訳"
WHERE "仕訳日" < '2023-02-07'
ORDER BY "仕訳日"
''')
rows = cur.fetchall()
print(f'  {len(rows)}件')
for r in rows:
    print(f'  id={r[0]:>4} {r[1]} ¥{r[2]:>10,} dr={r[4]} cr={r[5]} [{(r[3] or "")[:40]}]')

print()
print('=' * 60)
print('3. 代行会社テーブル < 2023-02-07')
print('=' * 60)
cur.execute('''
SELECT id, "発生日", "外貨金額", "備考", "決済口座", "nc_opau___freee勘定科目_id"
FROM "nc_opau___代行会社"
WHERE "発生日" < '2023-02-07'
ORDER BY "発生日"
''')
rows = cur.fetchall()
print(f'  {len(rows)}件')
for r in rows:
    print(f'  id={r[0]:>4} {r[1]} foreign={r[2]} acct={r[5]} [{(r[3] or "")[:30]}]')

print()
print('=' * 60)
print('4. 楽天銀行 振替_id IS NOT NULL かつ 相手勘定≠(3,5,7)')
print('=' * 60)
cur.execute('''
SELECT r.id, r.transaction_date, r.amount_jpy, r.counterparty_description,
       r."nc_opau___振替_id", r."nc_opau___freee勘定科目_id",
       ai.account_name, ai.nocodb_id
FROM "nc_opau___楽天銀行ビジネス口座入出金明細" r
LEFT JOIN "nc_opau___freee勘定科目" ai ON r."nc_opau___freee勘定科目_id" = ai.id
WHERE r."nc_opau___振替_id" IS NOT NULL
  AND (ai.nocodb_id IS NULL OR ai.nocodb_id NOT IN (3, 5, 7))
ORDER BY r.transaction_date
''')
rows = cur.fetchall()
print(f'  {len(rows)}件')
for r in rows:
    print(f'  id={r[0]:>4} {r[1]} ¥{r[2]:>10,} nocodb_id={r[7]} [{r[6]}] tr={r[4]} [{(r[3] or "")[:25]}]')

print()
print('=' * 60)
print('5. 楽天銀行 振替_id IS NOT NULL 相手勘定=未払金(70)')
print('=' * 60)
cur.execute('''
SELECT r.id, r.transaction_date, r.amount_jpy, r.counterparty_description,
       r."nc_opau___振替_id", ai.account_name
FROM "nc_opau___楽天銀行ビジネス口座入出金明細" r
LEFT JOIN "nc_opau___freee勘定科目" ai ON r."nc_opau___freee勘定科目_id" = ai.id
WHERE r."nc_opau___振替_id" IS NOT NULL
  AND ai.nocodb_id = 70
ORDER BY r.transaction_date
''')
rows = cur.fetchall()
print(f'  {len(rows)}件（未払金への支払）')
total = 0
for r in rows:
    print(f'  id={r[0]:>4} {r[1]} ¥{r[2]:>10,} [{(r[3] or "")[:25]}]')
    total += (r[2] or 0)
print(f'  合計: ¥{total:,}')

print()
print('=' * 60)
print('6. 商品勘定科目の確認')
print('=' * 60)
cur.execute('''
SELECT id, nocodb_id, account_name, account_type, shortcut_num
FROM "nc_opau___freee勘定科目"
WHERE account_name LIKE '%商品%' OR account_name LIKE '%棚卸%'
ORDER BY nocodb_id
''')
for r in cur.fetchall():
    print(f'  id={r[0]} nocodb_id={r[1]} name={r[2]} type={r[3]} shortcut={r[4]}')

conn.close()
print('\n調査完了')
