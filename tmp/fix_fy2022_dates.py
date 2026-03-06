"""
開業前エントリの日付を 2023-02-07（開業日）に変更

対象:
1. nc_opau___代行会社: 発生日 < 2023-02-07 → '2023-02-07'
2. nc_opau___事業主借: 仕訳日 < 2023-02-07 → '2023-02-07'

理由:
  開業日は2023/2/7。それ以前のエントリは全て開業前活動として
  開業日付けに統一することで、FY2022期末残高=0（繰越なし）とする。
"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print('=== 変更前確認 ===')

# 代行会社
cur.execute('SELECT COUNT(*), MIN("発生日"), MAX("発生日") FROM "nc_opau___代行会社" WHERE "発生日" < \'2023-02-07\'')
r = cur.fetchone()
print(f'代行会社 対象: {r[0]}件  {r[1]} 〜 {r[2]}')

# 事業主借
cur.execute('SELECT COUNT(*), MIN("仕訳日"), MAX("仕訳日") FROM "nc_opau___事業主借" WHERE "仕訳日" < \'2023-02-07\'')
r = cur.fetchone()
print(f'事業主借 対象: {r[0]}件  {r[1]} 〜 {r[2]}')

print()
# ===== 代行会社 更新 =====
cur.execute('UPDATE "nc_opau___代行会社" SET "発生日" = \'2023-02-07\' WHERE "発生日" < \'2023-02-07\'')
agency_count = cur.rowcount
print(f'代行会社 更新: {agency_count}件')

# ===== 事業主借 更新 =====
cur.execute('UPDATE "nc_opau___事業主借" SET "仕訳日" = \'2023-02-07\' WHERE "仕訳日" < \'2023-02-07\'')
oc_count = cur.rowcount
print(f'事業主借 更新: {oc_count}件')

conn.commit()

print()
print('=== 変更後確認 ===')

# FY2022相当（< 2023-01-01）が残っていないか確認
cur.execute('SELECT COUNT(*) FROM "nc_opau___代行会社" WHERE "発生日" < \'2023-01-01\'')
print(f'代行会社 2023年前: {cur.fetchone()[0]}件（0件であるべき）')

cur.execute('SELECT COUNT(*) FROM "nc_opau___事業主借" WHERE "仕訳日" < \'2023-01-01\'')
print(f'事業主借 2023年前: {cur.fetchone()[0]}件（0件であるべき）')

# 2023-02-07に集まった件数
cur.execute('SELECT COUNT(*) FROM "nc_opau___代行会社" WHERE "発生日" = \'2023-02-07\'')
print(f'代行会社 2023-02-07: {cur.fetchone()[0]}件')

cur.execute('SELECT COUNT(*) FROM "nc_opau___事業主借" WHERE "仕訳日" = \'2023-02-07\'')
print(f'事業主借 2023-02-07: {cur.fetchone()[0]}件')

conn.close()
print('\n完了')
