"""
Step 1: 代行会社 freee勘定科目_id を 商品(17) → 仕入高(109) に戻す
月次三分法への切り替え
"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute('SELECT COUNT(*) FROM "nc_opau___代行会社" WHERE "nc_opau___freee勘定科目_id" = 17')
before = cur.fetchone()[0]
print(f'変更前 (商品id=17): {before}件')

cur.execute('UPDATE "nc_opau___代行会社" SET "nc_opau___freee勘定科目_id" = 109 WHERE "nc_opau___freee勘定科目_id" = 17')
updated = cur.rowcount
conn.commit()

cur.execute('SELECT COUNT(*) FROM "nc_opau___代行会社" WHERE "nc_opau___freee勘定科目_id" = 109')
after = cur.fetchone()[0]
print(f'更新: {updated}件')
print(f'変更後 (仕入高id=109): {after}件')
conn.close()
print('完了')
