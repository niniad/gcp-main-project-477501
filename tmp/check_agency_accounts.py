"""代行会社テーブルの科目構成と商品計上方式への変更要件を調査"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 1. 商品勘定のID確認
print('=== 商品・棚卸資産 勘定 ===')
cur.execute('''
SELECT id, "nocodb_id", "勘定科目", "勘定科目タイプ"
FROM "nc_opau___freee勘定科目"
WHERE "勘定科目" LIKE '%商品%' OR "勘定科目" LIKE '%棚卸%'
ORDER BY id
''')
for r in cur.fetchall():
    print(f'  id={r[0]} nocodb_id={r[1]} name={r[2]} type={r[3]}')

# 2. 代行会社テーブルの freee勘定科目_id 分布
print('\n=== 代行会社 freee勘定科目_id 分布 ===')
cur.execute('''
SELECT a."nc_opau___freee勘定科目_id", f."勘定科目", COUNT(*) as cnt,
       SUM(ABS(ROUND(a."外貨金額" * COALESCE(a."為替レート", 1)))) as total_jpy
FROM "nc_opau___代行会社" a
LEFT JOIN "nc_opau___freee勘定科目" f ON a."nc_opau___freee勘定科目_id" = f."nocodb_id"
GROUP BY 1, 2
ORDER BY cnt DESC
''')
for r in cur.fetchall():
    print(f'  nocodb_id={r[0]} [{r[1]}] {r[2]}件 ¥{(r[3] or 0):,.0f}')

# 3. 年度別・科目別の金額
print('\n=== 年度別 仕入高 金額 ===')
cur.execute('''
SELECT
  SUBSTR(a."取引日付", 1, 4) as year,
  f."勘定科目",
  COUNT(*) as cnt,
  SUM(ABS(ROUND(a."外貨金額" * COALESCE(a."為替レート", 1)))) as total_jpy
FROM "nc_opau___代行会社" a
LEFT JOIN "nc_opau___freee勘定科目" f ON a."nc_opau___freee勘定科目_id" = f."nocodb_id"
WHERE f."勘定科目" = '仕入高'
  AND a."nc_opau___振替_id" IS NULL
GROUP BY 1, 2
ORDER BY 1
''')
for r in cur.fetchall():
    print(f'  FY{r[0]} [{r[1]}] {r[2]}件 ¥{(r[3] or 0):,.0f}')

# 4. 代行会社テーブルのカラム名確認
print('\n=== 代行会社 カラム確認 ===')
cur.execute('PRAGMA table_info("nc_opau___代行会社")')
cols = [r[1] for r in cur.fetchall()]
print(f'  カラム: {cols}')

conn.close()
print('\n完了')
