# -*- coding: utf-8 -*-
"""FY2023 BS修正: NocoDB SQLite 5件の修正"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')

db = sqlite3.connect('C:/Users/ninni/nocodb/noco.db')
cur = db.cursor()

# 1. 楽天銀行 id=3: freee勘定科目_id 3→7 (THE直行便→YP)
cur.execute('SELECT id, "nc_opau___freee勘定科目_id" FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE id = 3')
print(f'修正前 楽天銀行 id=3: {cur.fetchone()}')
cur.execute('UPDATE "nc_opau___楽天銀行ビジネス口座入出金明細" SET "nc_opau___freee勘定科目_id" = 7 WHERE id = 3')
print('  → freee勘定科目_id = 7 (YP) に変更')

# 2. 楽天銀行 id=13: freee勘定科目_id 3→7 (THE直行便→YP)
cur.execute('SELECT id, "nc_opau___freee勘定科目_id" FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE id = 13')
print(f'修正前 楽天銀行 id=13: {cur.fetchone()}')
cur.execute('UPDATE "nc_opau___楽天銀行ビジネス口座入出金明細" SET "nc_opau___freee勘定科目_id" = 7 WHERE id = 13')
print('  → freee勘定科目_id = 7 (YP) に変更')

# 3. 手動仕訳 id=194: 削除
cur.execute('SELECT id, "仕訳日", "借方科目_id", "貸方科目_id", "金額", "摘要" FROM "nc_opau___手動仕訳" WHERE id = 194')
row = cur.fetchone()
print(f'削除対象 手動仕訳 id=194: {row}')
cur.execute('DELETE FROM "nc_opau___手動仕訳" WHERE id = 194')
print('  → 削除完了')

# 4. 手動仕訳追加: Dr.YP(7) 200,000 / Cr.事業主借(85) 2023-01-01
cur.execute('SELECT MAX(id) FROM "nc_opau___手動仕訳"')
max_id = cur.fetchone()[0]
next_order = max_id + 1.0

cur.execute("""
INSERT INTO "nc_opau___手動仕訳" (
  created_at, updated_at, nc_order,
  "仕訳日", "借方科目_id", "貸方科目_id", "金額", "摘要", "ソース", "仕訳区分",
  "nc_opau___freee勘定科目_id", "nc_opau___freee勘定科目_id1"
) VALUES (
  datetime('now'), datetime('now'), ?,
  '2023-01-01', 7, 85, 200000,
  '個人口座からYP預入（2022/06/21 三井住友銀行イーウーパスポート 200,000円）',
  'MF調整', '期首調整',
  7, 85
)
""", (next_order,))
new_id1 = cur.lastrowid
print(f'追加 手動仕訳 id={new_id1}: Dr.YP(7) 200,000 / Cr.事業主借(85)')

# 5. 手動仕訳追加: Dr.開業費(15) 119,559 / Cr.YP(7) 2023-01-01
cur.execute("""
INSERT INTO "nc_opau___手動仕訳" (
  created_at, updated_at, nc_order,
  "仕訳日", "借方科目_id", "貸方科目_id", "金額", "摘要", "ソース", "仕訳区分",
  "nc_opau___freee勘定科目_id", "nc_opau___freee勘定科目_id1"
) VALUES (
  datetime('now'), datetime('now'), ?,
  '2023-01-01', 15, 7, 119559,
  'YP経由開業前サンプル費用（2022/6/23-2022/12/13 イーウーパスポート 5,879元×20.3366円）',
  'MF調整', '期首調整',
  15, 7
)
""", (next_order + 1,))
new_id2 = cur.lastrowid
print(f'追加 手動仕訳 id={new_id2}: Dr.開業費(15) 119,559 / Cr.YP(7)')

db.commit()

# 検証
print()
print('=== 検証 ===')
cur.execute('SELECT id, "nc_opau___freee勘定科目_id" FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE id IN (3, 13)')
for r in cur.fetchall():
    print(f'  楽天銀行 id={r[0]}: freee勘定科目_id={r[1]}')

cur.execute('SELECT id FROM "nc_opau___手動仕訳" WHERE id = 194')
print(f'  手動仕訳 id=194 存在: {cur.fetchone() is not None}')

cur.execute(f'SELECT id, "仕訳日", "借方科目_id", "貸方科目_id", "金額", "摘要" FROM "nc_opau___手動仕訳" WHERE id >= {new_id1}')
for r in cur.fetchall():
    print(f'  手動仕訳 id={r[0]}: {r[1]} Dr={r[2]} Cr={r[3]} ¥{r[4]:,} {r[5][:50]}')

db.close()
print('\nNocoDB修正完了。')
