import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('C:/Users/ninni/nocodb/noco.db')
cur = conn.cursor()

# 1. 楽天銀行 id=106: 工具器具備品(47) → 消耗品費(131)
cur.execute("""
UPDATE "nc_opau___楽天銀行ビジネス口座入出金明細"
SET "nc_opau___freee勘定科目_id" = 131,
    updated_at = datetime('now')
WHERE id = 106
""")
print(f"楽天銀行 id=106 更新: {cur.rowcount}件")

# 2. 手動仕訳 id=202 削除（工具器具備品→消耗品費の仕訳は不要）
cur.execute("DELETE FROM 'nc_opau___手動仕訳' WHERE id = 202")
print(f"手動仕訳 id=202 削除: {cur.rowcount}件")

conn.commit()

# 確認
cur.execute("""
SELECT id, "取引日", "入出金_円_", "nc_opau___freee勘定科目_id", "入出金先内容"
FROM "nc_opau___楽天銀行ビジネス口座入出金明細"
WHERE id = 106
""")
print()
print("=== 楽天銀行 id=106 確認 ===")
for r in cur.fetchall():
    print(r)

cur.execute("SELECT id FROM 'nc_opau___手動仕訳' WHERE id = 202")
if cur.fetchone():
    print("❌ id=202 まだ存在")
else:
    print("✓ 手動仕訳 id=202 削除済み")

# 現在の手動仕訳一覧
cur.execute("SELECT id, 仕訳日, 金額, 摘要 FROM 'nc_opau___手動仕訳' ORDER BY id")
print()
print("=== 手動仕訳 現在一覧 ===")
for r in cur.fetchall():
    print(r)

conn.close()
