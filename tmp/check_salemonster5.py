import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = "C:/Users/ninni/nocodb/noco.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# 事業主借のセールモンスターエントリ詳細
print("=== 事業主借のセールモンスターエントリ詳細 ===")
cur.execute("PRAGMA table_info('nc_opau___事業主借')")
cols = cur.fetchall()
col_names = [c['name'] for c in cols]
print(f"カラム: {col_names}")

cur.execute('SELECT * FROM "nc_opau___事業主借" WHERE 摘要 LIKE "%セールモンスター%"')
rows = cur.fetchall()
for r in rows:
    print()
    for cn in col_names:
        val = r[cn]
        if val is not None and val != '':
            print(f"  {cn}: {val}")

# 楽天銀行にセールモンスターからの振込がないか、金額ベースでも探す
# セールモンスター売上レポートの月次集計
print("\n=== セールモンスター売上レポート 月次サマリ ===")
cur.execute("""
    SELECT substr(売上日, 1, 7) as ym,
           SUM(税込合計金額_円_) as total_sales,
           COUNT(*) as cnt
    FROM "nc_opau___セールモンスター売上レポート"
    GROUP BY substr(売上日, 1, 7)
    ORDER BY ym
""")
for r in cur.fetchall():
    print(f"  {r['ym']}: 売上合計={r['total_sales']}円 ({r['cnt']}件)")

# 楽天銀行の入出金先内容一覧（ユニーク値）で手がかりを探す
print("\n=== 楽天銀行 入出金先内容のユニーク値（入金のみ） ===")
cur.execute('SELECT DISTINCT "入出金先内容" FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE "入出金_円_" > 0 ORDER BY "入出金先内容"')
rows = cur.fetchall()
for r in rows:
    print(f"  {r['入出金先内容']}")

conn.close()
print("\n完了")
