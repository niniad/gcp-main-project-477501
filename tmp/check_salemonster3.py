import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = "C:/Users/ninni/nocodb/noco.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# 1. セールモンスター売上レポート サマリ
print("=== セールモンスター売上レポート サマリ ===")
cur.execute('SELECT COUNT(*) as cnt FROM "nc_opau___セールモンスター売上レポート"')
print(f"レコード数: {cur.fetchone()['cnt']}件")

cur.execute('SELECT MIN(売上日) as mn, MAX(売上日) as mx FROM "nc_opau___セールモンスター売上レポート"')
r = cur.fetchone()
print(f"売上日範囲: {r['mn']} ~ {r['mx']}")

cur.execute('SELECT SUM(税込合計金額_円_) as total FROM "nc_opau___セールモンスター売上レポート"')
print(f"税込合計金額合計: {cur.fetchone()['total']}円")

cur.execute('SELECT 売上区分名, COUNT(*) as cnt, SUM(税込合計金額_円_) as total FROM "nc_opau___セールモンスター売上レポート" GROUP BY 売上区分名')
for r in cur.fetchall():
    print(f"  {r['売上区分名']}: {r['cnt']}件, 合計{r['total']}円")

cur.execute('SELECT モール名, COUNT(*) as cnt, SUM(税込合計金額_円_) as total FROM "nc_opau___セールモンスター売上レポート" GROUP BY モール名')
print("\nモール別:")
for r in cur.fetchall():
    print(f"  {r['モール名']}: {r['cnt']}件, 合計{r['total']}円")

# 年月別集計
cur.execute("""
    SELECT substr(売上日, 1, 7) as ym, COUNT(*) as cnt, SUM(税込合計金額_円_) as total
    FROM "nc_opau___セールモンスター売上レポート"
    GROUP BY substr(売上日, 1, 7)
    ORDER BY ym
""")
print("\n年月別:")
for r in cur.fetchall():
    print(f"  {r['ym']}: {r['cnt']}件, 合計{r['total']}円")

# 2. 代行会社テーブルでセールモンスター検索
print("\n=== 代行会社テーブルでセールモンスター検索 ===")
cur.execute('SELECT COUNT(*) as cnt FROM "nc_opau___代行会社" WHERE 備考 LIKE "%セールモンスター%" OR 備考 LIKE "%セルモン%" OR 備考 LIKE "%Sale%Monster%"')
cnt = cur.fetchone()['cnt']
print(f"備考にセールモンスター: {cnt}件")

cur.execute('SELECT COUNT(*) as cnt FROM "nc_opau___代行会社" WHERE 決済口座 LIKE "%セールモンスター%" OR 決済口座 LIKE "%セルモン%"')
cnt = cur.fetchone()['cnt']
print(f"決済口座にセールモンスター: {cnt}件")

# 代行会社の決済口座一覧
cur.execute('SELECT DISTINCT 決済口座 FROM "nc_opau___代行会社" WHERE 決済口座 IS NOT NULL ORDER BY 決済口座')
print("\n代行会社テーブルの決済口座一覧:")
for r in cur.fetchall():
    print(f"  {r['決済口座']}")

# 3. freee勘定科目でセールモンスター検索
print("\n=== freee勘定科目でセールモンスター検索 ===")
cur.execute("PRAGMA table_info('nc_opau___freee勘定科目')")
columns = cur.fetchall()
col_names = [c['name'] for c in columns]
print(f"カラム: {col_names}")

found = False
for cn in col_names:
    try:
        cur.execute(f'SELECT id, * FROM "nc_opau___freee勘定科目" WHERE "{cn}" LIKE "%セールモンスター%" OR "{cn}" LIKE "%セルモン%" OR "{cn}" LIKE "%Sale%Monster%"')
        matches = cur.fetchall()
        if matches:
            found = True
            for m in matches:
                print(f"  ヒット（カラム: {cn}）: {dict(m)}")
    except:
        pass
if not found:
    print("  → セールモンスター関連の勘定科目なし")

# 全勘定科目の名前だけリスト
cur.execute('SELECT * FROM "nc_opau___freee勘定科目" ORDER BY id')
rows = cur.fetchall()
print(f"\n全勘定科目一覧 ({len(rows)}件):")
for r in rows:
    d = dict(r)
    # 名前っぽいカラムを探す
    name_val = d.get('勘定科目名') or d.get('name') or d.get('Title') or d.get('account_name')
    id_val = d.get('id')
    print(f"  id={id_val}: ", end='')
    # 全ての非メタカラムを表示
    for cn in col_names:
        if cn not in ('created_at', 'updated_at', 'created_by', 'updated_by', 'nc_order') and d[cn] is not None and d[cn] != '':
            print(f"{cn}={d[cn]}, ", end='')
    print()

# 4. 楽天銀行・PayPay銀行でセールモンスター振込検索
print("\n=== 楽天銀行でセールモンスター振込検索 ===")
for kw in ['セールモンスター', 'セルモン', 'ｾｰﾙﾓﾝ', 'SaleMonster', 'SALEMONSTER', 'salemonster']:
    cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE 摘要 LIKE "%{kw}%"')
    cnt = cur.fetchone()['cnt']
    if cnt > 0:
        print(f"  摘要 LIKE '%{kw}%': {cnt}件")
        cur.execute(f'SELECT id, 取引日, 摘要, 入金, 出金 FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE 摘要 LIKE "%{kw}%"')
        for r in cur.fetchall():
            print(f"    id={r['id']}, 日付={r['取引日']}, 摘要={r['摘要']}, 入金={r['入金']}, 出金={r['出金']}")

# 楽天銀行の摘要カラム名を確認
cur.execute("PRAGMA table_info('nc_opau___楽天銀行ビジネス口座入出金明細')")
cols = cur.fetchall()
text_cols = [c['name'] for c in cols if 'TEXT' in str(c['type']).upper() or 'VARCHAR' in str(c['type']).upper()]
print(f"  テキストカラム: {text_cols}")

# 全テキストカラムで検索
for col in text_cols:
    for kw in ['セールモンスター', 'セルモン', 'ｾｰﾙﾓﾝ', 'SaleMonster', 'SALEMONSTER']:
        try:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE "{col}" LIKE "%{kw}%"')
            cnt = cur.fetchone()['cnt']
            if cnt > 0:
                print(f"  カラム'{col}' LIKE '%{kw}%': {cnt}件")
                cur.execute(f'SELECT * FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE "{col}" LIKE "%{kw}%"')
                for r in cur.fetchall():
                    print(f"    {dict(r)}")
        except:
            pass

print("\n=== PayPay銀行でセールモンスター振込検索 ===")
cur.execute("PRAGMA table_info('nc_opau___PayPay銀行入出金明細')")
cols = cur.fetchall()
text_cols = [c['name'] for c in cols if 'TEXT' in str(c['type']).upper() or 'VARCHAR' in str(c['type']).upper()]
for col in text_cols:
    for kw in ['セールモンスター', 'セルモン', 'ｾｰﾙﾓﾝ', 'SaleMonster', 'SALEMONSTER']:
        try:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___PayPay銀行入出金明細" WHERE "{col}" LIKE "%{kw}%"')
            cnt = cur.fetchone()['cnt']
            if cnt > 0:
                print(f"  カラム'{col}' LIKE '%{kw}%': {cnt}件")
                cur.execute(f'SELECT * FROM "nc_opau___PayPay銀行入出金明細" WHERE "{col}" LIKE "%{kw}%"')
                for r in cur.fetchall():
                    print(f"    {dict(r)}")
        except:
            pass

# 5. 手動仕訳でセールモンスター検索
print("\n=== 手動仕訳でセールモンスター検索 ===")
cur.execute("PRAGMA table_info('nc_opau___手動仕訳')")
cols = cur.fetchall()
text_cols = [c['name'] for c in cols if 'TEXT' in str(c['type']).upper() or 'VARCHAR' in str(c['type']).upper()]
for col in text_cols:
    for kw in ['セールモンスター', 'セルモン']:
        try:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___手動仕訳" WHERE "{col}" LIKE "%{kw}%"')
            cnt = cur.fetchone()['cnt']
            if cnt > 0:
                print(f"  カラム'{col}' LIKE '%{kw}%': {cnt}件")
                cur.execute(f'SELECT * FROM "nc_opau___手動仕訳" WHERE "{col}" LIKE "%{kw}%"')
                for r in cur.fetchall():
                    d = dict(r)
                    print(f"    {d}")
        except:
            pass

conn.close()
print("\n完了")
