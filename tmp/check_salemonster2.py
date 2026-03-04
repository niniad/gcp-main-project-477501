import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = "C:/Users/ninni/nocodb/noco.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# =====================================================
# 1. セールモンスター売上レポート テーブルの詳細
# =====================================================
print("=" * 80)
print("1. セールモンスター売上レポート テーブルの詳細")
print("=" * 80)

table_name = "nc_opau___セールモンスター売上レポート"

# カラム構造
print("\n  [カラム構造]")
cur.execute(f"PRAGMA table_info('{table_name}')")
columns = cur.fetchall()
for c in columns:
    print(f"    {c['name']:30s} {c['type']:15s} {'NOT NULL' if c['notnull'] else 'NULLABLE':10s} {'PK' if c['pk'] else ''}")

# レコード数
cur.execute(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
cnt = cur.fetchone()['cnt']
print(f"\n  [レコード数] {cnt}件")

# サンプルデータ（先頭10件）
print(f"\n  [先頭10件のデータ]")
cur.execute(f'SELECT * FROM "{table_name}" ORDER BY id LIMIT 10')
rows = cur.fetchall()
col_names = [c['name'] for c in columns]
for r in rows:
    print(f"\n    --- id={r['id']} ---")
    for cn in col_names:
        val = r[cn]
        if val is not None and val != '':
            print(f"      {cn}: {val}")

# 日付範囲・統計
print(f"\n  [日付系カラムの範囲]")
for cn in col_names:
    if '日' in cn or 'date' in cn.lower() or 'Date' in cn:
        try:
            cur.execute(f'SELECT MIN("{cn}") as mn, MAX("{cn}") as mx FROM "{table_name}" WHERE "{cn}" IS NOT NULL')
            r = cur.fetchone()
            print(f"    {cn}: {r['mn']} ~ {r['mx']}")
        except:
            pass

# 金額系カラムの合計
print(f"\n  [金額系カラムの統計]")
for cn in col_names:
    if '金額' in cn or '価格' in cn or '売上' in cn or '手数料' in cn or '受取' in cn or '報酬' in cn:
        try:
            cur.execute(f'SELECT SUM("{cn}") as total, COUNT("{cn}") as cnt, MIN("{cn}") as mn, MAX("{cn}") as mx FROM "{table_name}" WHERE "{cn}" IS NOT NULL')
            r = cur.fetchone()
            print(f"    {cn}: 合計={r['total']}, 件数={r['cnt']}, 最小={r['mn']}, 最大={r['mx']}")
        except:
            pass

# =====================================================
# 2. 代行会社テーブルの全内容
# =====================================================
print("\n" + "=" * 80)
print("2. 代行会社テーブルの全内容")
print("=" * 80)

table_name = "nc_opau___代行会社"

print("\n  [カラム構造]")
cur.execute(f"PRAGMA table_info('{table_name}')")
columns = cur.fetchall()
col_names = [c['name'] for c in columns]
for c in columns:
    print(f"    {c['name']:30s} {c['type']:15s} {'NOT NULL' if c['notnull'] else 'NULLABLE':10s} {'PK' if c['pk'] else ''}")

cur.execute(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
cnt = cur.fetchone()['cnt']
print(f"\n  [レコード数] {cnt}件")

print(f"\n  [全レコード]")
cur.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
rows = cur.fetchall()
for r in rows:
    print(f"\n    --- id={r['id']} ---")
    for cn in col_names:
        val = r[cn]
        if val is not None and val != '':
            print(f"      {cn}: {val}")

# セールモンスター検索
print(f"\n  [セールモンスター文字列検索]")
found = False
for cn in col_names:
    try:
        cur.execute(f'SELECT * FROM "{table_name}" WHERE "{cn}" LIKE "%セールモンスター%" OR "{cn}" LIKE "%sale%monster%" OR "{cn}" LIKE "%Sale%Monster%"')
        matches = cur.fetchall()
        if matches:
            found = True
            print(f"    カラム '{cn}' に {len(matches)}件ヒット")
    except:
        pass
if not found:
    print("    → 代行会社テーブルにセールモンスターの文字列は見つかりませんでした")

# =====================================================
# 3. freee勘定科目テーブルの全内容
# =====================================================
print("\n" + "=" * 80)
print("3. freee勘定科目テーブルの全内容")
print("=" * 80)

table_name = "nc_opau___freee勘定科目"

print("\n  [カラム構造]")
cur.execute(f"PRAGMA table_info('{table_name}')")
columns = cur.fetchall()
col_names = [c['name'] for c in columns]
for c in columns:
    print(f"    {c['name']:30s} {c['type']:15s} {'NOT NULL' if c['notnull'] else 'NULLABLE':10s} {'PK' if c['pk'] else ''}")

cur.execute(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
cnt = cur.fetchone()['cnt']
print(f"\n  [レコード数] {cnt}件")

# セールモンスター検索
print(f"\n  [セールモンスター文字列検索]")
found = False
for cn in col_names:
    try:
        cur.execute(f'SELECT * FROM "{table_name}" WHERE "{cn}" LIKE "%セールモンスター%" OR "{cn}" LIKE "%sale%monster%" OR "{cn}" LIKE "%Sale%Monster%"')
        matches = cur.fetchall()
        if matches:
            found = True
            print(f"    カラム '{cn}' に {len(matches)}件ヒット:")
            for m in matches:
                print(f"      {dict(m)}")
    except:
        pass
if not found:
    print("    → セールモンスター関連の勘定科目は見つかりませんでした")

# 全件表示（コンパクト）
print(f"\n  [全 {cnt}件の勘定科目]")
cur.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
rows = cur.fetchall()
for r in rows:
    # コンパクトに表示
    vals = {cn: r[cn] for cn in col_names if r[cn] is not None and r[cn] != '' and cn not in ('created_at', 'updated_at', 'created_by', 'updated_by', 'nc_order')}
    print(f"    {vals}")

# =====================================================
# 4. 楽天銀行明細でセールモンスター振込を検索
# =====================================================
print("\n" + "=" * 80)
print("4. 楽天銀行明細でセールモンスター振込を検索")
print("=" * 80)

table_name = "nc_opau___楽天銀行ビジネス口座入出金明細"
cur.execute(f"PRAGMA table_info('{table_name}')")
columns = cur.fetchall()
col_names = [c['name'] for c in columns]

found = False
for cn in col_names:
    try:
        cur.execute(f'SELECT * FROM "{table_name}" WHERE "{cn}" LIKE "%セールモンスター%" OR "{cn}" LIKE "%sale%monster%" OR "{cn}" LIKE "%Sale%Monster%" OR "{cn}" LIKE "%セルモン%" OR "{cn}" LIKE "%ｾｰﾙﾓﾝ%"')
        matches = cur.fetchall()
        if matches:
            found = True
            print(f"\n  カラム '{cn}' に {len(matches)}件ヒット:")
            for m in matches:
                print(f"\n    --- id={m['id']} ---")
                for c2 in col_names:
                    val = m[c2]
                    if val is not None and val != '':
                        print(f"      {c2}: {val}")
    except:
        pass
if not found:
    print("  → 楽天銀行明細にセールモンスター関連の振込は見つかりませんでした")

# =====================================================
# 5. PayPay銀行明細でセールモンスター振込を検索
# =====================================================
print("\n" + "=" * 80)
print("5. PayPay銀行明細でセールモンスター振込を検索")
print("=" * 80)

table_name = "nc_opau___PayPay銀行入出金明細"
cur.execute(f"PRAGMA table_info('{table_name}')")
columns = cur.fetchall()
col_names = [c['name'] for c in columns]

found = False
for cn in col_names:
    try:
        cur.execute(f'SELECT * FROM "{table_name}" WHERE "{cn}" LIKE "%セールモンスター%" OR "{cn}" LIKE "%sale%monster%" OR "{cn}" LIKE "%Sale%Monster%" OR "{cn}" LIKE "%セルモン%" OR "{cn}" LIKE "%ｾｰﾙﾓﾝ%"')
        matches = cur.fetchall()
        if matches:
            found = True
            print(f"\n  カラム '{cn}' に {len(matches)}件ヒット:")
            for m in matches:
                print(f"\n    --- id={m['id']} ---")
                for c2 in col_names:
                    val = m[c2]
                    if val is not None and val != '':
                        print(f"      {c2}: {val}")
    except:
        pass
if not found:
    print("  → PayPay銀行明細にセールモンスター関連の振込は見つかりませんでした")

# =====================================================
# 6. セールモンスター売上レポートの全件（末尾10件も）
# =====================================================
print("\n" + "=" * 80)
print("6. セールモンスター売上レポートの末尾10件")
print("=" * 80)

table_name = "nc_opau___セールモンスター売上レポート"
cur.execute(f"PRAGMA table_info('{table_name}')")
columns = cur.fetchall()
col_names = [c['name'] for c in columns]

cur.execute(f'SELECT * FROM "{table_name}" ORDER BY id DESC LIMIT 10')
rows = cur.fetchall()
for r in reversed(rows):
    print(f"\n    --- id={r['id']} ---")
    for cn in col_names:
        val = r[cn]
        if val is not None and val != '':
            print(f"      {cn}: {val}")

conn.close()
print("\n完了")
