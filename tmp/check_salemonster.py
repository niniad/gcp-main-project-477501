import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = "C:/Users/ninni/nocodb/noco.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=" * 80)
print("1. nc_models_v2 でセールモンスター関連テーブルを検索")
print("=" * 80)
cur.execute("SELECT id, title, table_name FROM nc_models_v2 WHERE title LIKE '%セールモンスター%' OR title LIKE '%Sale%Monster%' OR title LIKE '%sale%monster%' OR table_name LIKE '%sale%monster%'")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  id={r['id']}, title={r['title']}, table_name={r['table_name']}")
else:
    print("  → セールモンスター専用テーブルは見つかりませんでした")

# Also search broadly
print("\n  [参考] 全テーブル一覧:")
cur.execute("SELECT id, title, table_name FROM nc_models_v2 ORDER BY id")
for r in cur.fetchall():
    print(f"    id={r['id']}, title={r['title']}, table_name={r['table_name']}")

print("\n" + "=" * 80)
print("2. 全テーブルで 'セールモンスター' テキストを検索")
print("=" * 80)

# Get all user tables
cur.execute("SELECT table_name, title FROM nc_models_v2")
tables = cur.fetchall()

for tbl in tables:
    table_name = tbl['table_name']
    table_title = tbl['title']
    try:
        # Get column names
        cur.execute(f"PRAGMA table_info('{table_name}')")
        columns = cur.fetchall()
        text_cols = [c['name'] for c in columns if c['type'] in ('TEXT', 'VARCHAR', 'text', 'varchar', '')]

        if not text_cols:
            continue

        # Search each text column for セールモンスター or SaleMonster
        for col in text_cols:
            try:
                cur.execute(f"SELECT * FROM \"{table_name}\" WHERE \"{col}\" LIKE '%セールモンスター%' OR \"{col}\" LIKE '%Sale%Monster%' OR \"{col}\" LIKE '%sale%monster%' OR \"{col}\" LIKE '%セールモン%'")
                matches = cur.fetchall()
                if matches:
                    print(f"\n  テーブル: {table_title} ({table_name}), カラム: {col}")
                    print(f"  → {len(matches)}件ヒット")
                    for m in matches[:10]:
                        print(f"    {dict(m)}")
            except Exception as e:
                pass
    except Exception as e:
        pass

print("\n" + "=" * 80)
print("3. 代行会社 (agency_transactions) テーブルの構造と内容")
print("=" * 80)

# Find the table
cur.execute("SELECT table_name, title FROM nc_models_v2 WHERE title LIKE '%代行会社%'")
agency_table = cur.fetchone()
if agency_table:
    table_name = agency_table['table_name']
    print(f"  テーブル名: {agency_table['title']} ({table_name})")

    # Structure
    print("\n  [カラム構造]")
    cur.execute(f"PRAGMA table_info('{table_name}')")
    columns = cur.fetchall()
    for c in columns:
        print(f"    {c['name']} ({c['type']}) {'NOT NULL' if c['notnull'] else 'NULLABLE'} {'PK' if c['pk'] else ''}")

    # Count
    cur.execute(f"SELECT COUNT(*) as cnt FROM \"{table_name}\"")
    cnt = cur.fetchone()['cnt']
    print(f"\n  [レコード数] {cnt}件")

    # All records
    print(f"\n  [全レコード]")
    cur.execute(f"SELECT * FROM \"{table_name}\" ORDER BY id")
    rows = cur.fetchall()
    for r in rows:
        d = dict(r)
        print(f"    {d}")

    # Search for セールモンスター specifically
    print(f"\n  [セールモンスター検索]")
    text_cols = [c['name'] for c in columns if c['type'] in ('TEXT', 'VARCHAR', 'text', 'varchar', '')]
    found = False
    for col in text_cols:
        try:
            cur.execute(f"SELECT * FROM \"{table_name}\" WHERE \"{col}\" LIKE '%セールモンスター%' OR \"{col}\" LIKE '%Sale%Monster%'")
            matches = cur.fetchall()
            if matches:
                found = True
                print(f"    カラム {col} で {len(matches)}件ヒット")
                for m in matches:
                    print(f"      {dict(m)}")
        except:
            pass
    if not found:
        print("    → セールモンスターのエントリは見つかりませんでした")
else:
    print("  → 代行会社テーブルが見つかりませんでした")

print("\n" + "=" * 80)
print("4. freee勘定科目 (account_items) テーブルでセールモンスター検索")
print("=" * 80)

cur.execute("SELECT table_name, title FROM nc_models_v2 WHERE title LIKE '%勘定科目%' OR title LIKE '%account%'")
acct_tables = cur.fetchall()
for at in acct_tables:
    table_name = at['table_name']
    print(f"\n  テーブル: {at['title']} ({table_name})")

    cur.execute(f"PRAGMA table_info('{table_name}')")
    columns = cur.fetchall()
    text_cols = [c['name'] for c in columns if c['type'] in ('TEXT', 'VARCHAR', 'text', 'varchar', '')]

    found = False
    for col in text_cols:
        try:
            cur.execute(f"SELECT * FROM \"{table_name}\" WHERE \"{col}\" LIKE '%セールモンスター%' OR \"{col}\" LIKE '%Sale%Monster%' OR \"{col}\" LIKE '%セールモン%'")
            matches = cur.fetchall()
            if matches:
                found = True
                print(f"    カラム {col} で {len(matches)}件ヒット:")
                for m in matches:
                    print(f"      {dict(m)}")
        except:
            pass
    if not found:
        print(f"    → セールモンスター関連のエントリなし")

    # Also show all entries for reference
    cur.execute(f"SELECT * FROM \"{table_name}\" ORDER BY id")
    all_rows = cur.fetchall()
    print(f"\n    [全 {len(all_rows)} 件の勘定科目一覧]")
    for r in all_rows:
        d = dict(r)
        # Show compact view
        name_fields = [v for k, v in d.items() if isinstance(v, str) and len(v) > 0 and k != 'created_at' and k != 'updated_at']
        print(f"      id={d.get('id')}: {', '.join(str(v) for v in name_fields[:3])}")

print("\n" + "=" * 80)
print("5. 追加: 手動仕訳・振替テーブルでもセールモンスター検索")
print("=" * 80)

for keyword in ['手動仕訳', '振替']:
    cur.execute("SELECT table_name, title FROM nc_models_v2 WHERE title LIKE ?", (f'%{keyword}%',))
    tbls = cur.fetchall()
    for t in tbls:
        table_name = t['table_name']
        print(f"\n  テーブル: {t['title']} ({table_name})")
        cur.execute(f"PRAGMA table_info('{table_name}')")
        columns = cur.fetchall()
        text_cols = [c['name'] for c in columns if c['type'] in ('TEXT', 'VARCHAR', 'text', 'varchar', '')]
        found = False
        for col in text_cols:
            try:
                cur.execute(f"SELECT * FROM \"{table_name}\" WHERE \"{col}\" LIKE '%セールモンスター%' OR \"{col}\" LIKE '%Sale%Monster%' OR \"{col}\" LIKE '%セールモン%'")
                matches = cur.fetchall()
                if matches:
                    found = True
                    print(f"    カラム {col} で {len(matches)}件ヒット:")
                    for m in matches[:20]:
                        print(f"      {dict(m)}")
            except:
                pass
        if not found:
            print(f"    → セールモンスター関連のエントリなし")

conn.close()
print("\n完了")
