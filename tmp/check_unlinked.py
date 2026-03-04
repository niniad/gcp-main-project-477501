import sys
import sqlite3

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = "C:/Users/ninni/nocodb/noco.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=" * 70)
print("1. 楽天銀行 データ範囲")
print("=" * 70)
cur.execute("""
    SELECT MIN(transaction_date) as min_date, MAX(transaction_date) as max_date, COUNT(*) as cnt
    FROM nc_opau___楽天銀行ビジネス口座入出金明細
""")
row = cur.fetchone()
print(f"  件数: {row['cnt']}")
print(f"  最古: {row['min_date']}")
print(f"  最新: {row['max_date']}")

print()
print("=" * 70)
print("2. PayPay銀行 データ範囲")
print("=" * 70)
cur.execute("""
    SELECT MIN(transaction_date) as min_date, MAX(transaction_date) as max_date, COUNT(*) as cnt
    FROM `nc_opau___PayPay銀行入出金明細`
""")
row = cur.fetchone()
print(f"  件数: {row['cnt']}")
print(f"  最古: {row['min_date']}")
print(f"  最新: {row['max_date']}")

print()
print("=" * 70)
print("3. Amazon 未リンク DEPOSIT行")
print("=" * 70)

# First check column names
cur.execute("PRAGMA table_info(`nc_opau___Amazon出品アカウント明細`)")
cols = cur.fetchall()
print("  カラム一覧:")
for c in cols:
    print(f"    {c['cid']}: {c['name']} ({c['type']})")

print()
# Check distinct entry_type values
cur.execute("""
    SELECT DISTINCT entry_type, COUNT(*) as cnt
    FROM `nc_opau___Amazon出品アカウント明細`
    GROUP BY entry_type
    ORDER BY cnt DESC
""")
print("  entry_type 分布:")
for r in cur.fetchall():
    print(f"    '{r['entry_type']}': {r['cnt']}件")

print()
# Get unlinked DEPOSIT rows - try multiple column name variants
try:
    cur.execute("""
        SELECT id, transaction_date, amount, description, entry_type, `nc_opau___振替_id`
        FROM `nc_opau___Amazon出品アカウント明細`
        WHERE entry_type = 'DEPOSIT'
          AND (`nc_opau___振替_id` IS NULL OR `nc_opau___振替_id` = '')
        ORDER BY transaction_date
    """)
    rows = cur.fetchall()
    print(f"  未リンクDEPOSIT件数: {len(rows)}")
    for r in rows:
        print(f"    id={r['id']}, date={r['transaction_date']}, amount={r['amount']}, desc={r['description']}")
except Exception as e:
    print(f"  エラー: {e}")
    # Try to find the correct column name for transfer link
    cur.execute("PRAGMA table_info(`nc_opau___Amazon出品アカウント明細`)")
    cols = cur.fetchall()
    transfer_col = [c['name'] for c in cols if '振替' in c['name'] or 'transfer' in c['name'].lower()]
    print(f"  振替関連カラム: {transfer_col}")

print()
print("=" * 70)
print("4. 楽天銀行 Amazon入金（freee勘定科目_id=9, 未リンク）")
print("=" * 70)

# Check column names for 楽天銀行
cur.execute("PRAGMA table_info(`nc_opau___楽天銀行ビジネス口座入出金明細`)")
cols = cur.fetchall()
col_names = [c['name'] for c in cols]
# Find transfer and account columns
transfer_col = [c for c in col_names if '振替' in c or 'transfer' in c.lower()]
account_col = [c for c in col_names if '勘定' in c or 'account' in c.lower()]
print(f"  振替関連カラム: {transfer_col}")
print(f"  勘定科目関連カラム: {account_col}")

try:
    cur.execute("""
        SELECT id, transaction_date, amount, description, `nc_opau___振替_id`, `nc_opau___freee勘定科目_id`
        FROM `nc_opau___楽天銀行ビジネス口座入出金明細`
        WHERE `nc_opau___freee勘定科目_id` = 9
          AND (`nc_opau___振替_id` IS NULL OR `nc_opau___振替_id` = '')
        ORDER BY transaction_date
    """)
    rows = cur.fetchall()
    print(f"  未リンクAmazon入金件数: {len(rows)}")
    for r in rows:
        print(f"    id={r['id']}, date={r['transaction_date']}, amount={r['amount']}, desc={r['description']}, 振替_id={r['nc_opau___振替_id']}")
except Exception as e:
    print(f"  エラー: {e}")

print()
print("=" * 70)
print("5. PayPay銀行 Amazon入金（freee勘定科目_id=9, 未リンク）")
print("=" * 70)

cur.execute("PRAGMA table_info(`nc_opau___PayPay銀行入出金明細`)")
cols = cur.fetchall()
col_names = [c['name'] for c in cols]
transfer_col = [c for c in col_names if '振替' in c or 'transfer' in c.lower()]
account_col = [c for c in col_names if '勘定' in c or 'account' in c.lower()]
print(f"  振替関連カラム: {transfer_col}")
print(f"  勘定科目関連カラム: {account_col}")

try:
    cur.execute("""
        SELECT id, transaction_date, amount, description, `nc_opau___振替_id`, `nc_opau___freee勘定科目_id`
        FROM `nc_opau___PayPay銀行入出金明細`
        WHERE `nc_opau___freee勘定科目_id` = 9
          AND (`nc_opau___振替_id` IS NULL OR `nc_opau___振替_id` = '')
        ORDER BY transaction_date
    """)
    rows = cur.fetchall()
    print(f"  未リンクAmazon入金件数: {len(rows)}")
    for r in rows:
        print(f"    id={r['id']}, date={r['transaction_date']}, amount={r['amount']}, desc={r['description']}")
except Exception as e:
    print(f"  エラー: {e}")

print()
print("=" * 70)
print("6. 代行会社 振替_id=16 の詳細")
print("=" * 70)

# Check 振替テーブル
cur.execute("PRAGMA table_info(`nc_opau___振替`)")
cols = cur.fetchall()
print("  振替テーブル カラム:")
for c in cols:
    print(f"    {c['cid']}: {c['name']} ({c['type']})")

cur.execute("SELECT * FROM `nc_opau___振替` WHERE id = 16")
row = cur.fetchone()
if row:
    print(f"  振替 id=16:")
    for key in row.keys():
        print(f"    {key}: {row[key]}")
else:
    print("  振替 id=16 は存在しない")

# Check 代行会社テーブル
print()
cur.execute("PRAGMA table_info(`nc_opau___代行会社`)")
cols = cur.fetchall()
if cols:
    print("  代行会社テーブル カラム:")
    for c in cols:
        print(f"    {c['cid']}: {c['name']} ({c['type']})")

    cur.execute("""
        SELECT * FROM `nc_opau___代行会社`
        WHERE `nc_opau___振替_id` = 16
    """)
    rows = cur.fetchall()
    print(f"  代行会社 振替_id=16 の行: {len(rows)}件")
    for r in rows:
        for key in r.keys():
            print(f"    {key}: {r[key]}")
else:
    print("  代行会社テーブルが見つからない")

conn.close()
