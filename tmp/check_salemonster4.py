import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = "C:/Users/ninni/nocodb/noco.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# 楽天銀行でセールモンスター振込検索（全テキストカラム）
print("=== 楽天銀行でセールモンスター振込検索 ===")
cur.execute("PRAGMA table_info('nc_opau___楽天銀行ビジネス口座入出金明細')")
cols = cur.fetchall()
col_names = [c['name'] for c in cols]
print(f"カラム: {col_names}")

# 全テキストカラムで検索
for col in col_names:
    for kw in ['セールモンスター', 'セルモン', 'ｾｰﾙﾓﾝ', 'SaleMonster', 'SALEMONSTER', 'salemonster', 'Sale Monster']:
        try:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE "{col}" LIKE "%{kw}%"')
            cnt = cur.fetchone()['cnt']
            if cnt > 0:
                print(f"\n  カラム '{col}' LIKE '%{kw}%': {cnt}件")
                cur.execute(f'SELECT id, * FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE "{col}" LIKE "%{kw}%" ORDER BY id')
                for r in cur.fetchall():
                    d = dict(r)
                    # メタデータ除外
                    compact = {k: v for k, v in d.items() if k not in ('created_at', 'updated_at', 'created_by', 'updated_by', 'nc_order') and v is not None and v != ''}
                    print(f"    {compact}")
        except:
            pass

# PayPay銀行
print("\n=== PayPay銀行でセールモンスター振込検索 ===")
cur.execute("PRAGMA table_info('nc_opau___PayPay銀行入出金明細')")
cols = cur.fetchall()
col_names = [c['name'] for c in cols]
for col in col_names:
    for kw in ['セールモンスター', 'セルモン', 'ｾｰﾙﾓﾝ', 'SaleMonster', 'SALEMONSTER']:
        try:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___PayPay銀行入出金明細" WHERE "{col}" LIKE "%{kw}%"')
            cnt = cur.fetchone()['cnt']
            if cnt > 0:
                print(f"\n  カラム '{col}' LIKE '%{kw}%': {cnt}件")
                cur.execute(f'SELECT id, * FROM "nc_opau___PayPay銀行入出金明細" WHERE "{col}" LIKE "%{kw}%" ORDER BY id')
                for r in cur.fetchall():
                    d = dict(r)
                    compact = {k: v for k, v in d.items() if k not in ('created_at', 'updated_at', 'created_by', 'updated_by', 'nc_order') and v is not None and v != ''}
                    print(f"    {compact}")
        except:
            pass

# 手動仕訳
print("\n=== 手動仕訳でセールモンスター検索 ===")
cur.execute("PRAGMA table_info('nc_opau___手動仕訳')")
cols = cur.fetchall()
col_names = [c['name'] for c in cols]
print(f"カラム: {col_names}")
found = False
for col in col_names:
    for kw in ['セールモンスター', 'セルモン']:
        try:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___手動仕訳" WHERE "{col}" LIKE "%{kw}%"')
            cnt = cur.fetchone()['cnt']
            if cnt > 0:
                found = True
                print(f"\n  カラム '{col}' LIKE '%{kw}%': {cnt}件")
                cur.execute(f'SELECT id, * FROM "nc_opau___手動仕訳" WHERE "{col}" LIKE "%{kw}%" ORDER BY id')
                for r in cur.fetchall():
                    d = dict(r)
                    compact = {k: v for k, v in d.items() if k not in ('created_at', 'updated_at', 'created_by', 'updated_by', 'nc_order') and v is not None and v != ''}
                    print(f"    {compact}")
        except:
            pass
if not found:
    print("  → セールモンスター関連の手動仕訳なし")

# 事業主借
print("\n=== 事業主借でセールモンスター検索 ===")
cur.execute("PRAGMA table_info('nc_opau___事業主借')")
cols = cur.fetchall()
col_names = [c['name'] for c in cols]
found = False
for col in col_names:
    for kw in ['セールモンスター', 'セルモン']:
        try:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___事業主借" WHERE "{col}" LIKE "%{kw}%"')
            cnt = cur.fetchone()['cnt']
            if cnt > 0:
                found = True
                print(f"\n  カラム '{col}' LIKE '%{kw}%': {cnt}件")
        except:
            pass
if not found:
    print("  → セールモンスター関連の事業主借エントリなし")

# Amazon出品アカウント明細でセールモンスター検索
print("\n=== Amazon出品アカウント明細でセールモンスター検索 ===")
cur.execute("PRAGMA table_info('nc_opau___Amazon出品アカウント明細')")
cols = cur.fetchall()
col_names = [c['name'] for c in cols]
found = False
for col in col_names:
    for kw in ['セールモンスター', 'セルモン', 'Sale Monster', 'SaleMonster', 'Multi-Channel']:
        try:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "nc_opau___Amazon出品アカウント明細" WHERE "{col}" LIKE "%{kw}%"')
            cnt = cur.fetchone()['cnt']
            if cnt > 0:
                found = True
                print(f"\n  カラム '{col}' LIKE '%{kw}%': {cnt}件")
                if cnt <= 10:
                    cur.execute(f'SELECT id, * FROM "nc_opau___Amazon出品アカウント明細" WHERE "{col}" LIKE "%{kw}%" ORDER BY id')
                    for r in cur.fetchall():
                        d = dict(r)
                        compact = {k: v for k, v in d.items() if k not in ('created_at', 'updated_at', 'created_by', 'updated_by', 'nc_order') and v is not None and v != ''}
                        print(f"    {compact}")
        except:
            pass
if not found:
    print("  → セールモンスター関連のAmazon出品アカウント明細エントリなし")

conn.close()
print("\n完了")
