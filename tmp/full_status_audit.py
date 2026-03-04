"""
全テーブルリンク状態 総点検
- 振替テーブル経由リンクの完全性確認
- 未リンク・要確認エントリの洗い出し
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

SEP = '=' * 60

# ==================== 1. 振替テーブル サマリー ====================
print(f'{SEP}\n1. 振替テーブル（全107件）\n{SEP}')
cur.execute('''
SELECT COUNT(*), MIN(id), MAX(id), MIN("\u632f\u66ff\u65e5"), MAX("\u632f\u66ff\u65e5")
FROM "nc_opau___\u632f\u66ff"
''')
r = cur.fetchone()
print(f'  {r[0]}件  id={r[1]}〜{r[2]}  期間: {r[3]}〜{r[4]}')

# ==================== 2. Amazon出品アカウント ====================
print(f'\n{SEP}\n2. Amazon出品アカウント明細（DEPOSIT 80件）\n{SEP}')
cur.execute('''
SELECT
  CASE WHEN "nc_opau___\u632f\u66ff_id" IS NOT NULL THEN 'LINKED'
       WHEN "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id" IS NOT NULL THEN 'UNLINKED_ACCT'
       ELSE 'UNLINKED_NO_ACCT'
  END AS status,
  COUNT(*) AS cnt, SUM("\u91d1\u984d") AS total
FROM "nc_opau___Amazon\u51fa\u54c1\u30a2\u30ab\u30a6\u30f3\u30c8\u660e\u7d30"
WHERE entry_type = 'DEPOSIT'
GROUP BY 1
''')
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]}件 合計={r[2]:,.0f}')

# ==================== 3. 楽天銀行 ====================
print(f'\n{SEP}\n3. 楽天銀行 入出金明細（140件）\n{SEP}')

# 3a. リンク済み（正常）
cur.execute('''
SELECT COUNT(*), SUM("\u5165\u51fa\u91d1_\u5186_")
FROM "nc_opau___\u697d\u5929\u9280\u884c\u30d3\u30b8\u30cd\u30b9\u53e3\u5ea7\u5165\u51fa\u91d1\u660e\u7d30"
WHERE "nc_opau___\u632f\u66ff_id" IS NOT NULL
''')
r = cur.fetchone()
print(f'  振替リンク済み: {r[0]}件  合計={r[1]:,.0f}')

# 3b. 未リンク（勘定科目あり → 通常仕訳として処理）
cur.execute('''
SELECT id, "\u53d6\u5f15\u65e5", "\u5165\u51fa\u91d1_\u5186_", "\u5165\u51fa\u91d1\u5148\u5185\u5bb9",
       "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id"
FROM "nc_opau___\u697d\u5929\u9280\u884c\u30d3\u30b8\u30cd\u30b9\u53e3\u5ea7\u5165\u51fa\u91d1\u660e\u7d30"
WHERE "nc_opau___\u632f\u66ff_id" IS NULL
  AND "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id" IS NOT NULL
ORDER BY "\u53d6\u5f15\u65e5"
''')
rows = cur.fetchall()
print(f'\n  振替なし・勘定科目あり（通常仕訳）: {len(rows)}件')
for r in rows:
    print(f'    id={r[0]:>3} {r[1]} {r[2]:>10,} [{(r[3] or "")[:25]:<25}] acct={r[4]}')

# 3c. 完全未処理（勘定科目なし）
cur.execute('''
SELECT id, "\u53d6\u5f15\u65e5", "\u5165\u51fa\u91d1_\u5186_", "\u5165\u51fa\u91d1\u5148\u5185\u5bb9"
FROM "nc_opau___\u697d\u5929\u9280\u884c\u30d3\u30b8\u30cd\u30b9\u53e3\u5ea7\u5165\u51fa\u91d1\u660e\u7d30"
WHERE "nc_opau___\u632f\u66ff_id" IS NULL
  AND "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id" IS NULL
ORDER BY "\u53d6\u5f15\u65e5"
''')
rows = cur.fetchall()
print(f'\n  ★要対応 未分類（勘定科目なし）: {len(rows)}件')
for r in rows:
    print(f'    id={r[0]:>3} {r[1]} {r[2]:>10,} [{(r[3] or "")[:30]}]')

# ==================== 4. PayPay銀行 ====================
print(f'\n{SEP}\n4. PayPay銀行 入出金明細（112件）\n{SEP}')
cur.execute('''
SELECT COUNT(*), SUM("\u304a\u9810\u304b\u308a\u91d1\u984d")
FROM "nc_opau___PayPay\u9280\u884c\u5165\u51fa\u91d1\u660e\u7d30"
WHERE "nc_opau___\u632f\u66ff_id" IS NOT NULL
''')
r = cur.fetchone()
print(f'  振替リンク済み: {r[0]}件')

cur.execute('''
SELECT id, "\u64cd\u4f5c\u65e5", "\u304a\u9810\u304b\u308a\u91d1\u984d", "\u6458\u8981",
       "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id", "nc_opau___\u632f\u66ff_id"
FROM "nc_opau___PayPay\u9280\u884c\u5165\u51fa\u91d1\u660e\u7d30"
WHERE "nc_opau___\u632f\u66ff_id" IS NULL
  AND "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id" IS NULL
ORDER BY "\u64cd\u4f5c\u65e5"
''')
rows = cur.fetchall()
print(f'\n  ★要対応 未分類: {len(rows)}件')
for r in rows:
    print(f'    id={r[0]:>3} {r[1]} ¥{r[2]:>8,} [{(r[3] or "")[:30]}]')

# PayPay 勘定科目あり・振替なし（通常仕訳）
cur.execute('''
SELECT COUNT(*) FROM "nc_opau___PayPay\u9280\u884c\u5165\u51fa\u91d1\u660e\u7d30"
WHERE "nc_opau___\u632f\u66ff_id" IS NULL
  AND "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id" IS NOT NULL
''')
r = cur.fetchone()
print(f'\n  振替なし・勘定科目あり（通常仕訳）: {r[0]}件')

# ==================== 5. NTTカード ====================
print(f'\n{SEP}\n5. NTTファイナンスBizカード（219件）\n{SEP}')
cur.execute('''
SELECT COUNT(*) FROM "nc_opau___NTT\u30d5\u30a1\u30a4\u30ca\u30f3\u30b9Biz\u30ab\u30fc\u30c9\u660e\u7d30"
WHERE "nc_opau___\u632f\u66ff_id" IS NOT NULL
''')
r = cur.fetchone()
print(f'  振替リンク済み: {r[0]}件')

cur.execute('''
SELECT id, "\u5229\u7528\u65e5", "\u3054\u5229\u7528\u91d1\u984d", "\u3054\u5229\u7528\u52a0\u76df\u5e97",
       "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id"
FROM "nc_opau___NTT\u30d5\u30a1\u30a4\u30ca\u30f3\u30b9Biz\u30ab\u30fc\u30c9\u660e\u7d30"
WHERE "nc_opau___\u632f\u66ff_id" IS NULL
  AND "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id" IS NULL
ORDER BY "\u5229\u7528\u65e5"
''')
rows = cur.fetchall()
print(f'\n  ★要対応 未分類: {len(rows)}件')
for r in rows[:10]:
    print(f'    id={r[0]:>3} {r[1]} {r[2]:>8,} [{(r[3] or "")[:25]}] acct={r[4]}')

# ==================== 6. 代行会社 ====================
print(f'\n{SEP}\n6. 代行会社（226件）\n{SEP}')
cur.execute('''
SELECT COUNT(*), SUM("\u91d1\u984d_JPY_")
FROM "nc_opau___\u4ee3\u884c\u4f1a\u793e"
WHERE "nc_opau___\u632f\u66ff_id" IS NOT NULL
''')
r = cur.fetchone()
print(f'  振替リンク済み: {r[0]}件  合計={r[1]:,.0f}')

cur.execute('''
SELECT COUNT(*), SUM("\u91d1\u984d_JPY_")
FROM "nc_opau___\u4ee3\u884c\u4f1a\u793e"
WHERE "nc_opau___\u632f\u66ff_id" IS NULL
  AND "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id" IS NULL
''')
r = cur.fetchone()
print(f'  ★未分類: {r[0]}件  合計={r[1]:,.0f}')

conn.close()
print(f'\n{SEP}\n全テーブル監査完了\n{SEP}')
