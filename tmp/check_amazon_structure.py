"""
Amazon出品アカウント残高構造の確認
1. 負値精算のDEPOSIT期間にREVENUE/EXPENSEが重複しているか
2. 2023-03の欠損入金を事業主借・NTT等で検索
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
import sqlite3

client = bigquery.Client(project='main-project-477501')

# ========== 1. 負値精算期間の構造確認 ==========
print('=== settlement 11435905203 の全エントリ（DEPOSIT -36,318の期間） ===')
q1 = """
SELECT nocodb_id, transaction_date, entry_type, amount, description,
       `振替_id`, `freee勘定科目_id`
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE description LIKE '%11435905203%'
ORDER BY amount DESC
"""
rows1 = list(client.query(q1).result())
for r in rows1:
    print(f'  id={r.nocodb_id} {r.transaction_date} {r.entry_type:8} {r.amount:>10,.0f}  {(r.description or "")[:60]}')

# 同じ期間（2023-06-22〜2023-07-05）の全entry_type集計
print('\n=== 2023-06-22〜2023-07-05 entry_type別集計 ===')
q2 = """
SELECT entry_type, COUNT(*) AS cnt, SUM(amount) AS total
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE transaction_date BETWEEN '2023-06-22' AND '2023-07-05'
GROUP BY entry_type
ORDER BY entry_type
"""
for r in client.query(q2).result():
    print(f'  {r.entry_type:10} {r.cnt}件 合計={r.total:,.0f}')

# Amazon残高の累積推移（月次）
print('\n=== Amazon出品アカウント 月次累積残高 ===')
q3 = """
SELECT
  FORMAT_DATE('%Y-%m', transaction_date) AS ym,
  SUM(amount) AS monthly_net,
  SUM(SUM(amount)) OVER (ORDER BY FORMAT_DATE('%Y-%m', transaction_date)) AS cumulative_balance
FROM `main-project-477501.nocodb.amazon_account_statements`
GROUP BY 1
ORDER BY 1
"""
for r in client.query(q3).result():
    print(f'  {r.ym}  月次={r.monthly_net:>8,.0f}  残高={r.cumulative_balance:>10,.0f}')

# ========== 2. 2023-03欠損入金の照合 ==========
print('\n\n=== 2023-03 欠損入金照合 ===')
print('対象: ¥5,664 (2023-03-15) / ¥1,912 (2023-03-29) / ¥7,576 (2023-03-29)')
print()

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 事業主借テーブル（2023-03〜04）
print('--- 事業主借 2023-03〜04 ---')
cur.execute('''
SELECT id, "\u53d6\u5f15\u65e5", "\u91d1\u984d", "\u6458\u8981", "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id"
FROM "nc_opau___\u4e8b\u696d\u4e3b\u501f"
WHERE date("\u53d6\u5f15\u65e5") BETWEEN '2023-02-01' AND '2023-04-30'
ORDER BY "\u53d6\u5f15\u65e5"
''')
for r in cur.fetchall():
    print(f'  id={r[0]} {r[1]} \u00a5{r[2]:,} [{r[3] or ""}] acct={r[4]}')

# NTTカード（2023-03〜04）
print('\n--- NTTファイナンスBizカード 2023-03〜04 ---')
cur.execute('PRAGMA table_info("nc_opau___NTT\u30d5\u30a1\u30a4\u30ca\u30f3\u30b9Biz\u30ab\u30fc\u30c9\u660e\u7d30")')
ntt_cols = [c[1] for c in cur.fetchall()]
# Find date and amount columns
date_col = next((c for c in ntt_cols if '\u65e5' in c or 'date' in c.lower()), None)
amount_col = next((c for c in ntt_cols if '\u91d1\u984d' in c or 'amount' in c.lower()), None)
print(f'  日付列: {date_col}, 金額列: {amount_col}')
if date_col and amount_col:
    cur.execute(f'''
    SELECT id, "{date_col}", "{amount_col}"
    FROM "nc_opau___NTT\u30d5\u30a1\u30a4\u30ca\u30f3\u30b9Biz\u30ab\u30fc\u30c9\u660e\u7d30"
    WHERE date("{date_col}") BETWEEN '2023-02-01' AND '2023-04-30'
    ORDER BY "{date_col}"
    LIMIT 10
    ''')
    for r in cur.fetchall():
        print(f'  id={r[0]} {r[1]} \u00a5{r[2]:,}')

# 楽天銀行 2023-03〜04（全エントリ：正負問わず）
print('\n--- 楽天銀行 2023-03〜04（全エントリ） ---')
cur.execute('''
SELECT id, "\u53d6\u5f15\u65e5", "\u5165\u51fa\u91d1_\u5186_", "\u5165\u51fa\u91d1\u5148\u5185\u5bb9", "nc_opau___\u632f\u66ff_id", "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id"
FROM "nc_opau___\u697d\u5929\u9280\u884c\u30d3\u30b8\u30cd\u30b9\u53e3\u5ea7\u5165\u51fa\u91d1\u660e\u7d30"
WHERE "\u53d6\u5f15\u65e5" BETWEEN '2023-02-01' AND '2023-04-30'
ORDER BY "\u53d6\u5f15\u65e5"
''')
for r in cur.fetchall():
    print(f'  id={r[0]} {r[1]} \u00a5{r[2]:,} [{r[3] or ""}] tr={r[4]} acct={r[5]}')

conn.close()
