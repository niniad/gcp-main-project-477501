"""
1. Amazon残高の月次推移（正しい計算）
2. 2023-03の欠損入金を各テーブルで検索
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
import sqlite3

client = bigquery.Client(project='main-project-477501')

# ========== 1. Amazon残高 月次累積（CAST修正）==========
print('=== Amazon出品アカウント 月次推移（全entry_type合算） ===')
q_balance = """
WITH monthly AS (
  SELECT
    FORMAT_DATE('%Y-%m', CAST(transaction_date AS DATE)) AS ym,
    SUM(CASE WHEN entry_type IN ('REVENUE','EXPENSE','ADJUSTMENT') THEN amount
             WHEN entry_type = 'DEPOSIT' AND `振替_id` IS NOT NULL THEN amount
             ELSE 0 END) AS net_correct,
    SUM(amount) AS net_all
  FROM `main-project-477501.nocodb.amazon_account_statements`
  GROUP BY 1
)
SELECT
  ym,
  net_correct,
  net_all,
  SUM(net_correct) OVER (ORDER BY ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_correct
FROM monthly
ORDER BY ym
"""
print(f'{"年月":7} {"月次(正)":>10} {"月次(全)":>10} {"残高(正)":>12}')
for r in client.query(q_balance).result():
    diff = '' if r.net_correct == r.net_all else f' <- DEPOSIT diff={r.net_all - r.net_correct:+,.0f}'
    print(f'{r.ym}  {r.net_correct:>10,.0f}  {r.net_all:>10,.0f}  {r.balance_correct:>12,.0f}{diff}')

# ========== 2. 2023-03欠損入金の検索 ==========
print('\n\n=== 2023-03 欠損入金照合 ===')
print('対象: ¥5,664 (2023-03-15) / ¥1,912 or ¥7,576 (2023-03-29)')
print()

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 事業主借テーブル
print('--- 事業主借 2023-02〜04 ---')
cur.execute('''
SELECT id, "\u53d6\u5f15\u65e5", "\u91d1\u984d", "\u6458\u8981", "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id"
FROM "nc_opau___\u4e8b\u696d\u4e3b\u501f"
WHERE date("\u53d6\u5f15\u65e5") BETWEEN '2023-02-01' AND '2023-04-30'
ORDER BY "\u53d6\u5f15\u65e5"
''')
for r in cur.fetchall():
    print(f'  id={r[0]} {r[1]} \u00a5{r[2]:,} [{r[3] or ""}] acct={r[4]}')

# 楽天銀行 2023-02〜04（全エントリ）
print('\n--- 楽天銀行 2023-02〜04（全エントリ） ---')
cur.execute('''
SELECT id, "\u53d6\u5f15\u65e5", "\u5165\u51fa\u91d1_\u5186_", "\u5165\u51fa\u91d1\u5148\u5185\u5bb9", "nc_opau___\u632f\u66ff_id", "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id"
FROM "nc_opau___\u697d\u5929\u9280\u884c\u30d3\u30b8\u30cd\u30b9\u53e3\u5ea7\u5165\u51fa\u91d1\u660e\u7d30"
WHERE "\u53d6\u5f15\u65e5" BETWEEN '2023-02-01' AND '2023-04-30'
ORDER BY "\u53d6\u5f15\u65e5"
''')
for r in cur.fetchall():
    print(f'  id={r[0]} {r[1]} \u00a5{r[2]:,} [{r[3] or ""}] tr={r[4]} acct={r[5]}')

# NTTカード 2023-03〜04
print('\n--- NTTファイナンスBizカード 2023-02〜04 ---')
cur.execute('PRAGMA table_info("nc_opau___NTT\u30d5\u30a1\u30a4\u30ca\u30f3\u30b9Biz\u30ab\u30fc\u30c9\u660e\u7d30")')
ntt_cols = [c[1] for c in cur.fetchall()]
print(f'  カラム: {ntt_cols}')
# Find date and amount columns
date_col = next((c for c in ntt_cols if '\u5229\u7528\u65e5' in c or '\u65e5\u4ed8' in c or 'date' in c.lower()), None)
amount_col = next((c for c in ntt_cols if '\u91d1\u984d' in c or 'amount' in c.lower()), None)
desc_col = next((c for c in ntt_cols if '\u6458\u8981' in c or '\u5185\u5bb9' in c or 'desc' in c.lower()), None)
print(f'  -> date={date_col}, amount={amount_col}, desc={desc_col}')
if date_col and amount_col:
    cur.execute(f'''
    SELECT id, "{date_col}", "{amount_col}", {"\""+desc_col+"\"" if desc_col else "NULL"}
    FROM "nc_opau___NTT\u30d5\u30a1\u30a4\u30ca\u30f3\u30b9Biz\u30ab\u30fc\u30c9\u660e\u7d30"
    WHERE date("{date_col}") BETWEEN '2023-02-01' AND '2023-04-30'
    ORDER BY "{date_col}"
    ''')
    for r in cur.fetchall():
        print(f'  id={r[0]} {r[1]} \u00a5{r[2]:,} [{r[3] or ""}]')

conn.close()
