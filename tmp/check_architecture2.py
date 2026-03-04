"""未払金残高の詳細確認"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# 未払金の source_table 別内訳
q1 = """
SELECT
  fiscal_year, source_table, entry_side,
  COUNT(*) cnt,
  SUM(amount_jpy) total
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '未払金'
GROUP BY 1,2,3
ORDER BY 1,2,3
"""
print('=== 未払金 source_table 別内訳 ===')
for r in client.query(q1).result():
    print(f'  FY{r.fiscal_year} | {r.source_table:20s} | {r.entry_side:6s} | {r.cnt:3d}件 | {r.total:>10,}円')

# 累計未払金残高
q2 = """
SELECT
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) as cumulative_net
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '未払金'
"""
print()
for r in client.query(q2).result():
    print(f'未払金 累計残高: {r.cumulative_net:,}円')

# 振替テーブルでNTT関連の銀行支払がどう処理されているか
# 楽天銀行のNTT支払（freee勘定科目_id = 70 = 未払金）
q3 = """
SELECT COUNT(*) cnt, SUM(ABS(amount_jpy)) total
FROM `main-project-477501.nocodb.rakuten_bank_statements`
WHERE `freee勘定科目_id` = 70
"""
print()
print('=== 楽天銀行 未払金(id=70) 取引 ===')
for r in client.query(q3).result():
    print(f'  {r.cnt}件, 合計: {r.total:,}円')

# 楽天銀行からの未払金支払は journal_entries でどう反映？
q4 = """
SELECT fiscal_year, entry_side, COUNT(*) cnt, SUM(amount_jpy) total
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'rakuten_bank' AND account_name = '未払金'
GROUP BY 1,2
ORDER BY 1,2
"""
print()
print('=== 楽天銀行→未払金 journal entries ===')
for r in client.query(q4).result():
    print(f'  FY{r.fiscal_year} | {r.entry_side} | {r.cnt}件 | {r.total:,}円')
