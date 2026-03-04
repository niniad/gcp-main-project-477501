"""BQ journal_entries のアーキテクチャ確認"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# 事業主関連 の出現状況
q1 = """
SELECT account_name, entry_side, source_table, COUNT(*) cnt, SUM(amount_jpy) total
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name LIKE '%事業主%'
GROUP BY 1,2,3
ORDER BY 1,2,3
"""
print('=== 事業主関連 in journal_entries ===')
for r in client.query(q1).result():
    print(f'  {r.account_name} | {r.entry_side} | {r.source_table} | {r.cnt}件 | {r.total}円')

# 振替テーブルの参照状況（is_transfer フラグ）
q2 = """
SELECT source_table, COUNT(*) cnt
FROM `main-project-477501.accounting.journal_entries`
GROUP BY 1
ORDER BY 1
"""
print()
print('=== source_table 別件数 ===')
for r in client.query(q2).result():
    print(f'  {r.source_table}: {r.cnt}件')

# NTT: is_transfer で除外される件数確認
q3 = """
SELECT
  COUNTIF(is_transfer IS TRUE) as transfer_count,
  COUNTIF(is_transfer IS FALSE OR is_transfer IS NULL) as non_transfer_count
FROM `main-project-477501.nocodb.ntt_finance_statements`
"""
print()
print('=== NTT is_transfer フラグ ===')
for r in client.query(q3).result():
    print(f'  transfer(除外): {r.transfer_count}件, 通常: {r.non_transfer_count}件')

# 未払金の残高確認（NTTカード完済チェック）
q4 = """
SELECT
  fiscal_year,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit_total,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit_total,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) as net
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '未払金'
GROUP BY 1
ORDER BY 1
"""
print()
print('=== 未払金 年度別残高 ===')
for r in client.query(q4).result():
    print(f'  FY{r.fiscal_year}: 借方{r.debit_total:,} - 貸方{r.credit_total:,} = 残高{r.net:,}')
