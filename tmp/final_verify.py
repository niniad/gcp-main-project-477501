"""Final verification after all steps"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

print('=' * 60)
print('  最終検証: 会計アーキテクチャ統一')
print('=' * 60)

# 1. Balance check
q1 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) AS total_debit,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS total_credit,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) -
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS balance,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n1. 年度別 貸借バランスチェック')
print(f'  {"FY":<6} {"Debit":>12} {"Credit":>12} {"Balance":>10} {"Count":>6}')
print('  ' + '-' * 50)
all_balanced = True
for row in client.query(q1).result():
    status = '✓' if row.balance == 0 else '✗'
    print(f'  FY{row.fiscal_year:<4} {row.total_debit:>12,} {row.total_credit:>12,} {row.balance:>10,} {row.cnt:>6} {status}')
    if row.balance != 0:
        all_balanced = False
print(f'  → {"全年度バランス=0 ✓" if all_balanced else "バランス不一致あり ✗"}')

# 2. NULL account check
q2 = """
SELECT COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name IS NULL
"""
null_count = list(client.query(q2).result())[0].cnt
print(f'\n2. NULL勘定科目: {null_count}件 {"✓" if null_count == 0 else "✗"}')

# 3. Source table breakdown
q3 = """
SELECT source_table, COUNT(*) as cnt,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit_total,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit_total
FROM `main-project-477501.accounting.journal_entries`
GROUP BY source_table
ORDER BY source_table
"""
print('\n3. ソーステーブル別サマリ')
print(f'  {"Source":<25} {"Count":>6} {"Debit":>12} {"Credit":>12} {"Bal":>4}')
print('  ' + '-' * 62)
for row in client.query(q3).result():
    bal = '✓' if row.debit_total == row.credit_total else '✗'
    print(f'  {row.source_table:<25} {row.cnt:>6} {row.debit_total:>12,} {row.credit_total:>12,}  {bal}')

# 4. P/L via pl_journal_entries
q4 = """
SELECT fiscal_year, SUM(pl_contribution) AS net_income
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n4. P/L (pl_journal_entries VIEW)')
for row in client.query(q4).result():
    print(f'  FY{row.fiscal_year}: {row.net_income:+,}')

# 5. BQ table counts
q5 = """
SELECT COUNT(*) as cnt FROM `main-project-477501.nocodb.amazon_account_statements`
"""
amazon_cnt = list(client.query(q5).result())[0].cnt
q6 = """
SELECT COUNT(*) as cnt FROM `main-project-477501.nocodb.transfer_records`
"""
transfer_cnt = list(client.query(q6).result())[0].cnt
print(f'\n5. 新規テーブル確認')
print(f'  amazon_account_statements: {amazon_cnt}件')
print(f'  transfer_records: {transfer_cnt}件')

print('\n' + '=' * 60)
print('  検証完了')
print('=' * 60)
