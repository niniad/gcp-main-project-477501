"""Final verification after all changes"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

print('=' * 60)
print('  最終検証: NocoDB会計アーキテクチャ統一')
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
print('\n1. 貸借バランスチェック')
all_balanced = True
for row in client.query(q1).result():
    status = 'OK' if row.balance == 0 else 'NG'
    print(f'  FY{row.fiscal_year}: debit={row.total_debit:>12,} credit={row.total_credit:>12,} bal={row.balance:>6} [{status}]')
    if row.balance != 0:
        all_balanced = False
print(f'  -> {"全年度バランス=0" if all_balanced else "バランス不一致あり"}')

# 2. NULL account check
q2 = """
SELECT COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name IS NULL
"""
null_count = list(client.query(q2).result())[0].cnt
print(f'\n2. NULL勘定科目: {null_count}件 {"OK" if null_count == 0 else "NG"}')

# 3. P/L check
q3 = """
SELECT fiscal_year, COALESCE(SUM(pl_contribution), 0) as net_income
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n3. P/L (損益計算)')
expected = {2023: -1340610, 2024: -1088882}
for row in client.query(q3).result():
    ni = int(row.net_income)
    exp = expected.get(row.fiscal_year)
    if exp is not None:
        status = 'MF一致' if ni == exp else f'MF差{ni - exp:+,}'
        print(f'  FY{row.fiscal_year}: {ni:>+12,} (MF: {exp:>+12,}) [{status}]')
    else:
        print(f'  FY{row.fiscal_year}: {ni:>+12,}')

# 4. Source table summary
q4 = """
SELECT source_table, COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
GROUP BY source_table
ORDER BY source_table
"""
print('\n4. ソーステーブル別件数')
total = 0
for row in client.query(q4).result():
    print(f'  {row.source_table:25} {row.cnt:>6}件')
    total += row.cnt
print(f'  {"合計":<25} {total:>6}件')

# 5. Key tables
q5 = """
SELECT 'amazon_account_statements' as tbl, COUNT(*) as cnt FROM `main-project-477501.nocodb.amazon_account_statements`
UNION ALL
SELECT 'transfer_records', COUNT(*) FROM `main-project-477501.nocodb.transfer_records`
"""
print('\n5. 新規テーブル確認')
for row in client.query(q5).result():
    print(f'  {row.tbl}: {row.cnt}件')

print('\n' + '=' * 60)
print('  検証完了')
print('=' * 60)
