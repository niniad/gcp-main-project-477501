"""Step 3 P/L verification: Compare actual income/expense between old and new VIEW"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# P/L accounts only (exclude BS accounts)
BS_ACCOUNTS = (
    '楽天銀行', 'PayPay銀行', 'Amazon出品アカウント', '未払金', '売掛金',
    '事業主借', '棚卸資産', '預け金', 'ESPRIME', 'YP', 'THE直行便', '開業費',
    '仮払金'
)

bs_list = ', '.join(f"'{a}'" for a in BS_ACCOUNTS)

q = f"""
SELECT fiscal_year,
  -- Revenue
  SUM(CASE WHEN account_name IN ('売上高','雑収入') AND entry_side='credit' THEN amount_jpy
            WHEN account_name IN ('売上高','雑収入') AND entry_side='debit' THEN -amount_jpy
            ELSE 0 END) AS revenue,
  -- Expenses (all P/L accounts except revenue)
  SUM(CASE WHEN account_name NOT IN ('売上高','雑収入', {bs_list})
            AND entry_side='debit' THEN amount_jpy
            WHEN account_name NOT IN ('売上高','雑収入', {bs_list})
            AND entry_side='credit' THEN -amount_jpy
            ELSE 0 END) AS expense
FROM `main-project-477501.accounting.journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""

print('=== P/L (income - expense) ===')
print(f'{"FY":<6} {"Revenue":>12} {"Expense":>12} {"Profit":>12}')
print('-' * 44)
for row in client.query(q).result():
    profit = row.revenue - row.expense
    print(f'FY{row.fiscal_year:<4} {row.revenue:>12,} {row.expense:>12,} {profit:>12,}')

# Detailed account breakdown by year for FY2023 (reference year)
print()
print('=== FY2023 account breakdown (P/L) ===')
q2 = f"""
SELECT account_name,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE 0 END) as debit,
  SUM(CASE WHEN entry_side='credit' THEN amount_jpy ELSE 0 END) as credit
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2023
  AND account_name NOT IN ({bs_list})
GROUP BY account_name
ORDER BY debit + credit DESC
"""
for row in client.query(q2).result():
    net = row.credit - row.debit
    print(f'  {row.account_name:20} debit={row.debit:>10,} credit={row.credit:>10,} net={net:>+10,}')

# Check for NULL account names
q3 = """
SELECT source_table, COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name IS NULL
GROUP BY source_table
"""
print()
print('=== NULL account entries ===')
null_count = 0
for row in client.query(q3).result():
    print(f'  {row.source_table}: {row.cnt}')
    null_count += row.cnt
if null_count == 0:
    print('  なし ✓')
