import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# Check if 売掛金 still exists
q1 = """
SELECT account_name, COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '売掛金' AND fiscal_year = 2023
GROUP BY account_name
"""
print("=== 売掛金 FY2023 ===")
rows = list(client.query(q1).result())
if not rows:
    print("  売掛金エントリなし (修正反映済み)")
else:
    for row in rows:
        print(f"  {row.account_name}: {row.cnt} 件")

# PL impact check - id=164 was Dr.売掛金(BS)/Cr.未払金(BS), now Dr.Amazon出品アカウント(BS)/Cr.未払金(BS)
# Both sides are BS, so NO PL impact. id=190 should NOT need recalculation.
q2 = """
SELECT
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) AS pl_total
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2023
AND account_name NOT IN (
  'Amazon出品アカウント','ESPRIME','THE直行便','YP','PayPay銀行',
  '事業主借','事業主貸','売掛金','未払金','楽天銀行','商品','開業費','仮払金'
)
"""
print()
for row in client.query(q2).result():
    print(f"PL合計: {row.pl_total:,} (MF target: 1,340,610)")
    print(f"差異: {1340610 - row.pl_total:,}")

# BS check
q3 = """
SELECT account_name,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year <= 2023
AND account_name IN ('売掛金','Amazon出品アカウント','未払金')
GROUP BY account_name
ORDER BY account_name
"""
print()
print("=== FY2023末 BS (影響科目) ===")
for row in client.query(q3).result():
    print(f"  {row.account_name:25s} {row.balance:>12,}")

# FY2024
q4 = """
SELECT account_name,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year <= 2024
AND account_name IN ('売掛金','Amazon出品アカウント','未払金')
GROUP BY account_name
ORDER BY account_name
"""
print()
print("=== FY2024末 BS (影響科目) ===")
for row in client.query(q4).result():
    print(f"  {row.account_name:25s} {row.balance:>12,}")
