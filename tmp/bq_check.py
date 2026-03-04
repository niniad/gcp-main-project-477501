import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# Check manual journal source_ids in FY2023
query = """
SELECT source_table, source_id, entry_side, account_name, amount_jpy
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'manual_journal' AND fiscal_year = 2023
ORDER BY source_id, entry_side
"""
print("=== FY2023 手動仕訳 ===")
for row in client.query(query).result():
    print(f"  {row.source_id} | {row.entry_side} | {row.account_name} | {row.amount_jpy:,}")

# PL total with and without manual_190
query2 = """
SELECT
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) AS pl_total,
  SUM(CASE WHEN source_id != 'manual_190'
      THEN (CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END)
      ELSE 0 END) AS pl_excl_190
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2023
AND account_name NOT IN (
  'Amazon出品アカウント','ESPRIME','THE直行便','YP','PayPay銀行',
  '事業主借','事業主貸','売掛金','未払金','楽天銀行','商品','開業費','仮払金'
)
"""
print()
print("=== PL合計 ===")
for row in client.query(query2).result():
    print(f"  PL total (with id=190):    {row.pl_total:,}")
    print(f"  PL excl id=190:            {row.pl_excl_190:,}")
    print(f"  id=190 contribution:       {row.pl_total - row.pl_excl_190:,}")
    print(f"  MF target:                 1,340,610")
    print(f"  Diff from MF:              {1340610 - row.pl_total:,}")
    needed = 1340610 - row.pl_excl_190
    print(f"  id=190 should be:          {needed:,}")
