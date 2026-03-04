import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# How does manual_journal appear in journal_entries view?
print("=== 手動仕訳 in BQ journal_entries (全年度) ===")
q1 = """
SELECT source_id, journal_date, entry_side, account_name, amount_jpy, description
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'manual_journal'
ORDER BY source_id, entry_side
"""
for row in client.query(q1).result():
    print(f"  {row.source_id} | {row.journal_date} | {row.entry_side:6s} | {row.account_name:20s} | {row.amount_jpy:>8,} | {row.description[:50] if row.description else ''}")

# Where does FY2024 売掛金=2,200 come from?
print()
print("=== FY2024 売掛金 エントリ ===")
q2 = """
SELECT source_table, source_id, journal_date, entry_side, amount_jpy, description
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '売掛金' AND fiscal_year = 2024
ORDER BY journal_date
"""
rows = list(client.query(q2).result())
if not rows:
    print("  なし")
for row in rows:
    print(f"  {row.source_table}:{row.source_id} | {row.journal_date} | {row.entry_side} | {row.amount_jpy:,}")

# Where does FY2024 売掛金=2,200 come from? Check ALL years
print()
print("=== 売掛金 全エントリ (全年度) ===")
q3 = """
SELECT source_table, source_id, journal_date, fiscal_year, entry_side, amount_jpy
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '売掛金'
ORDER BY journal_date
"""
rows = list(client.query(q3).result())
if not rows:
    print("  なし")
for row in rows:
    print(f"  FY{row.fiscal_year} | {row.source_table}:{row.source_id} | {row.journal_date} | {row.entry_side} | {row.amount_jpy:,}")
