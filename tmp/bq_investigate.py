import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# 1. Check the specific entry: Dr.売掛金 / Cr.未払金 36,318 on 2023-07-03
print("=== 2023-07-03 売掛金 36,318 の仕訳 ===")
q1 = """
SELECT source_table, source_id, journal_date, entry_side, account_name, amount_jpy, description
FROM `main-project-477501.accounting.journal_entries`
WHERE amount_jpy = 36318 AND fiscal_year = 2023
ORDER BY journal_date, entry_side
"""
for row in client.query(q1).result():
    print(f"  {row.journal_date} | {row.source_table}:{row.source_id} | {row.entry_side} | {row.account_name} | {row.amount_jpy:,} | {row.description}")

# 2. Check what accounts are used in Amazon settlement entries
print()
print("=== Amazon精算の勘定科目一覧 ===")
q2 = """
SELECT DISTINCT account_name, entry_side, COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'amazon_settlement' AND fiscal_year = 2023
GROUP BY account_name, entry_side
ORDER BY account_name, entry_side
"""
for row in client.query(q2).result():
    print(f"  {row.account_name:25s} | {row.entry_side:6s} | {row.cnt} 件")

# 3. Check 売掛金 vs Amazon出品アカウント usage
print()
print("=== 売掛金 の全エントリ (FY2023) ===")
q3 = """
SELECT source_table, source_id, journal_date, entry_side, amount_jpy, description
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '売掛金' AND fiscal_year = 2023
ORDER BY journal_date
"""
for row in client.query(q3).result():
    print(f"  {row.journal_date} | {row.source_table}:{row.source_id} | {row.entry_side} | {row.amount_jpy:,} | {row.description[:50] if row.description else ''}")

# 4. Check inventory/COGS related entries
print()
print("=== 棚卸・仕入関連仕訳 (FY2023) ===")
q4 = """
SELECT source_table, source_id, journal_date, entry_side, account_name, amount_jpy, description
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name IN ('商品', '仕入高') AND fiscal_year = 2023
AND source_table = 'inventory_adjustment'
ORDER BY journal_date
"""
for row in client.query(q4).result():
    print(f"  {row.journal_date} | {row.source_table}:{row.source_id} | {row.entry_side} | {row.account_name} | {row.amount_jpy:,}")

# 5. Check FY2025 inventory entries
print()
print("=== 棚卸仕訳 (FY2025) ===")
q5 = """
SELECT source_table, source_id, journal_date, entry_side, account_name, amount_jpy, description
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name IN ('商品', '仕入高') AND fiscal_year = 2025
AND source_table = 'inventory_adjustment'
ORDER BY journal_date, entry_side
"""
for row in client.query(q5).result():
    print(f"  {row.journal_date} | {row.source_id} | {row.entry_side} | {row.account_name} | {row.amount_jpy:,}")
