"""Compute old Amazon P/L using account_map.account_name_debug"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Old Amazon P/L: settlement_journal_payload_view -> UNNEST -> account_map -> account_items
q1 = """
WITH old_entries AS (
  SELECT
    EXTRACT(YEAR FROM s.issue_date) as fiscal_year,
    d.entry_side,
    am.account_name_debug as account_name,
    d.amount as amount_jpy
  FROM `main-project-477501.accounting.settlement_journal_payload_view` s
  CROSS JOIN UNNEST(s.json_details) d
  LEFT JOIN `main-project-477501.accounting.account_map` am ON d.account_item_id = am.account_item_id
)
SELECT e.fiscal_year, e.account_name, e.entry_side,
  SUM(e.amount_jpy) as total,
  COUNT(*) as cnt,
  SUM(CASE
    WHEN ai.small_category = '収入金額' AND e.entry_side = 'credit' THEN e.amount_jpy
    WHEN ai.small_category = '収入金額' AND e.entry_side = 'debit' THEN -e.amount_jpy
    WHEN ai.small_category IN ('経費','売上原価','製品売上原価','繰入額等') AND e.entry_side = 'debit' THEN -e.amount_jpy
    WHEN ai.small_category IN ('経費','売上原価','製品売上原価','繰入額等') AND e.entry_side = 'credit' THEN e.amount_jpy
    ELSE 0
  END) as pl_contribution
FROM old_entries e
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON e.account_name = ai.account_name
GROUP BY e.fiscal_year, e.account_name, e.entry_side
ORDER BY e.fiscal_year, e.account_name, e.entry_side
"""
print('=== Old Amazon P/L (via account_map.account_name_debug) ===')
for row in client.query(q1).result():
    acct = row.account_name or 'NULL'
    pl = row.pl_contribution or 0
    if pl != 0:
        print(f'  FY{row.fiscal_year} {acct:25} {row.entry_side:6} total={row.total:>10,} pl={pl:>+10,} ({row.cnt})')

# Summary by year
q2 = """
WITH old_entries AS (
  SELECT
    EXTRACT(YEAR FROM s.issue_date) as fiscal_year,
    d.entry_side,
    am.account_name_debug as account_name,
    d.amount as amount_jpy
  FROM `main-project-477501.accounting.settlement_journal_payload_view` s
  CROSS JOIN UNNEST(s.json_details) d
  LEFT JOIN `main-project-477501.accounting.account_map` am ON d.account_item_id = am.account_item_id
)
SELECT e.fiscal_year,
  SUM(CASE
    WHEN ai.small_category = '収入金額' AND e.entry_side = 'credit' THEN e.amount_jpy
    WHEN ai.small_category = '収入金額' AND e.entry_side = 'debit' THEN -e.amount_jpy
    WHEN ai.small_category IN ('経費','売上原価','製品売上原価','繰入額等') AND e.entry_side = 'debit' THEN -e.amount_jpy
    WHEN ai.small_category IN ('経費','売上原価','製品売上原価','繰入額等') AND e.entry_side = 'credit' THEN e.amount_jpy
    ELSE 0
  END) as old_amazon_pl
FROM old_entries e
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON e.account_name = ai.account_name
GROUP BY e.fiscal_year
ORDER BY e.fiscal_year
"""
print('\n=== Old vs New Amazon P/L by year ===')
print('  Year    Old Amazon PL    New Amazon PL')
old_pls = {}
for row in client.query(q2).result():
    old_pls[row.fiscal_year] = row.old_amazon_pl or 0
    print(f'  FY{row.fiscal_year}: old={row.old_amazon_pl or 0:>+12,}')

# New Amazon PL
q3 = """
SELECT fiscal_year, SUM(pl_contribution) as pl
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE source_table = 'amazon_settlement'
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
for row in client.query(q3).result():
    old = old_pls.get(row.fiscal_year, 0)
    new = row.pl or 0
    diff = new - old
    print(f'  FY{row.fiscal_year}: old={old:>+12,}  new={new:>+12,}  diff={diff:>+10,}')
