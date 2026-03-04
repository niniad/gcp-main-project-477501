"""Compare old settlement_journal_payload_view (UNNEST) vs new Amazon NocoDB data"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Old approach: UNNEST the settlement_journal_payload_view
q1 = """
SELECT
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', CAST(s.issue_date AS STRING))) as yr,
  d.entry_side, ai.account_name,
  SUM(d.amount) as total_amount,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) d
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON d.account_item_id = ai.nocodb_id
GROUP BY yr, d.entry_side, ai.account_name
ORDER BY yr, ai.account_name, d.entry_side
"""
print('=== Old settlement_journal_payload_view (UNNEST) ===')
try:
    for row in client.query(q1).result():
        acct = row.account_name or 'NULL'
        print(f'  {row.yr} {acct:25} {row.entry_side:6} total={row.total_amount:>12,} cnt={row.cnt}')
except Exception as e:
    print(f'  Error: {e}')

# Old approach P/L by year
q2 = """
SELECT
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', CAST(s.issue_date AS STRING))) as yr,
  SUM(CASE
    WHEN ai.small_category IN ('売上高','雑収入') AND d.entry_side = 'credit' THEN d.amount
    WHEN ai.small_category IN ('売上高','雑収入') AND d.entry_side = 'debit' THEN -d.amount
    WHEN ai.big_category IN ('売上原価','経費') AND d.entry_side = 'debit' THEN -d.amount
    WHEN ai.big_category IN ('売上原価','経費') AND d.entry_side = 'credit' THEN d.amount
    ELSE 0
  END) as pl_contribution
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) d
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON d.account_item_id = ai.nocodb_id
GROUP BY yr
ORDER BY yr
"""
print('\n=== Old settlement P/L by year ===')
try:
    for row in client.query(q2).result():
        pl = row.pl_contribution or 0
        print(f'  {row.yr}: {pl:+,}')
except Exception as e:
    print(f'  Error: {e}')

# New approach P/L by year (from current journal_entries)
q3 = """
SELECT fiscal_year,
  SUM(pl_contribution) as pl
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE source_table = 'amazon_settlement'
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n=== New Amazon P/L by year ===')
for row in client.query(q3).result():
    pl = row.pl or 0
    print(f'  FY{row.fiscal_year}: {pl:+,}')

# Total journal_entries old vs new for Amazon
q4 = """
SELECT
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', CAST(s.issue_date AS STRING))) as yr,
  SUM(CASE WHEN d.entry_side = 'debit' THEN d.amount ELSE 0 END) as debit_total,
  SUM(CASE WHEN d.entry_side = 'credit' THEN d.amount ELSE 0 END) as credit_total,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) d
GROUP BY yr
ORDER BY yr
"""
print('\n=== Old settlement totals by year ===')
try:
    for row in client.query(q4).result():
        print(f'  {row.yr}: debit={row.debit_total:>12,}  credit={row.credit_total:>12,}  cnt={row.cnt}')
except Exception as e:
    print(f'  Error: {e}')

# New Amazon totals
q5 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) as debit_total,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) as credit_total,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'amazon_settlement'
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('\n=== New Amazon journal totals by year ===')
for row in client.query(q5).result():
    print(f'  FY{row.fiscal_year}: debit={row.debit_total:>12,}  credit={row.credit_total:>12,}  cnt={row.cnt}')
