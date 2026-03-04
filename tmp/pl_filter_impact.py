"""Check filter impact: is_transfer vs 振替_id for each table"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# For each table, compare old filter (is_transfer=FALSE) vs new filter (振替_id IS NULL)
tables = [
    ('nocodb.rakuten_bank', 'is_transfer', '振替_id'),
    ('nocodb.paypay_bank', 'is_transfer', '振替_id'),
    ('nocodb.agency_transactions', 'is_transfer', '振替_id'),
]

for table, bool_col, id_col in tables:
    q = f"""
    SELECT
      COUNTIF({bool_col} IS FALSE OR {bool_col} IS NULL) as old_filter_count,
      COUNTIF(`{id_col}` IS NULL) as new_filter_count,
      COUNTIF(({bool_col} IS FALSE OR {bool_col} IS NULL) AND `{id_col}` IS NOT NULL) as excluded_by_new,
      COUNTIF({bool_col} IS TRUE AND `{id_col}` IS NULL) as excluded_by_old,
      COUNT(*) as total
    FROM `main-project-477501.{table}`
    """
    print(f'=== {table} ===')
    for row in client.query(q).result():
        print(f'  Total rows: {row.total}')
        print(f'  Old filter (is_transfer=FALSE): {row.old_filter_count} rows pass')
        print(f'  New filter (振替_id IS NULL):    {row.new_filter_count} rows pass')
        print(f'  In old but NOT new (excluded by new): {row.excluded_by_new}')
        print(f'  In new but NOT old (excluded by old): {row.excluded_by_old}')
    print()

# Check the excluded entries - what are they?
print('=== Rakuten Bank: entries excluded by new filter ===')
q2 = """
SELECT nocodb_id, transaction_date, amount, description,
  is_transfer, `振替_id`, freee勘定科目_id,
  ai.account_name
FROM `main-project-477501.nocodb.rakuten_bank` rb
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON rb.freee勘定科目_id = ai.nocodb_id
WHERE (rb.is_transfer IS FALSE OR rb.is_transfer IS NULL)
  AND rb.`振替_id` IS NOT NULL
ORDER BY rb.transaction_date
"""
for row in client.query(q2).result():
    print(f'  id={row.nocodb_id} {row.transaction_date} {row.amount:>10,} {row.account_name} 振替_id={getattr(row, "振替_id", "?")} desc={row.description[:40]}')

print('\n=== Agency: entries excluded by new filter ===')
q3 = """
SELECT nocodb_id, transaction_date, amount, description,
  is_transfer, `振替_id`, freee勘定科目_id,
  ai.account_name
FROM `main-project-477501.nocodb.agency_transactions` at2
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON at2.freee勘定科目_id = ai.nocodb_id
WHERE (at2.is_transfer IS FALSE OR at2.is_transfer IS NULL)
  AND at2.`振替_id` IS NOT NULL
ORDER BY at2.transaction_date
"""
for row in client.query(q3).result():
    acct = row.account_name or 'NULL'
    desc = (row.description or '')[:40]
    print(f'  id={row.nocodb_id} {row.transaction_date} {row.amount:>10,} {acct} 振替_id={getattr(row, "振替_id", "?")} desc={desc}')

print('\n=== PayPay: entries excluded by new filter ===')
q4 = """
SELECT nocodb_id, transaction_date, amount, description,
  is_transfer, `振替_id`, freee勘定科目_id,
  ai.account_name
FROM `main-project-477501.nocodb.paypay_bank` pb
LEFT JOIN `main-project-477501.nocodb.account_items` ai ON pb.freee勘定科目_id = ai.nocodb_id
WHERE (pb.is_transfer IS FALSE OR pb.is_transfer IS NULL)
  AND pb.`振替_id` IS NOT NULL
ORDER BY pb.transaction_date
"""
for row in client.query(q4).result():
    acct = row.account_name or 'NULL'
    desc = (row.description or '')[:40]
    print(f'  id={row.nocodb_id} {row.transaction_date} {row.amount:>10,} {acct} 振替_id={getattr(row, "振替_id", "?")} desc={desc}')
