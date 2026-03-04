"""Check filter impact: is_transfer vs 振替_id for each table"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# For each table, compare old filter (is_transfer=FALSE) vs new filter (振替_id IS NULL)
tables = [
    ('nocodb.rakuten_bank_statements', 'is_transfer'),
    ('nocodb.paypay_bank_statements', 'is_transfer'),
    ('nocodb.agency_transactions', 'is_transfer'),
]

for table, bool_col in tables:
    q = f"""
    SELECT
      COUNTIF({bool_col} IS FALSE OR {bool_col} IS NULL) as old_includes,
      COUNTIF(`振替_id` IS NULL) as new_includes,
      COUNTIF(({bool_col} IS FALSE OR {bool_col} IS NULL) AND `振替_id` IS NOT NULL) as lost_in_new,
      COUNTIF({bool_col} IS TRUE AND `振替_id` IS NULL) as gained_in_new,
      COUNT(*) as total
    FROM `main-project-477501.{table}`
    """
    print(f'=== {table} ===')
    for row in client.query(q).result():
        print(f'  Total rows: {row.total}')
        print(f'  Old filter (is_transfer=FALSE): {row.old_includes} rows included')
        print(f'  New filter (振替_id IS NULL):    {row.new_includes} rows included')
        print(f'  LOST (old=yes, new=no): {row.lost_in_new}')
        print(f'  GAINED (old=no, new=yes): {row.gained_in_new}')
    print()

# Check the LOST entries for each table
for table, bool_col in tables:
    q = f"""
    SELECT nocodb_id, transaction_date, amount, description,
      {bool_col} as is_transfer_val, `振替_id`,
      freee勘定科目_id
    FROM `main-project-477501.{table}`
    WHERE ({bool_col} IS FALSE OR {bool_col} IS NULL)
      AND `振替_id` IS NOT NULL
    ORDER BY transaction_date
    LIMIT 20
    """
    print(f'=== LOST entries: {table} ===')
    try:
        rows = list(client.query(q).result())
        if not rows:
            print('  None')
        for row in rows:
            desc = (row.description or '')[:50]
            print(f'  id={row.nocodb_id} {row.transaction_date} {row.amount:>10,} acct={row.freee勘定科目_id} 振替={getattr(row, "振替_id", "?")} {desc}')
    except Exception as e:
        print(f'  Error: {e}')
    print()
