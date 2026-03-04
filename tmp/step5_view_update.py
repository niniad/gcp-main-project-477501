"""Step 5: Update journal_entries VIEW - 事業主借の貸方を固定値に変更"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Get current VIEW SQL
table = client.get_table('main-project-477501.accounting.journal_entries')
current_sql = table.view_query

# Replace the owner_contribution credit section
# Old: JOIN on credit_account_id to get account name
# New: Fixed '事業主借' string
old_credit_section = """-- ⑩ 事業主借（貸方側）
SELECT
  CONCAT('oc_', CAST(oc.nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', oc.journal_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', oc.journal_date)),
  'credit',
  ai_cr.account_name,
  oc.amount,
  NULL,
  oc.description,
  'owner_contribution'
FROM `main-project-477501.nocodb.owner_contribution_entries` oc
LEFT JOIN `main-project-477501.nocodb.account_items` ai_cr ON oc.credit_account_id = ai_cr.nocodb_id
WHERE oc.journal_date IS NOT NULL AND oc.amount IS NOT NULL"""

new_credit_section = """-- ⑩ 事業主借（貸方側 - 常に '事業主借'）
SELECT
  CONCAT('oc_', CAST(oc.nocodb_id AS STRING)),
  SAFE.PARSE_DATE('%Y-%m-%d', oc.journal_date),
  EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', oc.journal_date)),
  'credit',
  '事業主借' AS account_name,
  oc.amount,
  NULL,
  oc.description,
  'owner_contribution'
FROM `main-project-477501.nocodb.owner_contribution_entries` oc
WHERE oc.journal_date IS NOT NULL AND oc.amount IS NOT NULL"""

if old_credit_section in current_sql:
    new_sql = current_sql.replace(old_credit_section, new_credit_section)
    print('Found and replaced 事業主借 credit section')
else:
    print('ERROR: Could not find exact match for old credit section')
    print('Attempting to find similar pattern...')
    # Show what's around ⑩ 事業主借（貸方側
    import re
    match = re.search(r'⑩ 事業主借（貸方側.*?owner_contribution', current_sql, re.DOTALL)
    if match:
        print(f'Found: {match.group()[:200]}')
    sys.exit(1)

# Update VIEW
ddl = f"CREATE OR REPLACE VIEW `main-project-477501.accounting.journal_entries` AS\n{new_sql}"
job = client.query(ddl)
job.result()
print('VIEW updated successfully')

# Verify - check for NULL accounts
q = """
SELECT source_table, COUNT(*) as null_count
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name IS NULL
GROUP BY source_table
"""
null_count = 0
for row in client.query(q).result():
    print(f'  NULL accounts in {row.source_table}: {row.null_count}')
    null_count += row.null_count

if null_count == 0:
    print('  No NULL accounts ✓')

# P/L check
q2 = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) -
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS balance
FROM `main-project-477501.accounting.journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print()
print('Balance check:')
for row in client.query(q2).result():
    print(f'  FY{row.fiscal_year}: balance={row.balance:,}')
