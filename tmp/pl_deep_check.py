"""Deep check: pl_journal_entries VIEW definition and old vs new P/L"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Get pl_journal_entries VIEW SQL
q0 = """
SELECT view_definition
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.VIEWS`
WHERE table_name = 'pl_journal_entries'
"""
print('=== pl_journal_entries VIEW SQL ===')
for row in client.query(q0).result():
    print(row.view_definition)

# Check account_items for relevant accounts
q1 = """
SELECT nocodb_id, account_name, small_category, large_category
FROM `main-project-477501.nocodb.account_items`
WHERE account_name IN (
  '売上高', '雑収入', '荷造運賃', '販売手数料', '広告宣伝費',
  '諸会費', '地代家賃', '売上値引高', '売上戻り高', '仮払金',
  'Amazon出品アカウント', '雑費', '事業主借'
)
ORDER BY nocodb_id
"""
print('\n=== Relevant account_items ===')
for row in client.query(q1).result():
    large = row.large_category or ''
    small = row.small_category or ''
    print(f'  id={row.nocodb_id:>3} {row.account_name:20} large={large:10} small={small}')
