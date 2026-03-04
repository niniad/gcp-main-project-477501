"""Step 1: Amazon settlement journal data analysis"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Get settlement_journal_view for the flat detail rows
q = """
SELECT
  s.settlement_id,
  DATE(s.issue_date) as deposit_date,
  d.entry_side,
  d.account_item_id,
  COALESCE(am.account_name_debug, CAST(d.account_item_id AS STRING)) AS account_name,
  d.amount,
  d.tax_code,
  d.description
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) AS d
LEFT JOIN (
  SELECT DISTINCT account_item_id, account_name_debug
  FROM `main-project-477501.accounting.account_map`
) am ON d.account_item_id = am.account_item_id
ORDER BY deposit_date, s.settlement_id, d.entry_side
"""
rows = list(client.query(q).result())

print(f'=== Amazon journal entries: {len(rows)} total ===')
for row in rows[:20]:
    print(f'{row.deposit_date} | {row.settlement_id} | {row.entry_side:6} | {row.account_name:20} | {row.amount:>10,} | {row.description}')

# Totals
total_debit = sum(r.amount for r in rows if r.entry_side == 'debit')
total_credit = sum(r.amount for r in rows if r.entry_side == 'credit')
print(f'\nTotal debit: {total_debit:,}, Total credit: {total_credit:,}')

# Distinct account names with nocodb mapping
q2 = """
SELECT DISTINCT
  COALESCE(am.account_name_debug, CAST(d.account_item_id AS STRING)) AS account_name,
  d.account_item_id as freee_account_id,
  COUNT(*) as cnt
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) AS d
LEFT JOIN (
  SELECT DISTINCT account_item_id, account_name_debug
  FROM `main-project-477501.accounting.account_map`
) am ON d.account_item_id = am.account_item_id
GROUP BY 1, 2
ORDER BY cnt DESC
"""
print('\n=== Distinct account names ===')
for row in client.query(q2).result():
    print(f'{row.account_name:25} (freee_id={row.freee_account_id}): {row.cnt} entries')

# Check NocoDB account_items mapping
q3 = """
SELECT nocodb_id, account_name
FROM `main-project-477501.nocodb.account_items`
WHERE account_name IN ('売上高', '販売手数料', 'FBA配送費', '広告費', '荷造運賃', '保管費', '雑費', '雑収入', '売掛金', 'Amazon出品アカウント')
ORDER BY account_name
"""
print('\n=== NocoDB account_items (relevant) ===')
for row in client.query(q3).result():
    print(f'nocodb_id={row.nocodb_id}: {row.account_name}')

# Check account_map for freee_id → nocodb_id mapping
q4 = """
SELECT account_item_id as freee_id, account_name_debug, nocodb_account_item_id
FROM `main-project-477501.accounting.account_map`
WHERE account_name_debug IN ('売上高', '販売手数料', 'FBA配送費', '広告費', '荷造運賃', '保管費', '雑費', '雑収入', '売掛金', 'Amazon出品アカウント', 'プロモーション費')
ORDER BY account_name_debug
"""
print('\n=== account_map (freee → nocodb) ===')
for row in client.query(q4).result():
    print(f'freee_id={row.freee_id}: {row.account_name_debug} → nocodb_id={row.nocodb_account_item_id}')

# Settlement count
q5 = """
SELECT COUNT(DISTINCT settlement_id) as settlement_cnt
FROM `main-project-477501.accounting.settlement_journal_payload_view`
"""
for row in client.query(q5).result():
    print(f'\nTotal settlements: {row.settlement_cnt}')
