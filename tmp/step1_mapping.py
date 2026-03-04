"""Get freee → NocoDB account ID mapping for Amazon entries"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Get freee → nocodb mapping for all Amazon accounts
q = """
SELECT am.account_item_id as freee_id, am.account_name_debug,
  ai.nocodb_id as nocodb_account_id
FROM (
  SELECT DISTINCT account_item_id, account_name_debug
  FROM `main-project-477501.accounting.account_map`
) am
LEFT JOIN `main-project-477501.nocodb.account_items` ai
  ON am.account_name_debug = ai.account_name
WHERE am.account_item_id IN (786598267, 786598298, 1008403397, 786598349, 786598290,
  786598269, 786598329, 786598354, 786598270, 786598277, 786598216, 786598332)
ORDER BY am.account_name_debug
"""
print('=== freee → nocodb account mapping ===')
for row in client.query(q).result():
    print(f'freee={row.freee_id} | {row.account_name_debug:15} | nocodb={row.nocodb_account_id}')

# Also check all NocoDB accounts to find missing ones
q2 = """
SELECT nocodb_id, account_name
FROM `main-project-477501.nocodb.account_items`
WHERE account_name IN ('売上高','広告宣伝費','Amazon出品アカウント','販売手数料','荷造運賃',
  '売上値引高','地代家賃','諸会費','売上戻り高','雑収入','仮払金','支払手数料',
  'FBA配送費','広告費','保管費','プロモーション費')
ORDER BY account_name
"""
print()
print('=== NocoDB account_items (all relevant) ===')
for row in client.query(q2).result():
    print(f'nocodb_id={row.nocodb_id}: {row.account_name}')
