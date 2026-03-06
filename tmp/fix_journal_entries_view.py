import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')
table_ref = 'main-project-477501.accounting.journal_entries'

# 現在のVIEW定義を取得
view = client.get_table(table_ref)
sql = view.view_query

print("=== 変更前の確認 ===")
for line in sql.split('\n'):
    if 'nocodb_id IN (3' in line or 'nocodb_id = 9' in line:
        print(repr(line))

# 楽天セクションの修正: (3, 5, 7, 9, 70) → (3, 5, 6, 7, 8, 9, 70)
sql_new = sql.replace(
    'ai.nocodb_id IN (3, 5, 7, 9, 70)',
    'ai.nocodb_id IN (3, 5, 6, 7, 8, 9, 70)'
)

# PayPayセクションの修正: OR ai.nocodb_id = 9 → OR ai.nocodb_id IN (5, 6, 9)
sql_new = sql_new.replace(
    'OR ai.nocodb_id = 9)',
    'OR ai.nocodb_id IN (5, 6, 9))'
)

print()
print("=== 変更後の確認 ===")
for line in sql_new.split('\n'):
    if 'nocodb_id IN (3' in line or 'nocodb_id IN (5' in line:
        print(repr(line))

# 変更が行われたか確認
if sql_new == sql:
    print("ERROR: 変更なし！パターンが一致しませんでした")
    sys.exit(1)

# BQ VIEWを更新
view.view_query = sql_new
client.update_table(view, ['view_query'])
print()
print("✅ journal_entries VIEW を更新しました")
