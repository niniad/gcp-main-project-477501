"""Create link columns for Amazon table via NocoDB API (proper way)"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import json
import urllib.request
from google.cloud import secretmanager

sm_client = secretmanager.SecretManagerServiceClient()
resp = sm_client.access_secret_version(name='projects/main-project-477501/secrets/NOCODB_API_TOKEN/versions/latest')
TOKEN = resp.payload.data.decode('utf-8').strip()

BASE_URL = 'http://localhost:8080/api/v2'
AMAZON_TABLE_ID = 'mwaoi5cfvolp1fu'

# Target table IDs
FURIKAE_TABLE_ID = 'm6qm3ca7r4deu4y'
ACCOUNT_TABLE_ID = 'mvvmdn559d8sejw'

def api_call(method, path, data=None):
    url = f'{BASE_URL}{path}'
    body = json.dumps(data).encode('utf-8') if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header('xc-token', TOKEN)
    req.add_header('Content-Type', 'application/json')
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        err_body = e.read().decode('utf-8', errors='replace')
        print(f'  HTTP {e.code}: {err_body[:300]}')
        return None

# Create BelongsTo link to 振替
print('=== Create link to 振替 ===')
link1 = {
    "title": "振替",
    "uidt": "LinkToAnotherRecord",
    "parentId": FURIKAE_TABLE_ID,
    "childId": AMAZON_TABLE_ID,
    "type": "bt"
}
result1 = api_call('POST', f'/meta/tables/{AMAZON_TABLE_ID}/columns', link1)
if result1:
    print(f'  Success: {json.dumps(result1, ensure_ascii=False)[:200]}')
else:
    print('  Failed!')

# Create BelongsTo link to freee勘定科目
print('\n=== Create link to freee勘定科目 ===')
link2 = {
    "title": "freee勘定科目",
    "uidt": "LinkToAnotherRecord",
    "parentId": ACCOUNT_TABLE_ID,
    "childId": AMAZON_TABLE_ID,
    "type": "bt"
}
result2 = api_call('POST', f'/meta/tables/{AMAZON_TABLE_ID}/columns', link2)
if result2:
    print(f'  Success: {json.dumps(result2, ensure_ascii=False)[:200]}')
else:
    print('  Failed!')

# Verify columns
print('\n=== Verify table columns ===')
table_info = api_call('GET', f'/meta/tables/{AMAZON_TABLE_ID}')
if table_info:
    for col in table_info['columns']:
        extra = ''
        if col['uidt'] in ('LinkToAnotherRecord', 'Links', 'ForeignKey'):
            extra = f' [{col["uidt"]}]'
        if extra:
            print(f'  {col["title"]:40} {extra}')

# Test record access
print('\n=== Test record access ===')
records = api_call('GET', f'/tables/{AMAZON_TABLE_ID}/records?limit=3')
if records:
    total = records.get('pageInfo', {}).get('totalRows', '?')
    print(f'  Total: {total} rows')
    for rec in records.get('list', [])[:3]:
        print(f'  id={rec.get("Id")}, amount={rec.get("金額")}, '
              f'振替={rec.get("振替")}, freee勘定科目={rec.get("freee勘定科目")}')
