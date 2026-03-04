"""Check Amazon table columns via NocoDB API"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import json
import urllib.request
from google.cloud import secretmanager

sm_client = secretmanager.SecretManagerServiceClient()
resp = sm_client.access_secret_version(name='projects/main-project-477501/secrets/NOCODB_API_TOKEN/versions/latest')
TOKEN = resp.payload.data.decode('utf-8').strip()

BASE_URL = 'http://localhost:8080/api/v2'
TABLE_ID = 'mwaoi5cfvolp1fu'

req = urllib.request.Request(f'{BASE_URL}/meta/tables/{TABLE_ID}')
req.add_header('xc-token', TOKEN)
with urllib.request.urlopen(req) as r:
    data = json.loads(r.read().decode('utf-8'))

print(f'Table: {data["title"]}')
print(f'Columns ({len(data["columns"])}):')
for col in data['columns']:
    extra = ''
    if col['uidt'] == 'LinkToAnotherRecord':
        extra = f' -> {col.get("colOptions", {}).get("type", "?")}'
    elif col['uidt'] == 'ForeignKey':
        extra = ' (FK)'
    print(f'  {col["title"]:35} {col["uidt"]:25} {extra}')

# Check a few records
req2 = urllib.request.Request(f'{BASE_URL}/tables/{TABLE_ID}/records?limit=3')
req2.add_header('xc-token', TOKEN)
with urllib.request.urlopen(req2) as r:
    records = json.loads(r.read().decode('utf-8'))

print(f'\nSample records ({records.get("pageInfo", {}).get("totalRows", "?")} total):')
for rec in records.get('list', [])[:3]:
    print(f'  id={rec.get("Id")}, date={rec.get("取引日")}, amount={rec.get("金額")}, '
          f'振替={rec.get("振替")}, freee勘定科目={rec.get("freee勘定科目")}')
