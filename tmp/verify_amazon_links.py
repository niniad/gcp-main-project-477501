"""Verify Amazon table links work in NocoDB API"""
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

def api_call(method, path):
    req = urllib.request.Request(f'{BASE_URL}{path}', method=method)
    req.add_header('xc-token', TOKEN)
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode('utf-8'))

# Get some records with transfer links
records = api_call('GET', f'/tables/{TABLE_ID}/records?where=(entry_type,eq,DEPOSIT)&limit=5')
print(f'DEPOSIT records (should have 振替 links):')
for rec in records.get('list', [])[:5]:
    print(f'  id={rec.get("Id")}, date={rec.get("取引日")}, amount={rec.get("金額")}, '
          f'振替={rec.get("振替")}, freee勘定科目={rec.get("freee勘定科目")}')

# Get non-deposit records
records2 = api_call('GET', f'/tables/{TABLE_ID}/records?where=(entry_type,eq,EXPENSE)&limit=5')
print(f'\nEXPENSE records (should have freee勘定科目 links):')
for rec in records2.get('list', [])[:5]:
    print(f'  id={rec.get("Id")}, date={rec.get("取引日")}, amount={rec.get("金額")}, '
          f'freee勘定科目={rec.get("freee勘定科目")}')

total = records.get('pageInfo', {}).get('totalRows', '?')
print(f'\nTotal DEPOSIT rows: {total}')
