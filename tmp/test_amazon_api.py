"""Test Amazon table records via NocoDB API"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import json
import urllib.request
from google.cloud import secretmanager

sm_client = secretmanager.SecretManagerServiceClient()
resp = sm_client.access_secret_version(name='projects/main-project-477501/secrets/NOCODB_API_TOKEN/versions/latest')
TOKEN = resp.payload.data.decode('utf-8').strip()

TABLE_ID = 'mwaoi5cfvolp1fu'

url = f'http://localhost:8080/api/v2/tables/{TABLE_ID}/records?limit=5'
req = urllib.request.Request(url)
req.add_header('xc-token', TOKEN)

try:
    with urllib.request.urlopen(req) as r:
        data = json.loads(r.read().decode('utf-8'))
    
    total = data.get('pageInfo', {}).get('totalRows', '?')
    print(f'Total rows: {total}')
    print(f'\nSample records:')
    for rec in data.get('list', [])[:5]:
        print(f'  id={rec.get("Id")}, date={rec.get("取引日")}, amount={rec.get("金額")}, '
              f'type={rec.get("entry_type")}, '
              f'振替={rec.get("振替")}, freee勘定科目={rec.get("freee勘定科目")}')
    print('\nAPI access OK!')
except urllib.error.HTTPError as e:
    print(f'HTTP Error: {e.code} {e.reason}')
    body = e.read().decode('utf-8', errors='replace')
    print(f'Response: {body[:500]}')
