"""Check specific record with transfer link"""
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

req = urllib.request.Request(f'{BASE_URL}/tables/{TABLE_ID}/records/44')
req.add_header('xc-token', TOKEN)
with urllib.request.urlopen(req) as r:
    data = json.loads(r.read().decode('utf-8'))

print(json.dumps(data, ensure_ascii=False, indent=2))
