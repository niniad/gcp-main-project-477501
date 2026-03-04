"""Check NocoDB API for Amazon table"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import json
import urllib.request
from google.cloud import secretmanager

sm_client = secretmanager.SecretManagerServiceClient()
resp = sm_client.access_secret_version(name='projects/main-project-477501/secrets/NOCODB_API_TOKEN/versions/latest')
TOKEN = resp.payload.data.decode('utf-8').strip()

BASE_URL = 'http://localhost:8080/api/v2'
BASE_ID = 'pbvdkr5cvkj4n2e'

req = urllib.request.Request(f'{BASE_URL}/meta/bases/{BASE_ID}/tables')
req.add_header('xc-token', TOKEN)
with urllib.request.urlopen(req) as r:
    data = json.loads(r.read().decode('utf-8'))

for t in data.get('list', []):
    marker = ' *** ' if 'Amazon' in t['title'] else '     '
    print(f'{marker}{t["title"]:40} id={t["id"]}')
