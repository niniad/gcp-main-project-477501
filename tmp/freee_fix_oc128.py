"""
oc_128 の負値エントリーを freee に手動登録
debit 支払手数料 -13,240 → credit 支払手数料 13,240
credit 事業主借  -13,240 → debit  事業主借  13,240
"""
import sys, json
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/ninni/.claude/skills/freee/scripts')
from auth import get_access_token, get_company_id, get_headers, FREEE_API_BASE
import requests

token = get_access_token()
cid   = get_company_id(token)
headers = get_headers(token)

payload = {
    'company_id': cid,
    'issue_date': '2023-12-31',
    'adjustment': False,
    'details': [
        {
            'entry_side': 'debit',
            'account_item_id': 786598262,   # 事業主借
            'amount': 13240,
            'tax_code': 0,
            'description': '確定申告値一致調整（FY2023）Amazon送金失敗返金¥13,240 誤分類修正',
        },
        {
            'entry_side': 'credit',
            'account_item_id': 786598332,   # 支払手数料
            'amount': 13240,
            'tax_code': 0,
            'description': '確定申告値一致調整（FY2023）Amazon送金失敗返金¥13,240 誤分類修正',
        },
    ],
}

res = requests.post(f'{FREEE_API_BASE}/manual_journals', headers=headers, json=payload)
if res.status_code in (200, 201):
    mj = res.json().get('manual_journal', {})
    print(f'✅ 登録完了 id={mj.get("id")} date={mj.get("issue_date")}')
else:
    print(f'✗ エラー {res.status_code}: {res.text[:300]}')
