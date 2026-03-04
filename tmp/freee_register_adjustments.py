# -*- coding: utf-8 -*-
"""freee に3件の調整仕訳を振替伝票として登録"""
import sys, time
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/ninni/.claude/skills/freee/scripts')

from auth import get_access_token, get_company_id, get_headers, FREEE_API_BASE
import requests

# freee account_item_id mapping
ACCOUNTS = {
    'Amazon出品アカウント': 1008403397,
    '事業主借': 786598262,
    '未払金': 786598249,
    '雑費': 786598367,
    'ESPRIME': 1007511503,
}

ENTRIES = [
    {
        'date': '2023-02-27',
        'details': [
            {'entry_side': 'debit', 'account': 'Amazon出品アカウント', 'amount': 15078},
            {'entry_side': 'credit', 'account': '事業主借', 'amount': 15078},
        ],
        'description': 'Amazon不足額支払い（個人口座→Amazon）',
    },
    {
        'date': '2024-01-01',
        'details': [
            {'entry_side': 'debit', 'account': '未払金', 'amount': 5998},
            {'entry_side': 'credit', 'account': '事業主借', 'amount': 5998},
        ],
        'description': 'NTTカード個人利用分振替（MF決算書FY2024期首調整に対応）',
    },
    {
        'date': '2024-12-31',
        'details': [
            {'entry_side': 'debit', 'account': '雑費', 'amount': 189},
            {'entry_side': 'credit', 'account': 'ESPRIME', 'amount': 189},
        ],
        'description': 'ESPRIME預け金 CNY→JPY為替換算端数調整（実残高200,657円に合わせる）',
    },
]


def main():
    token = get_access_token()
    cid = get_company_id(token)
    headers = get_headers(token)
    print(f'Company ID: {cid}')

    print('\n=== 登録予定の仕訳 ===')
    for i, entry in enumerate(ENTRIES, 1):
        print(f'\n仕訳 {i}: {entry["date"]}')
        for d in entry['details']:
            side = '借方' if d['entry_side'] == 'debit' else '貸方'
            print(f'  {side}: {d["account"]} ¥{d["amount"]:,}')
        print(f'  摘要: {entry["description"]}')

    confirm = input('\n上記3件をfreeeに登録しますか？ (y/n): ').strip().lower()
    if confirm != 'y':
        print('キャンセル')
        return

    for i, entry in enumerate(ENTRIES, 1):
        details = []
        for d in entry['details']:
            details.append({
                'entry_side': d['entry_side'],
                'account_item_id': ACCOUNTS[d['account']],
                'amount': d['amount'],
                'tax_code': 0,
                'description': entry['description'],
            })

        payload = {
            'company_id': cid,
            'issue_date': entry['date'],
            'adjustment': False,
            'details': details,
        }

        url = f'{FREEE_API_BASE}/manual_journals'
        res = requests.post(url, headers=headers, json=payload)

        if res.status_code in (200, 201):
            mj_id = res.json()['manual_journal']['id']
            print(f'✓ 仕訳{i}: 登録成功 (ID: {mj_id})')
        else:
            print(f'✗ 仕訳{i}: 登録失敗 ({res.status_code})')
            print(f'  {res.text[:500]}')

        time.sleep(1)

    print('\n完了')


if __name__ == '__main__':
    main()
