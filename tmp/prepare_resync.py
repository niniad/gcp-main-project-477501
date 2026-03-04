# -*- coding: utf-8 -*-
"""FY2023再同期の準備: freee手動登録3件削除 + NocoDB手動仕訳3件追加"""
import sys, time
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/ninni/.claude/skills/freee/scripts')

from auth import get_access_token, get_company_id, get_headers, FREEE_API_BASE
import requests
import sqlite3

# === Part 1: freee 手動登録3件を削除 ===
print('=== Part 1: freee 手動登録エントリ削除 ===')
token = get_access_token()
cid = get_company_id(token)
headers = get_headers(token)

manual_ids = [3321786398, 3321786415, 3321786440]
for mj_id in manual_ids:
    url = f'{FREEE_API_BASE}/manual_journals/{mj_id}?company_id={cid}'
    res = requests.delete(url, headers=headers)
    if res.status_code in (200, 204):
        print(f'  ✓ ID:{mj_id} 削除成功')
    else:
        print(f'  ✗ ID:{mj_id} 削除失敗 ({res.status_code}): {res.text[:200]}')
    time.sleep(1)

# === Part 2: NocoDB手動仕訳に3件追加 ===
print('\n=== Part 2: NocoDB手動仕訳に3件追加 ===')
db = sqlite3.connect('C:/Users/ninni/nocodb/noco.db')
cur = db.cursor()

entries = [
    {
        'date': '2023-02-27',
        'dr_id': 9,   # Amazon出品アカウント
        'cr_id': 85,  # 事業主借
        'amount': 15078,
        'description': 'Amazon不足額支払い（個人口座→Amazon）',
        'source': 'MF調整',
        'category': '調整',
    },
    {
        'date': '2024-01-01',
        'dr_id': 70,  # 未払金
        'cr_id': 85,  # 事業主借
        'amount': 5998,
        'description': 'NTTカード個人利用分振替（MF決算書FY2024期首調整に対応）',
        'source': 'MF調整',
        'category': '期首調整',
    },
    {
        'date': '2024-12-31',
        'dr_id': 162, # 雑費
        'cr_id': 5,   # ESPRIME
        'amount': 189,
        'description': 'ESPRIME預け金 CNY→JPY為替換算端数調整（実残高200,657円に合わせる）',
        'source': 'MF調整',
        'category': '期末調整',
    },
]

for entry in entries:
    # 重複チェック
    cur.execute("""
        SELECT id FROM "nc_opau___手動仕訳"
        WHERE "金額" = ? AND "仕訳日" = ? AND "借方科目_id" = ? AND "貸方科目_id" = ?
    """, (entry['amount'], entry['date'], entry['dr_id'], entry['cr_id']))
    existing = cur.fetchone()
    if existing:
        print(f'  既存あり id={existing[0]}: {entry["date"]} ¥{entry["amount"]:,} → スキップ')
        continue

    cur.execute('SELECT MAX(id) FROM "nc_opau___手動仕訳"')
    max_id = cur.fetchone()[0]
    next_order = max_id + 1.0

    cur.execute("""
    INSERT INTO "nc_opau___手動仕訳" (
      created_at, updated_at, nc_order,
      "仕訳日", "借方科目_id", "貸方科目_id", "金額", "摘要", "ソース", "仕訳区分",
      "nc_opau___freee勘定科目_id", "nc_opau___freee勘定科目_id1"
    ) VALUES (
      datetime('now'), datetime('now'), ?,
      ?, ?, ?, ?,
      ?,
      ?, ?,
      ?, ?
    )
    """, (next_order,
          entry['date'], entry['dr_id'], entry['cr_id'], entry['amount'],
          entry['description'],
          entry['source'], entry['category'],
          entry['dr_id'], entry['cr_id']))

    new_id = cur.lastrowid
    print(f'  ✓ id={new_id}: {entry["date"]} Dr={entry["dr_id"]} Cr={entry["cr_id"]} ¥{entry["amount"]:,}')

db.commit()
db.close()
print('\n準備完了')
