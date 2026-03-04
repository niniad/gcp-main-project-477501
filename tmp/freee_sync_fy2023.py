"""
BQ journal_entries → freee 振替伝票 一括同期（FY2023）
1. BQ から FY2023 全仕訳を取得
2. freee の FY2023 振替伝票・取引を全削除
3. BQ データを振替伝票として freee に登録
"""
import sys, json, time
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/ninni/.claude/skills/freee/scripts')

from auth import get_access_token, get_company_id, get_headers, FREEE_API_BASE
import requests
from google.cloud import bigquery

# === 設定 ===
FISCAL_YEAR = 2023
BQ_PROJECT = 'main-project-477501'

# BQ account_name → freee account_item_id マッピング
ACCOUNT_MAP = {
    'Amazon出品アカウント': 1008403397,
    'ESPRIME': 1007511503,
    'THE直行便': 1007507685,
    'YP': 1007511655,
    'PayPay銀行': 1007592863,
    '事業主借': 786598262,
    '事業主貸': 786598241,
    '仕入高': 786598280,
    '仮払金': 786598216,
    '商品': 786598202,
    '地代家賃': 786598329,
    '売上値引高': 786598269,
    '売上戻り高': 786598270,
    '売上高': 786598267,
    '売掛金': 786598200,
    '外注費': 786598323,
    '広告宣伝費': 786598298,
    '支払手数料': 786598332,
    '新聞図書費': 786598346,
    '未払金': 786598249,
    '楽天銀行': 1007579001,
    '消耗品費': 786598305,
    '為替差損益': 1007603892,
    '研修採用費': 1007579026,
    '研究開発費': 786598431,
    '荷造運賃': 786598290,
    '諸会費': 786598354,
    '販売手数料': 786598349,
    '通信費': 786598297,
    '開業費': 786598240,
    '雑収入': 786598277,
    '雑費': 786598367,
    '保険料': 786598302,
    '減価償却費': 786598309,
    '旅費交通費': 786598295,
    'Amazon手数料': 1007578986,
}


def api_call_with_retry(method, url, headers, json_data=None, max_retries=3):
    """freee APIコール（レート制限対応）"""
    for attempt in range(max_retries):
        if method == 'GET':
            res = requests.get(url, headers=headers)
        elif method == 'POST':
            res = requests.post(url, headers=headers, json=json_data)
        elif method == 'DELETE':
            res = requests.delete(url, headers=headers)
        else:
            raise ValueError(f"Unknown method: {method}")

        if res.status_code == 429:
            wait = int(res.headers.get('Retry-After', 60))
            print(f"    Rate limited. Waiting {wait}s...")
            time.sleep(wait)
            continue
        return res
    return res


def step1_fetch_bq(fiscal_year):
    """BQ から仕訳データを取得してトランザクション単位にグループ化"""
    print(f"\n=== Step 1: BQ FY{fiscal_year} データ取得 ===")
    client = bigquery.Client(project=BQ_PROJECT)
    query = f"""
    SELECT source_table, source_id,
           FORMAT_DATE('%Y-%m-%d', journal_date) as journal_date,
           entry_side, account_name, amount_jpy, description
    FROM `{BQ_PROJECT}.accounting.journal_entries`
    WHERE fiscal_year = {fiscal_year}
    ORDER BY journal_date, source_table, source_id, entry_side
    """
    rows = list(client.query(query).result())
    print(f"  取得行数: {len(rows)}")

    # トランザクション単位にグループ化
    txns = {}
    for row in rows:
        key = f"{row.source_table}:{row.source_id}"
        if key not in txns:
            txns[key] = {
                'journal_date': row.journal_date,
                'source_key': key,
                'details': []
            }
        txns[key]['details'].append({
            'entry_side': row.entry_side,
            'account_name': row.account_name,
            'amount': row.amount_jpy,
            'description': row.description or '',
        })

    print(f"  トランザクション数: {len(txns)}")

    # マッピング確認
    missing = set()
    for txn in txns.values():
        for d in txn['details']:
            if d['account_name'] not in ACCOUNT_MAP:
                missing.add(d['account_name'])
    if missing:
        print(f"  ⚠ マッピング未定義: {missing}")
        sys.exit(1)

    # 貸借バランス検証
    for key, txn in txns.items():
        dr = sum(d['amount'] for d in txn['details'] if d['entry_side'] == 'debit')
        cr = sum(d['amount'] for d in txn['details'] if d['entry_side'] == 'credit')
        if dr != cr:
            print(f"  ⚠ 貸借不一致: {key} Dr={dr:,} Cr={cr:,}")

    # FY2022期末残高を開始残高仕訳として追加（freee にはFY2022データがないため）
    if fiscal_year == 2023:
        q_fy22 = f"""
        SELECT account_name,
          SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
        FROM `{BQ_PROJECT}.accounting.journal_entries`
        WHERE fiscal_year < {fiscal_year}
        GROUP BY 1
        HAVING ABS(SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END)) > 0
        """
        fy22_rows = list(client.query(q_fy22).result())
        if fy22_rows:
            opening = {'journal_date': f'{fiscal_year}-01-01', 'source_key': 'opening_balance', 'details': []}
            for row in fy22_rows:
                bal = int(row.balance)
                if row.account_name not in ACCOUNT_MAP:
                    print(f"  ⚠ 開始残高マッピング未定義: {row.account_name}")
                    sys.exit(1)
                opening['details'].append({
                    'entry_side': 'debit' if bal > 0 else 'credit',
                    'account_name': row.account_name,
                    'amount': abs(bal),
                    'description': 'FY2022期末残高（開始残高）',
                })
            dr = sum(d['amount'] for d in opening['details'] if d['entry_side'] == 'debit')
            cr = sum(d['amount'] for d in opening['details'] if d['entry_side'] == 'credit')
            assert dr == cr, f"開始残高貸借不一致: Dr={dr} Cr={cr}"
            txns['opening_balance'] = opening
            print(f"  開始残高仕訳追加: {len(fy22_rows)}科目 {dr:,}円")

    return list(txns.values())


def step2_clear_freee(token, cid, fiscal_year):
    """freee の FY データを全削除"""
    print(f"\n=== Step 2: freee FY{fiscal_year} データ削除 ===")
    headers = get_headers(token)
    start = f"{fiscal_year}-01-01"
    end = f"{fiscal_year}-12-31"

    # 振替伝票の削除
    print("  振替伝票を取得中...")
    mj_ids = []
    offset = 0
    while True:
        url = f"{FREEE_API_BASE}/manual_journals?company_id={cid}&start_issue_date={start}&end_issue_date={end}&limit=100&offset={offset}"
        res = api_call_with_retry('GET', url, headers)
        if res.status_code != 200:
            print(f"    Error: {res.status_code} {res.text[:200]}")
            break
        batch = res.json().get('manual_journals', [])
        if not batch:
            break
        mj_ids.extend([mj['id'] for mj in batch])
        offset += len(batch)

    print(f"  振替伝票: {len(mj_ids)}件")
    for i, mj_id in enumerate(mj_ids):
        url = f"{FREEE_API_BASE}/manual_journals/{mj_id}?company_id={cid}"
        res = api_call_with_retry('DELETE', url, headers)
        if res.status_code not in (200, 204):
            print(f"    ✗ ID:{mj_id} 削除失敗: {res.status_code}")
        if (i + 1) % 50 == 0:
            print(f"    {i+1}/{len(mj_ids)} 削除済み")
    if mj_ids:
        print(f"  ✓ 振替伝票 {len(mj_ids)}件 削除完了")

    # 取引の削除
    print("  取引を取得中...")
    deal_ids = []
    offset = 0
    while True:
        url = f"{FREEE_API_BASE}/deals?company_id={cid}&start_issue_date={start}&end_issue_date={end}&limit=100&offset={offset}"
        res = api_call_with_retry('GET', url, headers)
        if res.status_code != 200:
            print(f"    Error: {res.status_code} {res.text[:200]}")
            break
        batch = res.json().get('deals', [])
        if not batch:
            break
        deal_ids.extend([d['id'] for d in batch])
        offset += len(batch)

    print(f"  取引: {len(deal_ids)}件")
    for i, deal_id in enumerate(deal_ids):
        url = f"{FREEE_API_BASE}/deals/{deal_id}?company_id={cid}"
        res = api_call_with_retry('DELETE', url, headers)
        if res.status_code not in (200, 204):
            print(f"    ✗ ID:{deal_id} 削除失敗: {res.status_code}")
        if (i + 1) % 50 == 0:
            print(f"    {i+1}/{len(deal_ids)} 削除済み")
    if deal_ids:
        print(f"  ✓ 取引 {len(deal_ids)}件 削除完了")


def step3_import(token, cid, txns):
    """BQ トランザクションを freee 振替伝票として登録"""
    print(f"\n=== Step 3: freee 振替伝票登録 ({len(txns)}件) ===")
    headers = get_headers(token)

    success = 0
    errors = 0
    for i, txn in enumerate(txns):
        details = []
        for d in txn['details']:
            details.append({
                'entry_side': d['entry_side'],
                'account_item_id': ACCOUNT_MAP[d['account_name']],
                'amount': d['amount'],
                'tax_code': 0,
                'description': d['description'][:255] if d['description'] else '',
            })

        payload = {
            'company_id': cid,
            'issue_date': txn['journal_date'],
            'adjustment': False,
            'details': details,
        }

        url = f"{FREEE_API_BASE}/manual_journals"
        res = api_call_with_retry('POST', url, headers, json_data=payload)

        if res.status_code in (200, 201):
            success += 1
        else:
            errors += 1
            print(f"  ✗ [{txn['source_key']}] {txn['journal_date']}: {res.status_code}")
            err_text = res.text[:300]
            print(f"    {err_text}")
            if errors >= 5:
                print("  エラーが多すぎるため中断")
                break

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(txns)} 登録済み (成功:{success} 失敗:{errors})")

    print(f"\n  完了: 成功 {success} / 失敗 {errors} / 合計 {len(txns)}")
    return success, errors


def main():
    # Step 1: BQ データ取得
    txns = step1_fetch_bq(FISCAL_YEAR)

    # 確認
    print(f"\n{'='*50}")
    print(f"FY{FISCAL_YEAR}: {len(txns)}件の振替伝票を freee に同期します")
    print(f"  - freee の既存 FY{FISCAL_YEAR} データを全削除")
    print(f"  - BQ のデータを振替伝票として新規登録")
    confirm = input("実行しますか？ (y/n): ").strip().lower()
    if confirm != 'y':
        print("キャンセル")
        return

    # freee認証
    token = get_access_token()
    cid = get_company_id(token)
    print(f"Company: {cid}")

    # Step 2: freee 既存データ削除
    step2_clear_freee(token, cid, FISCAL_YEAR)

    # Step 3: インポート
    step3_import(token, cid, txns)

    print("\n=== 同期完了 ===")


if __name__ == '__main__':
    main()
