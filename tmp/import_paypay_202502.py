"""
PayPay銀行 2026年2月明細 インポート + Amazon振替リンク
"""
import sys, sqlite3, csv, io
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
CSV_PATH = 'C:/Users/ninni/projects/rawdata/paypay銀行/202502_paypay銀行入出金明細(005-1216264).csv'

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# ===== Step 1: CSV 読み込み =====
with open(CSV_PATH, encoding='cp932') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f'CSV読み込み: {len(rows)}行')

# ===== Step 2: 挿入データ準備 =====
records = []
for r in rows:
    y, m, d = r['操作日(年)'].strip('"'), r['操作日(月)'].strip('"'), r['操作日(日)'].strip('"')
    h, mi, s = r['操作時刻(時)'].strip('"'), r['操作時刻(分)'].strip('"'), r['操作時刻(秒)'].strip('"')
    date_str = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    time_str = f"{int(h):02d}:{int(mi):02d}:{int(s):02d}"

    pay = r['お支払金額'].strip('"').strip()
    recv = r['お預り金額'].strip('"').strip()

    if recv:
        amount = int(recv)
    elif pay:
        amount = -int(pay)
    else:
        amount = 0

    zandaka = int(r['残高'].strip('"')) if r['残高'].strip('"') else None
    memo = r['メモ'].strip('"') if r['メモ'] else None

    records.append({
        'date': date_str,
        'time': time_str,
        'desc': r['摘要'].strip('"'),
        'amount': amount,
        'balance': zandaka,
        'memo': memo,
    })

# ===== Step 3: 重複チェック =====
cur.execute('SELECT MAX(id) FROM "nc_opau___PayPay銀行入出金明細"')
max_id = cur.fetchone()[0]
print(f'現在のPayPay MAX id: {max_id}')

# ===== Step 4: 挿入 =====
inserted = []
for rec in records:
    cur.execute('''
        INSERT INTO "nc_opau___PayPay銀行入出金明細"
        (操作日, 操作時刻, 摘要, お預かり金額, 残高, メモ)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (rec['date'], rec['time'], rec['desc'], rec['amount'], rec['balance'], rec['memo']))
    inserted.append(cur.lastrowid)
    print(f'  INSERT id={cur.lastrowid} {rec["date"]} ¥{rec["amount"]:,} [{rec["desc"]}]')

print(f'\n{len(inserted)}行挿入完了')

# ===== Step 5: Amazonマッチング確認 =====
print('\n=== Amazon対応エントリ確認 ===')
# 20,333 → 2026-02-12, 89,455 → 2026-02-26
amazon_matches = []
for new_id, rec in zip(inserted, records):
    if rec['amount'] in (20333, 89455) and 'アマゾン' in rec['desc']:
        print(f'  PayPay id={new_id} {rec["date"]} ¥{rec["amount"]:,} [{rec["desc"]}]')
        amazon_matches.append((new_id, rec['date'], rec['amount']))

# ===== Step 6: 振替レコード作成 =====
print('\n=== 振替レコード作成 ===')
cur.execute('SELECT MAX(id) FROM "nc_opau___振替"')
transfer_max = cur.fetchone()[0]
print(f'現在の振替 MAX id: {transfer_max}')

# Amazon id=675 (¥20,333) × PayPay
amazon_data = {
    20333: {'amazon_id': 675, 'settlement': '12246601693'},
    89455: {'amazon_id': 685, 'settlement': '12258743623'},
}

new_transfer_ids = {}
for paypay_id, date, amount in amazon_matches:
    info = amazon_data[amount]
    memo = f'Amazon settlement {info["settlement"]}'
    cur.execute('''
        INSERT INTO "nc_opau___振替" (振替日, 金額, メモ)
        VALUES (?, ?, ?)
    ''', (date, amount, memo))
    tr_id = cur.lastrowid
    new_transfer_ids[amount] = tr_id
    print(f'  振替 id={tr_id} {date} ¥{amount:,} [{memo}]')

    # PayPay 振替_id 更新
    cur.execute('UPDATE "nc_opau___PayPay銀行入出金明細" SET "nc_opau___振替_id"=? WHERE id=?',
                (tr_id, paypay_id))
    print(f'    PayPay id={paypay_id} → 振替_id={tr_id}')

    # Amazon 振替_id 更新
    cur.execute('UPDATE "nc_opau___Amazon出品アカウント明細" SET "nc_opau___振替_id"=? WHERE id=?',
                (tr_id, info['amazon_id']))
    print(f'    Amazon id={info["amazon_id"]} → 振替_id={tr_id}')

# ===== Step 7: コミット & 確認 =====
conn.commit()

print('\n=== 最終確認 ===')
cur.execute('SELECT id, 操作日, お預かり金額, 摘要, "nc_opau___振替_id" FROM "nc_opau___PayPay銀行入出金明細" WHERE id > ?', (max_id,))
for r in cur.fetchall():
    tr = f' ✅tr={r[4]}' if r[4] else ''
    print(f'  id={r[0]} {r[1]} ¥{r[2]:,} [{r[3]}]{tr}')

conn.close()
print('\n完了')
