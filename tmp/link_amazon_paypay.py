"""
Step 3: Amazon DEPOSIT x PayPay銀行 振替リンク
2025-07以降、Amazon振込先がPayPay銀行に変更されていた。
金額一致した14ペアを振替テーブルに追加しリンクする。
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
from datetime import datetime, timezone

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
AMAZON_TABLE = 'nc_opau___Amazon\u51fa\u54c1\u30a2\u30ab\u30a6\u30f3\u30c8\u660e\u7d30'  # Amazon出品アカウント明細
PAYPAY_TABLE = 'nc_opau___PayPay\u9280\u884c\u5165\u51fa\u91d1\u660e\u7d30'              # PayPay銀行入出金明細
TRANSFER_TABLE = 'nc_opau___\u632f\u66ff'                                                # 振替

# 金額一致確認済みのペア: (amazon_nocodb_id, paypay_nocodb_id, date, amount)
PAIRS = [
    (513, 16,  '2025-07-02', 64490),
    (522, 20,  '2025-07-16', 24497),
    (543, 33,  '2025-08-13', 66627),
    (553, 37,  '2025-08-27', 19036),
    (562, 41,  '2025-09-10', 17315),
    (573, 44,  '2025-09-24', 31126),
    (585, 54,  '2025-10-08', 73046),
    (593, 60,  '2025-10-22', 108943),
    (613, 74,  '2025-11-19', 115597),
    (624, 85,  '2025-12-03', 14339),
    (635, 88,  '2025-12-17', 97377),
    (644, 97,  '2025-12-31', 62866),
    (654, 101, '2026-01-14', 13819),
    (665, 111, '2026-01-28', 62392),
]

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')

# テーブルと列の存在確認
cursor.execute(f'PRAGMA table_info("{AMAZON_TABLE}")')
amazon_cols = [c[1] for c in cursor.fetchall()]
print('Amazon列:', amazon_cols)

cursor.execute(f'PRAGMA table_info("{PAYPAY_TABLE}")')
paypay_cols = [c[1] for c in cursor.fetchall()]
print('PayPay列:', paypay_cols)

# 振替_id列を特定（Unicodeで直接）
AMAZON_TRANSFER_COL = 'nc_opau___\u632f\u66ff_id'   # nc_opau___振替_id
PAYPAY_TRANSFER_COL = 'nc_opau___\u632f\u66ff_id'    # 同じ列名

print(f'\nAmazon振替列: {AMAZON_TRANSFER_COL}')
print(f'PayPay振替列: {PAYPAY_TRANSFER_COL}')

# 現在の最大ID確認
cursor.execute(f'SELECT MAX(id), MAX(nc_order) FROM "{TRANSFER_TABLE}"')
max_id, max_order = cursor.fetchone()
print(f'\n現在の振替テーブル: MAX id={max_id}, MAX nc_order={max_order}')

# 事前確認: 既存振替_idがないか
print('\n=== 事前確認（既存リンク状況） ===')
for amazon_id, paypay_id, date, amount in PAIRS:
    cursor.execute(f'SELECT id, "{AMAZON_TRANSFER_COL}" FROM "{AMAZON_TABLE}" WHERE id=?', (amazon_id,))
    a_row = cursor.fetchone()
    cursor.execute(f'SELECT id, "{PAYPAY_TRANSFER_COL}" FROM "{PAYPAY_TABLE}" WHERE id=?', (paypay_id,))
    p_row = cursor.fetchone()
    a_status = a_row[1] if a_row else 'NOT FOUND'
    p_status = p_row[1] if p_row else 'NOT FOUND'
    print(f'  Amazon {amazon_id} ({date} ¥{amount:,}): 振替_id={a_status}  |  PayPay {paypay_id}: 振替_id={p_status}')

print('\n=== 振替レコード追加 + リンク ===')
new_transfer_ids = []
for i, (amazon_id, paypay_id, date, amount) in enumerate(PAIRS):
    new_id = max_id + 1 + i
    new_order = float(max_order + 1 + i) if max_order else float(new_id)

    # 振替テーブルに INSERT
    cursor.execute(f'''
        INSERT INTO "{TRANSFER_TABLE}" (id, created_at, updated_at, nc_order, "\u632f\u66ff\u65e5", "\u91d1\u984d", "\u30e1\u30e2")
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (new_id, now, now, new_order, date, amount, f'Amazon\u2192PayPay {date} \xa5{amount:,}'))

    # Amazon DEPOSIT に振替_id セット
    cursor.execute(f'UPDATE "{AMAZON_TABLE}" SET "{AMAZON_TRANSFER_COL}"=?, updated_at=? WHERE id=?',
                   (new_id, now, amazon_id))

    # PayPay銀行 に振替_id セット
    cursor.execute(f'UPDATE "{PAYPAY_TABLE}" SET "{PAYPAY_TRANSFER_COL}"=?, updated_at=? WHERE id=?',
                   (new_id, now, paypay_id))

    print(f'  振替 id={new_id}: Amazon {amazon_id} <-> PayPay {paypay_id} ({date} ¥{amount:,})')
    new_transfer_ids.append(new_id)

conn.commit()
conn.close()
print(f'\n=== SQLite コミット完了 ===')
print(f'追加振替: {len(PAIRS)} 件 (id={new_transfer_ids[0]}〜{new_transfer_ids[-1]})')
print('次のステップ: BQ sync -> P/L検証')
