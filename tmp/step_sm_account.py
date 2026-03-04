"""
Step 1: セールモンスター口座化
- freee勘定科目テーブルに「セールモンスター」を INSERT（Amazon id=9 のスキーマを複製）
- 楽天銀行 6件・PayPay 2件の freee勘定科目_id を新 SM ID に更新
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
from datetime import datetime, timezone

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
RAKUTEN_IDS = [97, 104, 114, 124, 134, 137]
PAYPAY_IDS = [81, 106]
OLD_ACCOUNT_ID = 12  # 売掛金

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 1. 現在のスキーマ確認（Amazon id=9）
cursor.execute('SELECT * FROM "nc_opau___freee勘定科目" WHERE id = 9')
amazon_row = cursor.fetchone()
cols = [desc[0] for desc in cursor.description]
amazon = dict(zip(cols, amazon_row))
print('=== Amazon 行 (id=9) ===')
for k, v in amazon.items():
    print(f'  {k}: {repr(v)}')

# 既存のセールモンスターがないか確認
cursor.execute('SELECT id, "勘定科目" FROM "nc_opau___freee勘定科目" WHERE "勘定科目" = ?', ('セールモンスター',))
existing = cursor.fetchone()
if existing:
    sm_id = existing[0]
    print(f'\n既存のセールモンスターが見つかりました: id={sm_id}')
else:
    # 2. セールモンスターを INSERT
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')
    cursor.execute('''
        INSERT INTO "nc_opau___freee勘定科目" (
            created_at, updated_at, created_by, updated_by, nc_order,
            "勘定科目", "表示名_決算書_", "小分類", "中分類", "大分類",
            "収入取引相手方勘定科目", "支出取引相手方勘定科目",
            "税区分", "ショートカット1", "ショートカット2", "入力候補",
            "補助科目優先タグ", cf_category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        now, now,
        amazon['created_by'], amazon['updated_by'],
        166.0,                                # nc_order
        'セールモンスター',                      # 勘定科目
        amazon['表示名_決算書_'],                # 現金
        amazon['小分類'],                       # 現金・預金
        amazon['中分類'],                       # 流動資産
        amazon['大分類'],                       # 資産
        amazon['収入取引相手方勘定科目'],
        amazon['支出取引相手方勘定科目'],
        amazon['税区分'],                       # 対象外
        amazon['ショートカット1'],
        amazon['ショートカット2'],
        amazon['入力候補'],                     # YES
        amazon['補助科目優先タグ'],
        amazon['cf_category'],                 # 営業活動
    ))
    sm_id = cursor.lastrowid
    print(f'\nセールモンスター INSERT 完了: id={sm_id}')

print(f'\n=== SM アカウント ID: {sm_id} ===')

# 3. 楽天銀行 6件 更新前確認
cursor.execute(
    f'SELECT id, "nc_opau___freee勘定科目_id" FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE id IN ({",".join(["?"]*len(RAKUTEN_IDS))})',
    RAKUTEN_IDS
)
print('\n=== 楽天銀行 更新前 ===')
for row in cursor.fetchall():
    print(f'  id={row[0]}, account_id={row[1]}')

# 4. PayPay銀行 2件 更新前確認
cursor.execute(
    f'SELECT id, "nc_opau___freee勘定科目_id" FROM "nc_opau___PayPay銀行入出金明細" WHERE id IN ({",".join(["?"]*len(PAYPAY_IDS))})',
    PAYPAY_IDS
)
print('\n=== PayPay銀行 更新前 ===')
for row in cursor.fetchall():
    print(f'  id={row[0]}, account_id={row[1]}')

# 5. 楽天銀行 6件 UPDATE
now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')
cursor.execute(
    f'UPDATE "nc_opau___楽天銀行ビジネス口座入出金明細" SET "nc_opau___freee勘定科目_id"=?, updated_at=? WHERE id IN ({",".join(["?"]*len(RAKUTEN_IDS))})',
    [sm_id, now] + RAKUTEN_IDS
)
print(f'\n楽天銀行 {cursor.rowcount}件 更新完了: account_id → {sm_id}')

# 6. PayPay銀行 2件 UPDATE
cursor.execute(
    f'UPDATE "nc_opau___PayPay銀行入出金明細" SET "nc_opau___freee勘定科目_id"=?, updated_at=? WHERE id IN ({",".join(["?"]*len(PAYPAY_IDS))})',
    [sm_id, now] + PAYPAY_IDS
)
print(f'PayPay銀行 {cursor.rowcount}件 更新完了: account_id → {sm_id}')

# 7. 更新後確認
cursor.execute(
    f'SELECT id, "nc_opau___freee勘定科目_id" FROM "nc_opau___楽天銀行ビジネス口座入出金明細" WHERE id IN ({",".join(["?"]*len(RAKUTEN_IDS))})',
    RAKUTEN_IDS
)
print('\n=== 楽天銀行 更新後 ===')
for row in cursor.fetchall():
    print(f'  id={row[0]}, account_id={row[1]}')

cursor.execute(
    f'SELECT id, "nc_opau___freee勘定科目_id" FROM "nc_opau___PayPay銀行入出金明細" WHERE id IN ({",".join(["?"]*len(PAYPAY_IDS))})',
    PAYPAY_IDS
)
print('\n=== PayPay銀行 更新後 ===')
for row in cursor.fetchall():
    print(f'  id={row[0]}, account_id={row[1]}')

conn.commit()
conn.close()
print('\n=== SQLite コミット完了 ===')
print(f'セールモンスター id={sm_id}')
print('次のステップ: BQ sync → VIEW修正')
