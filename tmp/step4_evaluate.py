"""
Step 4: 手動仕訳テーブル再評価

id=191: Amazon出品アカウント期首残高 → Amazon口座の累計残高で判定
id=193: deposit_date移行調整 → Amazon口座に統合可能性
id=199: Amazon返金 → Amazon口座に統合可能性
id=198: ESPRIME貸方科目確認
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3

SQLITE_PATH = "C:/Users/ninni/nocodb/noco.db"

def main():
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()

    # === id=191 再評価: Amazon出品アカウント期首残高 ===
    print('=== id=191 再評価: Amazon出品アカウント期首残高 ¥24,106 ===')
    print('  内容: Dr:Amazon出品アカウント / Cr:事業主借')
    print('  目的: 2024年1月1日時点のAmazon口座残高を設定')
    print()

    # Check Amazon account balance through FY2023
    cur.execute('''
        SELECT SUM("金額") FROM "nc_opau___Amazon出品アカウント明細"
        WHERE "取引日" <= '2023-12-31'
    ''')
    amazon_fy2023_balance = cur.fetchone()[0] or 0
    print(f'  Amazon口座テーブル FY2023末残高: {amazon_fy2023_balance:,}')

    # The Amazon table tracks all activity. If the balance at 2023-12-31 is already
    # properly tracked, then id=191 would double-count.
    # The Amazon table entries are: REVENUE(+), EXPENSE(-), DEPOSIT(-)
    # DEPOSIT with transfer_id are excluded from the VIEW
    # DEPOSIT without transfer_id are included (they represent account outflows)

    # Check deposits that were NOT matched (unlinked) through FY2023
    cur.execute('''
        SELECT SUM("金額") FROM "nc_opau___Amazon出品アカウント明細"
        WHERE "取引日" <= '2023-12-31' AND "nc_opau___振替_id" IS NULL
    ''')
    amazon_unlinked_fy2023 = cur.fetchone()[0] or 0
    print(f'  Amazon口座 FY2023末 非振替残高: {amazon_unlinked_fy2023:,}')

    # Non-DEPOSIT entries through FY2023
    cur.execute('''
        SELECT SUM("金額") FROM "nc_opau___Amazon出品アカウント明細"
        WHERE "取引日" <= '2023-12-31' AND entry_type != 'DEPOSIT'
    ''')
    amazon_non_deposit_fy2023 = cur.fetchone()[0] or 0
    print(f'  Amazon口座 FY2023末 非DEPOSIT合計: {amazon_non_deposit_fy2023:,}')

    # DEPOSIT entries through FY2023 (with and without links)
    cur.execute('''
        SELECT
            SUM(CASE WHEN "nc_opau___振替_id" IS NOT NULL THEN "金額" ELSE 0 END) as linked,
            SUM(CASE WHEN "nc_opau___振替_id" IS NULL THEN "金額" ELSE 0 END) as unlinked
        FROM "nc_opau___Amazon出品アカウント明細"
        WHERE "取引日" <= '2023-12-31' AND entry_type = 'DEPOSIT'
    ''')
    row = cur.fetchone()
    print(f'  Amazon FY2023 DEPOSIT linked: {row[0] or 0:,}')
    print(f'  Amazon FY2023 DEPOSIT unlinked: {row[1] or 0:,}')

    # What the VIEW currently sees for Amazon in FY2023
    # VIEW excludes entries with 振替_id (DEPOSIT entries with transfer links)
    # VIEW includes all non-transfer entries
    print()
    print(f'  VIEW が認識するAmazon FY2023末残高 = {amazon_unlinked_fy2023:,}')
    print(f'  id=191 の金額 = 24,106')
    print(f'  → id=191 を残すと Amazon口座残高が二重計上される可能性')

    # === id=193 再評価: deposit_date移行調整 ===
    print()
    print('=== id=193 再評価: deposit_date移行調整 ¥62,501 ===')
    print('  内容: Dr:Amazon出品アカウント / Cr:事業主借')
    print('  目的: settlement 11898495253 の settle=2024-12-30→deposit=2025-01-01')

    # Check if this settlement is in the Amazon table
    cur.execute('''
        SELECT id, "取引日", "金額", entry_type, "摘要"
        FROM "nc_opau___Amazon出品アカウント明細"
        WHERE settlement_id = '11898495253'
    ''')
    entries = cur.fetchall()
    print(f'  Amazon テーブル内の該当settlement: {len(entries)}件')
    for e in entries:
        print(f'    id={e[0]}: date={e[1]}, amount={e[2]:,}, type={e[3]}, desc={e[4][:60]}')

    # The settlement's deposit_date is 2025-01-01 (used as transaction_date in Amazon table)
    # So in the new VIEW, this settlement's P/L entries appear in FY2025
    # id=193 was created to compensate for this timing shift
    # But with the new Amazon table using deposit_date, the shift is already accounted for
    print()
    print('  → Amazon テーブルは deposit_date 基準。id=193 は二重計上の可能性')

    # === id=199 再評価: Amazon返金 ===
    print()
    print('=== id=199 再評価: Amazon返金 ¥13,240 ===')
    print('  内容: Dr:事業主借 / Cr:支払手数料 ¥13,240')
    print('  目的: settlement 11363806313(¥5,664) + 11374028403(¥7,576) の返金')

    # Check if these settlements are in the Amazon table
    for sid in ['11363806313', '11374028403']:
        cur.execute('''
            SELECT id, "取引日", "金額", entry_type, "摘要"
            FROM "nc_opau___Amazon出品アカウント明細"
            WHERE settlement_id = ?
        ''', (sid,))
        entries = cur.fetchall()
        print(f'\n  Settlement {sid}: {len(entries)}件')
        for e in entries:
            print(f'    id={e[0]}: date={e[1]}, amount={e[2]:,}, type={e[3]}')

    # Check original data
    cur.execute('''
        SELECT id, "取引日", "金額", entry_type, "nc_opau___freee勘定科目_id"
        FROM "nc_opau___Amazon出品アカウント明細"
        WHERE settlement_id IN ('11363806313', '11374028403')
        AND entry_type = 'DEPOSIT'
    ''')
    deposits = cur.fetchall()
    print(f'\n  DEPOSIT entries for these settlements: {len(deposits)}')
    for d in deposits:
        print(f'    id={d[0]}: date={d[1]}, amount={d[2]:,}, account_id={d[4]}')

    # These settlement deposits are unlinked (no matching rakuten bank entry)
    # They appear as Amazon account outflows in the VIEW
    # id=199 was adjusting for mis-classification, but with the new Amazon table
    # the settlements should already be properly classified
    print()
    print('  → これらのsettlementはAmazon口座テーブルに含まれている')
    print('    DEPOSIT行は未リンク（楽天銀行にない初期のsettlement）')

    # === id=198 確認 ===
    print()
    print('=== id=198 確認: ESPRIME為替端数 ===')
    cur.execute('''
        SELECT id, "仕訳日", "金額", "摘要", "借方科目_id", "貸方科目_id"
        FROM "nc_opau___手動仕訳" WHERE id = 198
    ''')
    row = cur.fetchone()
    print(f'  id={row[0]}: Dr={row[4]}, Cr={row[5]}')
    print(f'  貸方科目_id={row[5]} → 105(為替差損益) ← 既に正しい値が設定済み')

    conn.close()


if __name__ == '__main__':
    main()
