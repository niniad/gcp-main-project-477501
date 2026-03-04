"""
Step 2: 楽天銀行 × Amazon出品アカウント 振替リンク

Amazon精算日と楽天銀行入金日は1-3日ずれるため、
金額一致 + 日付±5日以内でマッチングする。
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
from datetime import datetime, timedelta

SQLITE_PATH = "C:/Users/ninni/nocodb/noco.db"

def parse_date(s):
    return datetime.strptime(s, '%Y-%m-%d').date()

def main():
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()

    # === 楽天銀行の Amazon入金エントリ取得 ===
    print('=== 楽天銀行 Amazon入金エントリ ===')
    cur.execute('''
        SELECT id, "取引日", "入出金_円_", "入出金先内容", "振替", "nc_opau___振替_id"
        FROM "nc_opau___楽天銀行ビジネス口座入出金明細"
        WHERE "nc_opau___freee勘定科目_id" = 9
        ORDER BY "取引日"
    ''')
    rakuten_amazon = cur.fetchall()
    print(f'  件数: {len(rakuten_amazon)}')

    # === Amazon DEPOSIT行取得 ===
    print('=== Amazon出品アカウント DEPOSIT行 ===')
    cur.execute('''
        SELECT id, "取引日", "金額", "摘要", "settlement_id", "nc_opau___振替_id"
        FROM "nc_opau___Amazon出品アカウント明細"
        WHERE entry_type = 'DEPOSIT'
        ORDER BY "取引日"
    ''')
    amazon_deposits = cur.fetchall()
    print(f'  件数: {len(amazon_deposits)}')

    # === 照合: 金額一致 + 日付±5日以内 ===
    print()
    print('=== 照合 (金額一致 + 日付±5日) ===')

    # Build Amazon deposit index by abs amount
    amazon_by_amount = {}
    for dep in amazon_deposits:
        amt = abs(dep[2])
        if amt not in amazon_by_amount:
            amazon_by_amount[amt] = []
        amazon_by_amount[amt].append(dep)

    matched = []
    unmatched_rakuten = []
    used_amazon_ids = set()

    for rak in rakuten_amazon:
        rak_id, rak_date_str, rak_amount, rak_desc, rak_transfer, rak_transfer_id = rak
        rak_date = parse_date(rak_date_str)
        rak_amt = abs(rak_amount)

        best_match = None
        best_diff = None

        if rak_amt in amazon_by_amount:
            for amazon_dep in amazon_by_amount[rak_amt]:
                if amazon_dep[0] in used_amazon_ids:
                    continue
                amazon_date = parse_date(amazon_dep[1])
                diff = abs((rak_date - amazon_date).days)
                if diff <= 5:
                    if best_diff is None or diff < best_diff:
                        best_match = amazon_dep
                        best_diff = diff

        if best_match:
            matched.append((rak, best_match, best_diff))
            used_amazon_ids.add(best_match[0])
        else:
            unmatched_rakuten.append(rak)

    unmatched_amazon = [d for d in amazon_deposits if d[0] not in used_amazon_ids]

    print(f'  マッチ成功: {len(matched)} 件')
    print(f'  楽天銀行 未マッチ: {len(unmatched_rakuten)} 件')
    print(f'  Amazon DEPOSIT 未マッチ: {len(unmatched_amazon)} 件')

    # Show date diffs
    date_diffs = [d for _, _, d in matched]
    if date_diffs:
        from collections import Counter
        diff_counts = Counter(date_diffs)
        print(f'  日付差の分布: {dict(sorted(diff_counts.items()))}')

    # Show matches
    print()
    print('=== マッチ詳細 (先頭10件) ===')
    for i, (rak, amazon_dep, diff) in enumerate(matched[:10]):
        print(f'  楽天 id={rak[0]} ({rak[1]}, {abs(rak[2]):,}) ↔ Amazon id={amazon_dep[0]} ({amazon_dep[1]}, {abs(amazon_dep[2]):,}) diff={diff}d')

    if unmatched_rakuten:
        print()
        print('=== 未マッチ 楽天銀行 ===')
        for r in unmatched_rakuten:
            print(f'  id={r[0]}: date={r[1]}, amount={abs(r[2]):,}')

    if unmatched_amazon:
        print()
        print('=== 未マッチ Amazon DEPOSIT ===')
        for d in unmatched_amazon:
            print(f'  id={d[0]}: date={d[1]}, amount={abs(d[2]):,}, settlement={d[4]}')

    if len(matched) == 0:
        print('\nマッチなし。処理を終了します。')
        conn.close()
        return

    # === 振替レコード作成とリンク設定 ===
    cur.execute('SELECT MAX(id) FROM "nc_opau___振替"')
    max_transfer_id = cur.fetchone()[0] or 0
    print(f'\n=== 振替レコード作成 ({len(matched)}件, 現在max_id={max_transfer_id}) ===')

    created_count = 0
    linked_existing = 0

    for rak, amazon_dep, diff in matched:
        rak_id = rak[0]
        rak_date = rak[1]
        rak_amount = abs(rak[2])
        rak_transfer_id = rak[5]
        amazon_id = amazon_dep[0]
        amazon_settlement = amazon_dep[4]

        if rak_transfer_id is not None:
            # Rakuten already has transfer link, just link Amazon side
            cur.execute('''
                UPDATE "nc_opau___Amazon出品アカウント明細"
                SET "nc_opau___振替_id" = ?
                WHERE id = ?
            ''', (rak_transfer_id, amazon_id))
            linked_existing += 1
        else:
            # Create new transfer record
            memo = f'Amazon settlement {amazon_settlement} → 楽天銀行'
            cur.execute('''
                INSERT INTO "nc_opau___振替" ("振替日", "金額", "メモ", created_at, updated_at, created_by)
                VALUES (?, ?, ?, datetime('now'), datetime('now'), 'claude')
            ''', (rak_date, rak_amount, memo))
            transfer_id = cur.lastrowid

            # Link rakuten bank entry
            cur.execute('''
                UPDATE "nc_opau___楽天銀行ビジネス口座入出金明細"
                SET "nc_opau___振替_id" = ?, "振替" = 1
                WHERE id = ?
            ''', (transfer_id, rak_id))

            # Link Amazon deposit entry
            cur.execute('''
                UPDATE "nc_opau___Amazon出品アカウント明細"
                SET "nc_opau___振替_id" = ?
                WHERE id = ?
            ''', (transfer_id, amazon_id))
            created_count += 1

    conn.commit()
    print(f'  新規振替レコード: {created_count} 件')
    print(f'  既存リンクにAmazon追加: {linked_existing} 件')

    # === 検証 ===
    print()
    print('=== 検証 ===')

    cur.execute('''
        SELECT COUNT(*) FROM "nc_opau___楽天銀行ビジネス口座入出金明細"
        WHERE "nc_opau___freee勘定科目_id" = 9 AND "nc_opau___振替_id" IS NOT NULL
    ''')
    print(f'  楽天銀行 Amazon入金 リンク済み: {cur.fetchone()[0]}/45 件')

    cur.execute('''
        SELECT COUNT(*) FROM "nc_opau___Amazon出品アカウント明細"
        WHERE entry_type = 'DEPOSIT' AND "nc_opau___振替_id" IS NOT NULL
    ''')
    print(f'  Amazon DEPOSIT リンク済み: {cur.fetchone()[0]}/{len(amazon_deposits)} 件')

    cur.execute('SELECT COUNT(*) FROM "nc_opau___振替"')
    print(f'  振替テーブル総件数: {cur.fetchone()[0]} 件')

    cur.execute('''
        SELECT SUM(ABS("入出金_円_")) FROM "nc_opau___楽天銀行ビジネス口座入出金明細"
        WHERE "nc_opau___freee勘定科目_id" = 9 AND "nc_opau___振替_id" IS NOT NULL
    ''')
    rakuten_total = cur.fetchone()[0] or 0
    cur.execute('''
        SELECT SUM(ABS("金額")) FROM "nc_opau___Amazon出品アカウント明細"
        WHERE entry_type = 'DEPOSIT' AND "nc_opau___振替_id" IS NOT NULL
    ''')
    amazon_total = cur.fetchone()[0] or 0
    print(f'  楽天リンク済み金額: {rakuten_total:,}')
    print(f'  Amazonリンク済み金額: {amazon_total:,}')
    print(f'  金額一致: {rakuten_total == amazon_total}')

    conn.close()
    print('\n=== Step 2 完了 ===')


if __name__ == '__main__':
    main()
