"""
Step 1: Amazon出品アカウント明細テーブルの作成とデータ投入

1. NocoDB SQLite にテーブル作成
2. BQ settlement_journal_payload_view からデータ取得
3. freee account ID → NocoDB account ID にマッピング
4. SQLite にINSERT
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
from google.cloud import bigquery

SQLITE_PATH = "C:/Users/ninni/nocodb/noco.db"
BQ_PROJECT = "main-project-477501"

# freee account_item_id → NocoDB account_items.nocodb_id
FREEE_TO_NOCODB = {
    1008403397: 9,    # Amazon出品アカウント
    786598216: 32,    # 仮払金
    786598329: 146,   # 地代家賃
    786598269: 100,   # 売上値引高
    786598270: 101,   # 売上戻り高
    786598267: 99,    # 売上高
    786598298: 125,   # 広告宣伝費
    786598332: 148,   # 支払手数料
    786598290: 119,   # 荷造運賃
    786598354: 156,   # 諸会費
    786598349: 126,   # 販売手数料
    786598277: 104,   # 雑収入
}

def main():
    # === BQ からデータ取得 ===
    print('=== BQ からデータ取得 ===')
    bq_client = bigquery.Client(project=BQ_PROJECT)

    q = """
    SELECT
      s.settlement_id,
      DATE(s.issue_date) as deposit_date,
      d.entry_side,
      d.account_item_id as freee_account_id,
      COALESCE(am.account_name_debug, CAST(d.account_item_id AS STRING)) AS account_name,
      CAST(d.amount AS INT64) as amount,
      d.tax_code,
      d.description
    FROM `main-project-477501.accounting.settlement_journal_payload_view` s
    CROSS JOIN UNNEST(s.json_details) AS d
    LEFT JOIN (
      SELECT DISTINCT account_item_id, account_name_debug
      FROM `main-project-477501.accounting.account_map`
    ) am ON d.account_item_id = am.account_item_id
    ORDER BY deposit_date, s.settlement_id, d.entry_side DESC, d.description
    """
    rows = list(bq_client.query(q).result())
    print(f'  取得件数: {len(rows)}')

    # === 口座視点に変換 ===
    # Amazon出品アカウント（口座）として:
    # - 収入（credit側、口座に入る）: 売上高 → 口座に+、売上値引/売上戻り → 口座に-
    # - 費用（debit側、口座から出る）: 手数料等 → 口座から-
    # - DEPOSIT（Amazon Settlement Net）: 口座から- (銀行への送金)
    #
    # 現在のVIEWでは settlement ごとに debit/credit の仕訳明細がある。
    # NocoDB口座テーブルでは「口座の入出金」として記録する。
    #
    # 変換ルール:
    # - entry_side='credit' (売上等): 金額=+ (口座に入金)
    # - entry_side='debit' (手数料等): 金額=- (口座から出金)
    # - Amazon Settlement Net (DEPOSIT): entry_side='credit'だがこれは
    #   口座から銀行への送金なので 金額=- (口座から出金) → 振替
    #
    # ただし、現在のBQ VIEWの構造を見ると:
    # - 「Amazon出品アカウント」account_nameの行 = settlement net (銀行送金額)
    #   entry_side='credit' → これは口座側のcredit記入
    # - 他の行 = 売上や手数料（相手勘定側）
    #
    # 口座テーブルでは各行を「口座の入出金」として記録:
    # - 売上(credit): 口座に入る → 金額=+
    # - 手数料(debit): 口座から出る → 金額=-
    # - Settlement Net(credit, account=Amazon出品アカウント): 銀行送金 → 金額=- (振替)

    records = []
    for row in rows:
        nocodb_account_id = FREEE_TO_NOCODB.get(row.freee_account_id)
        if nocodb_account_id is None:
            print(f'  WARNING: Unknown freee account {row.freee_account_id} ({row.account_name})')
            continue

        is_deposit = (row.account_name == 'Amazon出品アカウント')

        if is_deposit:
            # DEPOSIT: 口座から銀行への送金（振替）
            # 口座視点: マイナス（出金）
            amount_account_perspective = -row.amount
            entry_type = 'DEPOSIT'
        elif row.entry_side == 'credit':
            # 売上等: 口座に入金
            amount_account_perspective = row.amount
            entry_type = 'REVENUE'
        else:
            # 手数料等: 口座から出金
            amount_account_perspective = -row.amount
            entry_type = 'EXPENSE'

        records.append({
            'settlement_id': str(row.settlement_id),
            'deposit_date': str(row.deposit_date),
            'amount': amount_account_perspective,
            'description': f'settlement {row.settlement_id}: {row.description}',
            'entry_type': entry_type,
            'nocodb_account_id': nocodb_account_id,
            'tax_code': row.tax_code,
        })

    print(f'  変換後レコード数: {len(records)}')

    # 検証: 口座の入出金合計（DEPOSIT以外の合計 = 口座残高変動、DEPOSIT合計 = 銀行送金額）
    non_deposit_sum = sum(r['amount'] for r in records if r['entry_type'] != 'DEPOSIT')
    deposit_sum = sum(r['amount'] for r in records if r['entry_type'] == 'DEPOSIT')
    total_sum = sum(r['amount'] for r in records)
    print(f'  非DEPOSIT合計: {non_deposit_sum:,} (口座への純入金)')
    print(f'  DEPOSIT合計: {deposit_sum:,} (銀行送金)')
    print(f'  全体合計: {total_sum:,} (口座残高変動)')

    # === SQLite テーブル作成 ===
    print()
    print('=== NocoDB SQLite テーブル作成 ===')
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()

    # テーブルが既に存在する場合は削除
    cur.execute('DROP TABLE IF EXISTS "nc_opau___Amazon出品アカウント明細"')

    create_sql = """
    CREATE TABLE "nc_opau___Amazon出品アカウント明細" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at datetime DEFAULT (datetime('now')),
        updated_at datetime DEFAULT (datetime('now')),
        created_by varchar DEFAULT 'claude',
        updated_by varchar,
        nc_order REAL,
        "取引日" date,
        "金額" INTEGER,
        "摘要" TEXT,
        "settlement_id" TEXT,
        "entry_type" TEXT,
        "nc_opau___振替_id" INTEGER,
        "nc_opau___freee勘定科目_id" INTEGER,
        "品目" varchar,
        "税区分" TEXT
    )
    """
    cur.execute(create_sql)
    print('  テーブル作成完了')

    # === データ投入 ===
    print()
    print('=== データ投入 ===')
    insert_sql = """
    INSERT INTO "nc_opau___Amazon出品アカウント明細"
        ("取引日", "金額", "摘要", "settlement_id", "entry_type",
         "nc_opau___freee勘定科目_id", "税区分", nc_order)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    for i, rec in enumerate(records):
        tax_str = str(rec['tax_code']) if rec['tax_code'] is not None else None
        cur.execute(insert_sql, (
            rec['deposit_date'],
            rec['amount'],
            rec['description'],
            rec['settlement_id'],
            rec['entry_type'],
            rec['nocodb_account_id'],
            tax_str,
            float(i + 1),
        ))

    conn.commit()
    print(f'  {len(records)} 件挿入完了')

    # === 検証 ===
    print()
    print('=== 検証 ===')
    cur.execute('SELECT COUNT(*) FROM "nc_opau___Amazon出品アカウント明細"')
    count = cur.fetchone()[0]
    print(f'  テーブル行数: {count}')

    cur.execute('SELECT SUM("金額") FROM "nc_opau___Amazon出品アカウント明細"')
    total = cur.fetchone()[0]
    print(f'  金額合計: {total:,}')

    cur.execute('''
        SELECT entry_type, COUNT(*), SUM("金額")
        FROM "nc_opau___Amazon出品アカウント明細"
        GROUP BY entry_type
    ''')
    print()
    print('  entry_type別:')
    for row in cur.fetchall():
        print(f'    {row[0]}: {row[1]}件, 合計={row[2]:,}')

    # DEPOSIT行は後で振替リンクを設定する
    cur.execute('''
        SELECT COUNT(*) FROM "nc_opau___Amazon出品アカウント明細"
        WHERE entry_type = 'DEPOSIT'
    ''')
    deposit_count = cur.fetchone()[0]
    print(f'\n  DEPOSIT行（Step 2で振替リンク設定予定）: {deposit_count}件')

    conn.close()
    print('\n=== Step 1 完了 ===')


if __name__ == '__main__':
    main()
