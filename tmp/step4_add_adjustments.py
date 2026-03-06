"""
Step 4: FY2023/2024 年度末 手動仕訳調整

BQ sync 完了後に実行。
月次三分法のSP-API計算値をMF確定申告値に合わせるための調整。

調整内容:
- FY2023-12-31: Dr.商品(17) / Cr.仕入高(109) → 期末棚卸を93,389に合わせる
- FY2024-12-31: Dr.仕入高(109) / Cr.商品(17) → 期末棚卸を0に合わせる
"""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
SHOHIN_ID = 17    # 商品（nocodb account_items id）
SHIIRE_ID = 109   # 仕入高

MF_FY2023_ENDING = 93389   # MF確定申告 FY2023期末棚卸
MF_FY2024_ENDING = 0       # MF確定申告 FY2024期末棚卸

# ==========================================
# BQ から FY2023/2024 の 商品 残高を取得
# ==========================================
print('=== BQ: 商品残高確認（月次三分法ベース）===')
q = """
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS net
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '\u5546\u54c1'
GROUP BY 1 ORDER BY 1
"""
cumulative = 0
fy_net = {}
for r in client.query(q).result():
    cumulative += r.net
    fy_net[r.fiscal_year] = {'net': r.net, 'cum': cumulative}
    print(f'  FY{r.fiscal_year}: 当年¥{r.net:,.0f}  累積¥{cumulative:,.0f}')

fy2023_cum = fy_net.get(2023, {}).get('cum', 0)
fy2024_cum = fy_net.get(2024, {}).get('cum', 0)

print(f'\nFY2023 商品累積残高（SP-API）: ¥{fy2023_cum:,.0f}')
print(f'FY2023 MF確定値:              ¥{MF_FY2023_ENDING:,.0f}')
adj_2023 = MF_FY2023_ENDING - fy2023_cum
print(f'→ FY2023調整額: ¥{adj_2023:,.0f}', end='')
if adj_2023 > 0:
    print(' (Dr.商品 / Cr.仕入高)')
elif adj_2023 < 0:
    print(' (Dr.仕入高 / Cr.商品)')
else:
    print(' (調整不要)')

# FY2024は adj_2023を加味した累積
fy2024_cum_with_adj = fy2024_cum + adj_2023
print(f'\nFY2024 商品累積残高（SP-API + FY2023調整後）: ¥{fy2024_cum_with_adj:,.0f}')
print(f'FY2024 MF確定値: ¥{MF_FY2024_ENDING:,.0f}')
adj_2024 = MF_FY2024_ENDING - fy2024_cum_with_adj
print(f'→ FY2024調整額: ¥{adj_2024:,.0f}', end='')
if adj_2024 > 0:
    print(' (Dr.商品 / Cr.仕入高)')
elif adj_2024 < 0:
    print(' (Dr.仕入高 / Cr.商品)')
else:
    print(' (調整不要)')

# ==========================================
# 確認
# ==========================================
if adj_2023 == 0 and adj_2024 == 0:
    print('\n調整不要。終了。')
    sys.exit(0)

print('\n上記内容で手動仕訳を NocoDB に追加します。続行しますか？ [y/N] ', end='')
ans = input().strip().lower()
if ans != 'y':
    print('キャンセル')
    sys.exit(0)

# ==========================================
# NocoDB に手動仕訳を追加
# ==========================================
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 既存の在庫調整エントリを確認・削除（再実行時の重複防止）
cur.execute("""
    SELECT id FROM "nc_opau___手動仕訳"
    WHERE 摘要 LIKE '%棚卸調整%' AND (仕訳日 = '2023-12-31' OR 仕訳日 = '2024-12-31')
""")
existing = cur.fetchall()
if existing:
    ids = [r[0] for r in existing]
    print(f'既存の棚卸調整仕訳を削除: {ids}')
    cur.execute(f'DELETE FROM "nc_opau___手動仕訳" WHERE id IN ({",".join(str(i) for i in ids)})')

# 最大IDを取得
cur.execute('SELECT MAX(id) FROM "nc_opau___手動仕訳"')
max_id = cur.fetchone()[0] or 200

entries_added = []

# FY2023 調整
if adj_2023 != 0:
    new_id = max_id + 1
    if adj_2023 > 0:
        dr_id, cr_id = SHOHIN_ID, SHIIRE_ID
        label = 'Dr.\u5546\u54c1/Cr.\u4ed5\u5165\u9ad8'
    else:
        dr_id, cr_id = SHIIRE_ID, SHOHIN_ID
        label = 'Dr.\u4ed5\u5165\u9ad8/Cr.\u5546\u54c1'
    cur.execute("""
        INSERT INTO "nc_opau___手動仕訳"
        (id, nc_order, 仕訳日, 借方科目_id, 貸方科目_id, 金額, 摘要,
         "nc_opau___freee勘定科目_id", "nc_opau___freee勘定科目_id1",
         created_at, updated_at)
        VALUES (?, ?, '2023-12-31', ?, ?, ?, ?, ?, ?,
                datetime('now'), datetime('now'))
    """, (new_id, float(new_id), dr_id, cr_id, abs(adj_2023),
          f'FY2023期末棚卸調整（MF確定値¥93,389との差額）{label}',
          dr_id, cr_id))
    entries_added.append(f'FY2023: {label} ¥{abs(adj_2023):,}')
    max_id = new_id

# FY2024 調整
if adj_2024 != 0:
    new_id = max_id + 1
    if adj_2024 > 0:
        dr_id, cr_id = SHOHIN_ID, SHIIRE_ID
        label = 'Dr.\u5546\u54c1/Cr.\u4ed5\u5165\u9ad8'
    else:
        dr_id, cr_id = SHIIRE_ID, SHOHIN_ID
        label = 'Dr.\u4ed5\u5165\u9ad8/Cr.\u5546\u54c1'
    cur.execute("""
        INSERT INTO "nc_opau___手動仕訳"
        (id, nc_order, 仕訳日, 借方科目_id, 貸方科目_id, 金額, 摘要,
         "nc_opau___freee勘定科目_id", "nc_opau___freee勘定科目_id1",
         created_at, updated_at)
        VALUES (?, ?, '2024-12-31', ?, ?, ?, ?, ?, ?,
                datetime('now'), datetime('now'))
    """, (new_id, float(new_id), dr_id, cr_id, abs(adj_2024),
          f'FY2024期末棚卸調整（MF確定値¥0との差額）{label}',
          dr_id, cr_id))
    entries_added.append(f'FY2024: {label} ¥{abs(adj_2024):,}')

conn.commit()
conn.close()

print('\n追加完了:')
for e in entries_added:
    print(f'  {e}')

print('\n>>> 次: BQ sync を実行してから検証 ===')
print('    cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py')
