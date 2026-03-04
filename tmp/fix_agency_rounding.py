"""
Step 2: 代行会社 入金丸め修正
振替リンク済み行の 外貨金額 を transfer_amount / 為替レート の高精度値に更新。
対象: 計算値と振替金額が ±1円以上ずれている行。
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3
from datetime import datetime, timezone

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 1. 修正対象の特定（丸め誤差 ≠ 0）
cursor.execute('''
SELECT a.id, a.外貨金額, a.為替レート, t.金額 as transfer_yen,
       ROUND(a.外貨金額 * a.為替レート, 0) as calc_yen,
       ROUND(a.外貨金額 * a.為替レート, 0) - t.金額 as diff
FROM "nc_opau___代行会社" a
JOIN "nc_opau___振替" t ON a.nc_opau___振替_id = t.id
WHERE a.nc_opau___振替_id IS NOT NULL
  AND a.外貨金額 > 0
ORDER BY a.id
''')
rows = cursor.fetchall()

print('=== 全振替リンク済み行（外貨金額 > 0） ===')
print(f'{"id":>5} {"外貨金額":>12} {"為替レート":>10} {"振替円":>8} {"計算円":>8} {"差":>5}')
mismatches = []
for row in rows:
    id_, foreign, rate, transfer, calc, diff = row
    marker = ' ← 要修正' if diff != 0 else ''
    print(f'{id_:>5} {foreign:>12} {rate:>10.4f} {transfer:>8} {calc:>8.0f} {diff:>5.0f}{marker}')
    if diff != 0:
        mismatches.append(row)

print(f'\n修正対象: {len(mismatches)} 件')

# 2. 高精度値で更新
now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S+00:00')
for row in mismatches:
    id_, foreign, rate, transfer, calc, diff = row
    new_foreign = transfer / rate  # 高精度計算
    print(f'\nid={id_}: 外貨金額 {foreign} → {new_foreign:.10f}')
    print(f'  検証: {new_foreign:.10f} * {rate} = {new_foreign * rate:.6f} (振替={transfer})')

    cursor.execute('''
        UPDATE "nc_opau___代行会社"
        SET 外貨金額 = ?, updated_at = ?
        WHERE id = ?
    ''', (new_foreign, now, id_))

# 3. 更新後確認
cursor.execute('''
SELECT a.id, a.外貨金額, a.為替レート, t.金額 as transfer_yen,
       ROUND(a.外貨金額 * a.為替レート, 0) as calc_yen,
       ROUND(a.外貨金額 * a.為替レート, 0) - t.金額 as diff
FROM "nc_opau___代行会社" a
JOIN "nc_opau___振替" t ON a.nc_opau___振替_id = t.id
WHERE a.nc_opau___振替_id IS NOT NULL
  AND a.外貨金額 > 0
ORDER BY a.id
''')
rows_after = cursor.fetchall()

print('\n=== 更新後確認 ===')
all_zero = True
for row in rows_after:
    id_, foreign, rate, transfer, calc, diff = row
    status = '✅' if diff == 0 else f'❌ diff={diff}'
    if diff != 0:
        all_zero = False
    print(f'id={id_}: {foreign:.10f} × {rate} = {calc:.0f} vs {transfer} {status}')

conn.commit()
conn.close()

if all_zero:
    print('\n✅ 全件 diff=0 確認完了。BQ syncへ進む。')
else:
    print('\n⚠️ まだ差異あり。確認が必要。')
