"""
Amazon unlinked DEPOSIT 16件の銀行照合（修正版）
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3

DB_PATH = 'C:/Users/ninni/nocodb/noco.db'
RAKUTEN_TABLE = 'nc_opau___\u697d\u5929\u9280\u884c\u30d3\u30b8\u30cd\u30b9\u53e3\u5ea7\u5165\u51fa\u91d1\u660e\u7d30'
PAYPAY_TABLE = 'nc_opau___PayPay\u9280\u884c\u5165\u51fa\u91d1\u660e\u7d30'

UNLINKED = [
    (17,  '2023-03-15', -5664),
    (25,  '2023-03-29', -1912),
    (40,  '2023-04-26', -7576),
    (78,  '2023-06-21', -1129),
    (87,  '2023-07-05', -36318),
    (139, '2023-09-27', -678),
    (208, '2024-01-31', -1680),
    (242, '2024-03-27', -2303),
    (254, '2024-04-24', -8303),
    (272, '2024-06-05', -5749),
    (285, '2024-07-03', -8700),
    (300, '2024-07-31', -15601),
    (539, '2025-07-30', -8139),
    (609, '2025-11-05', -3992),
    (675, '2026-02-11', -20333),
    (685, '2026-02-25', -89455),
]

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Data ranges
cursor.execute(f'SELECT MIN("\u53d6\u5f15\u65e5"), MAX("\u53d6\u5f15\u65e5"), COUNT(*) FROM "{RAKUTEN_TABLE}"')
r_min, r_max, r_cnt = cursor.fetchone()
print(f'楽天データ範囲: {r_min} 〜 {r_max} ({r_cnt}件)')

cursor.execute(f'SELECT MIN("\u64cd\u4f5c\u65e5"), MAX("\u64cd\u4f5c\u65e5"), COUNT(*) FROM "{PAYPAY_TABLE}"')
p_min, p_max, p_cnt = cursor.fetchone()
print(f'PayPayデータ範囲: {p_min} 〜 {p_max} ({p_cnt}件)')
print()

# Check unlinked PayPay entries (without transfer_id, positive amounts)
cursor.execute(f'''
SELECT id, "\u64cd\u4f5c\u65e5", "\u304a\u9810\u304b\u308a\u91d1\u984d", "nc_opau___\u632f\u66ff_id", "nc_opau___freee\u52d8\u5b9a\u79d1\u76ee_id"
FROM "{PAYPAY_TABLE}"
WHERE "nc_opau___\u632f\u66ff_id" IS NULL
  AND "\u304a\u9810\u304b\u308a\u91d1\u984d" > 0
ORDER BY "\u64cd\u4f5c\u65e5"
''')
print('=== 未リンクPayPay入金 ===')
for row in cursor.fetchall():
    print(f'  id={row[0]} {row[1]} ¥{row[2]:,} tr={row[3]} acct={row[4]}')

print()
print('=== Amazon各DEPOSIT銀行照合（±7日, ±1円） ===')
print(f'{"id":>4} {"date":12} {"amount":>8}  楽天照合  PayPay照合')

for amz_id, date, amount in UNLINKED:
    amt_abs = abs(amount)

    # Rakuten: 入出金_円_ (positive=入金)
    cursor.execute(f'''
        SELECT id, "\u53d6\u5f15\u65e5", "\u5165\u51fa\u91d1_\u5186_", "nc_opau___\u632f\u66ff_id"
        FROM "{RAKUTEN_TABLE}"
        WHERE "\u5165\u51fa\u91d1_\u5186_" BETWEEN ? AND ?
          AND date("\u53d6\u5f15\u65e5") BETWEEN date(?, "-7 days") AND date(?, "+7 days")
    ''', (amt_abs - 1, amt_abs + 1, date, date))
    r_matches = cursor.fetchall()

    # PayPay: お預かり金額 (positive=入金)
    cursor.execute(f'''
        SELECT id, "\u64cd\u4f5c\u65e5", "\u304a\u9810\u304b\u308a\u91d1\u984d", "nc_opau___\u632f\u66ff_id"
        FROM "{PAYPAY_TABLE}"
        WHERE "\u304a\u9810\u304b\u308a\u91d1\u984d" BETWEEN ? AND ?
          AND date("\u64cd\u4f5c\u65e5") BETWEEN date(?, "-7 days") AND date(?, "+7 days")
    ''', (amt_abs - 1, amt_abs + 1, date, date))
    p_matches = cursor.fetchall()

    r_info = ', '.join([f'id={r[0]}({r[1]} ¥{r[2]:,} tr={r[3]})' for r in r_matches]) if r_matches else '---'
    p_info = ', '.join([f'id={p[0]}({p[1]} ¥{p[2]:,} tr={p[3]})' for p in p_matches]) if p_matches else '---'

    status = ''
    if r_matches or p_matches:
        status = '← 照合候補あり'

    print(f'{amz_id:>4} {date}  {amount:>8,}  楽:{r_info}  Pay:{p_info}  {status}')

conn.close()
