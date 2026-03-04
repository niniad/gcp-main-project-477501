"""Fix manual_journal id=190: adjust amount from 24,856 to 25,056 to restore MF match"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sqlite3

db_path = 'C:/Users/ninni/nocodb/noco.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Check current value
cur.execute("""
SELECT id, "取引日", "金額", "摘要"
FROM "nc_opau___手動仕訳"
WHERE id = 190
""")
row = cur.fetchone()
print(f'Before: id={row[0]}, date={row[1]}, amount={row[2]}, desc={row[3]}')

# Update: 24,856 -> 25,056 (absorb NTT +200 from Step 0)
cur.execute("""
UPDATE "nc_opau___手動仕訳"
SET "金額" = 25056,
    "摘要" = 'FY2023確定申告値一致調整（BQ-MF記録方法論差+NTT雑費修正分200円）。BQが適切だが申告値が間違っていたための補正。'
WHERE id = 190
""")
conn.commit()

# Verify
cur.execute("""
SELECT id, "取引日", "金額", "摘要"
FROM "nc_opau___手動仕訳"
WHERE id = 190
""")
row = cur.fetchone()
print(f'After:  id={row[0]}, date={row[1]}, amount={row[2]}, desc={row[3]}')
conn.close()
print('Done.')
