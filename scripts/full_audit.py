import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('C:/Users/ninni/nocodb/noco.db')
c = conn.cursor()

print("=" * 70)
print("全テーブル勘定科目監査")
print("=" * 70)

# 1. id=191: 売掛金→Amazon出品アカウント 修正後、この仕訳はまだ正しいか？
print()
print("=== 手動仕訳 id=191 の妥当性 ===")
c.execute('SELECT id, 仕訳日, 借方科目_id, 貸方科目_id, 金額, 摘要 FROM "nc_opau___手動仕訳" WHERE id = 191')
r = c.fetchone()
print(f"  id={r[0]} | {r[1]} | Dr={r[2]} Cr={r[3]} | ¥{r[4]:,}")
print(f"  摘要: {r[5]}")
c.execute('SELECT id, 勘定科目 FROM "nc_opau___freee勘定科目" WHERE id IN (9, 104)')
for r2 in c.fetchall():
    print(f"  科目id={r2[0]}: {r2[1]}")
print("  → Dr.Amazon出品アカウント(9) / Cr.売掛金(104)")
print("  問題: id=164修正後、FY2023に売掛金が存在しないのに")
print("        FY2024でCr.売掛金24,106が発生 → 売掛金がマイナス残高に")

# 2. NTTテーブル: 振替フラグの確認
print()
print("=== NTT 振替フラグ監査 ===")
c.execute('''SELECT n.id, n.利用日, n."ご利用加盟店", n."ご利用金額", n.振替, n."nc_opau___振替_id", a.勘定科目
    FROM "nc_opau___NTTファイナンスBizカード明細" n
    LEFT JOIN "nc_opau___freee勘定科目" a ON n."nc_opau___freee勘定科目_id" = a.id
    WHERE n.振替 = 1
    ORDER BY n.利用日 LIMIT 10''')
print("  振替=1 のエントリ (先頭10):")
for r in c.fetchall():
    print(f"    id={r[0]} | {r[1]} | {r[2]} | ¥{r[3]:,} | acct={r[6]}")

# 3. NTTテーブル: freee勘定科目が未設定のもの
print()
print("=== NTT freee勘定科目 未設定 ===")
c.execute('''SELECT COUNT(*) FROM "nc_opau___NTTファイナンスBizカード明細"
    WHERE "nc_opau___freee勘定科目_id" IS NULL AND (振替 IS NULL OR 振替 = 0)''')
cnt = c.fetchone()[0]
print(f"  未設定(非振替): {cnt} 件")
if cnt > 0:
    c.execute('''SELECT id, 利用日, "ご利用加盟店", "ご利用金額" FROM "nc_opau___NTTファイナンスBizカード明細"
        WHERE "nc_opau___freee勘定科目_id" IS NULL AND (振替 IS NULL OR 振替 = 0) ORDER BY 利用日 LIMIT 10''')
    for r in c.fetchall():
        print(f"    id={r[0]} | {r[1]} | {r[2]} | ¥{r[3]:,}")

# 4. 楽天銀行: freee勘定科目未設定
print()
print("=== 楽天銀行 freee勘定科目 未設定 ===")
c.execute('PRAGMA table_info("nc_opau___楽天銀行明細")')
cols = [r[1] for r in c.fetchall()]
# find the freee column
freee_col = [c2 for c2 in cols if 'freee' in c2]
if freee_col:
    fc = freee_col[0]
    c.execute(f'SELECT COUNT(*) FROM "nc_opau___楽天銀行明細" WHERE "{fc}" IS NULL')
    cnt2 = c.fetchone()[0]
    print(f"  未設定: {cnt2} 件 (カラム: {fc})")

# 5. PayPay銀行: freee勘定科目未設定
print()
print("=== PayPay銀行 freee勘定科目 未設定 ===")
c.execute('PRAGMA table_info("nc_opau___PayPay銀行明細")')
cols = [r[1] for r in c.fetchall()]
freee_col = [c2 for c2 in cols if 'freee' in c2]
if freee_col:
    fc = freee_col[0]
    c.execute(f'SELECT COUNT(*) FROM "nc_opau___PayPay銀行明細" WHERE "{fc}" IS NULL')
    cnt3 = c.fetchone()[0]
    print(f"  未設定: {cnt3} 件 (カラム: {fc})")

# 6. 事業主借: 勘定科目の異常値
print()
print("=== 事業主借 勘定科目チェック ===")
c.execute('PRAGMA table_info("nc_opau___事業主借")')
cols = [r[1] for r in c.fetchall()]
freee_col = [c2 for c2 in cols if 'freee' in c2]
if freee_col:
    fc = freee_col[0]
    c.execute(f'''SELECT t."{fc}", a.勘定科目, COUNT(*) as cnt
        FROM "nc_opau___事業主借" t
        LEFT JOIN "nc_opau___freee勘定科目" a ON t."{fc}" = a.id
        GROUP BY t."{fc}", a.勘定科目
        ORDER BY cnt DESC''')
    for r in c.fetchall():
        print(f"  acct_id={r[0]}: {r[1]} ({r[2]} 件)")

# 7. merchant_account_rules check (BQ side) - we can note this for later
print()
print("=== 手動仕訳 全件の整合性 ===")
c.execute('SELECT id, 仕訳日, 借方科目_id, 貸方科目_id, 金額, 摘要 FROM "nc_opau___手動仕訳" ORDER BY id')
for r in c.fetchall():
    c.execute('SELECT 勘定科目 FROM "nc_opau___freee勘定科目" WHERE id = ?', (r[2],))
    dr = c.fetchone()
    c.execute('SELECT 勘定科目 FROM "nc_opau___freee勘定科目" WHERE id = ?', (r[3],))
    cr = c.fetchone()
    dr_name = dr[0] if dr else '?'
    cr_name = cr[0] if cr else '?'
    print(f"  id={r[0]} | {r[1]} | Dr.{dr_name}({r[2]}) / Cr.{cr_name}({r[3]}) | ¥{r[4]:,}")
    print(f"    {r[5][:80]}")

conn.close()
