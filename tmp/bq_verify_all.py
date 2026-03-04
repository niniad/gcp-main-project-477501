import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
import csv

client = bigquery.Client(project='main-project-477501')

# === MF FY2023 data ===
mf_2023 = {}
with open('C:/Users/ninni/projects/rawdata/マネーフォワード/MF_総勘定元帳_2023年度.csv', 'r', encoding='shift_jis') as f:
    reader = csv.DictReader(f)
    for row in reader:
        acct = row.get('勘定科目', '')
        if not acct:
            continue
        bal_str = row.get('残高', '0').replace(',', '')
        try:
            mf_2023[acct] = int(bal_str)
        except:
            pass

# MF account mapping to BQ
MF_TO_BQ_BS = {
    '預け金': 'ESPRIME',
    '普通預金': '楽天銀行',
    '商品': '商品',
    '開業費': '開業費',
}

MF_TO_BQ_PL = {
    '売上高': '売上高',
    'Amazon手数料': None,  # BQ splits into multiple
    '仕入高': '仕入高',
    '外注工賃': '外注費',
    '支払手数料': '支払手数料',
    '新聞図書費': '新聞図書費',
    '消耗品費': '消耗品費',
    '研修採用費': '研修採用費',
    '研究開発費': '研究開発費',
    '通信費': '通信費',
    '雑収入': '雑収入',
    '雑費': '雑費',
}

# === BQ FY2023 BS balances ===
query_bs = """
SELECT account_name,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year <= 2023
GROUP BY account_name
"""
bq_bs_2023 = {}
for row in client.query(query_bs).result():
    bq_bs_2023[row.account_name] = row.balance

# === BQ FY2023 PL ===
query_pl = """
SELECT account_name,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) AS amount
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year = 2023
GROUP BY account_name
"""
bq_pl_2023 = {}
for row in client.query(query_pl).result():
    bq_pl_2023[row.account_name] = row.amount

# === FY2023 BS Comparison ===
print("=" * 70)
print("FY2023年末 BS残高 比較 (BQ vs MF)")
print("=" * 70)
print(f"{'科目':20s} {'BQ':>12s} {'MF':>12s} {'差異':>10s} {'備考'}")
print("-" * 70)

bs_accounts = ['ESPRIME', '楽天銀行', '商品', '開業費', 'Amazon出品アカウント', '売掛金', '未払金', '事業主借', '事業主貸', 'YP', 'THE直行便', 'PayPay銀行', '仮払金']
mf_rev = {v: k for k, v in MF_TO_BQ_BS.items()}

for acct in bs_accounts:
    bq_val = bq_bs_2023.get(acct, 0)
    mf_key = mf_rev.get(acct, acct)
    mf_val = mf_2023.get(mf_key)

    if mf_val is not None:
        # MF shows debit-normal balances as positive
        # BQ: debit-credit, so assets positive, liabilities negative
        # For comparison: MF事業主借 is shown as positive (credit normal = liability)
        # BQ事業主借 is negative (credit balance)
        if acct == '事業主借':
            diff = abs(bq_val) - mf_val
        elif acct == '未払金':
            diff = abs(bq_val) - mf_val
        elif acct == '売掛金':
            diff = bq_val - (-mf_val)  # MF shows negative for credit balance
        else:
            diff = bq_val - mf_val

        mark = "✓" if diff == 0 else f"差 {diff:,}"
        print(f"  {acct:18s} {bq_val:>12,} {mf_val:>12,} {mark}")
    else:
        note = "MFになし"
        if acct == 'Amazon出品アカウント':
            note = "MF: 売掛金に含む（構造差）"
        elif acct in ('YP', 'THE直行便', 'PayPay銀行', '仮払金', '事業主貸'):
            note = f"BQ={bq_val:,} (MF該当科目なし)"
        print(f"  {acct:18s} {bq_val:>12,} {'--':>12s} {note}")

# === FY2023 PL Net ===
print()
print("=" * 70)
print("FY2023 PL 合計比較")
print("=" * 70)
pl_accounts = [a for a in bq_pl_2023.keys() if a not in bs_accounts]
bq_pl_total = sum(bq_pl_2023.get(a, 0) for a in pl_accounts)
mf_pl_total = sum(v for k, v in mf_2023.items() if k not in ('預け金', '普通預金', '商品', '開業費', '売掛金', '未払金', '事業主借'))
# MF: expenses positive, revenue positive → need to compute net loss
mf_expenses = sum(v for k, v in mf_2023.items() if k not in ('預け金', '普通預金', '商品', '開業費', '売掛金', '未払金', '事業主借', '売上高', '雑収入'))
mf_revenue = mf_2023.get('売上高', 0) + mf_2023.get('雑収入', 0)
mf_net = mf_expenses - mf_revenue

print(f"  BQ PL合計(当期純損失): {bq_pl_total:>12,}")
print(f"  MF PL合計(当期純損失): {mf_net:>12,}")
print(f"  差異:                  {bq_pl_total - mf_net:>12,} {'✓' if bq_pl_total == mf_net else '要確認'}")

# === FY2024 BS ===
query_bs_2024 = """
SELECT account_name,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
WHERE fiscal_year <= 2024
GROUP BY account_name
"""
bq_bs_2024 = {}
for row in client.query(query_bs_2024).result():
    bq_bs_2024[row.account_name] = row.balance

print()
print("=" * 70)
print("FY2024年末 BS残高 (BQ)")
print("=" * 70)
for acct in bs_accounts:
    bq_val = bq_bs_2024.get(acct, 0)
    print(f"  {acct:20s} {bq_val:>12,}")

# Key FY2024 checks
print()
print("=== FY2024 主要チェック ===")
print(f"  ESPRIME:   {bq_bs_2024.get('ESPRIME', 0):>10,}  (実態: 200,657)")
esprime_ok = bq_bs_2024.get('ESPRIME', 0) == 200657
print(f"             {'✓ 一致' if esprime_ok else '× 不一致'}")
