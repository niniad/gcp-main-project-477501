"""
NTTファイナンスBizカード 完全突合スクリプト
NTT生明細 vs MF総勘定元帳 vs BQ/NocoDB の3点突合
未払金が最終的にゼロになるかを検証
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import csv
import re
from pathlib import Path
from collections import defaultdict

# ============================================================
# Part 1: NTT生明細の読み込み
# ============================================================
raw_dir = Path(r"c:\Users\ninni\projects\rawdata\NTTファイナンスBizカード明細")
all_raw_entries = []

for csv_file in sorted(raw_dir.glob("MYLINK_*.csv")):
    billing_month = csv_file.stem.replace("MYLINK_", "")

    content = None
    for enc in ['utf-8-sig', 'utf-8', 'cp932', 'shift_jis']:
        try:
            content = csv_file.read_text(encoding=enc)
            if '利用' in content or 'カード' in content or '支払' in content:
                break
            content = None
        except:
            content = None

    if content is None:
        continue

    lines = content.strip().split('\n')
    first_line = lines[0].strip().replace('\ufeff', '')

    if first_line.startswith('利用日'):
        # Simple UTF-8 format
        reader = csv.reader(lines)
        next(reader)
        for row in reader:
            if len(row) >= 4 and row[0].strip() and row[3].strip():
                try:
                    all_raw_entries.append({
                        'date': row[0].strip(),
                        'merchant': row[1].strip(),
                        'amount': int(row[3].strip()),
                        'billing_month': billing_month,
                    })
                except ValueError:
                    pass
    else:
        # Detailed cp932 format
        for line in lines:
            parts = line.split(',')
            if len(parts) >= 4 and re.match(r'^\d{8}$', parts[0].strip()):
                try:
                    d = parts[0].strip()
                    all_raw_entries.append({
                        'date': f"{d[:4]}-{d[4:6]}-{d[6:8]}",
                        'merchant': parts[1].strip(),
                        'amount': int(parts[3].strip()),
                        'billing_month': billing_month,
                    })
                except ValueError:
                    pass

# Sort by date
all_raw_entries.sort(key=lambda x: x['date'])
raw_total = sum(e['amount'] for e in all_raw_entries)

print("=" * 110)
print("【1】NTT生明細 全取引一覧")
print("    ※ MYLINK_202307.csv が存在しない（PDFのみ） → 6月利用分が欠落")
print("=" * 110)
print(f"\n{'利用日':>12} {'金額':>8} {'加盟店名':<45} {'請求月':>8}")
print("-" * 85)
for e in all_raw_entries:
    print(f"{e['date']:>12} {e['amount']:>8,} {e['merchant']:<45} {e['billing_month']:>8}")
print(f"\nNTT生明細合計: {raw_total:,}円 ({len(all_raw_entries)}件)")
print("※ 202307(6月利用分)が欠落しているため、実際の総額はこれより大きい")

# ============================================================
# Part 2: MF総勘定元帳から未払金セクションを抽出
# ============================================================
mf_dir = Path(r"c:\Users\ninni\projects\rawdata\マネーフォワード")
mf_charges = []  # 貸方(利用)
mf_payments = []  # 借方(支払)

for fy, fname in [(2023, "MF_総勘定元帳_2023年度.csv"),
                   (2024, "MF_総勘定元帳_2024年度.csv"),
                   (2025, "MF_総勘定元帳_2025年度(途中).csv")]:
    fpath = mf_dir / fname
    if not fpath.exists():
        continue

    content = fpath.read_text(encoding='cp932')
    for line in content.split('\n'):
        row = list(csv.reader([line]))[0] if line.strip() else []
        if len(row) < 11:
            continue

        # Column 2 = main account. We want rows where main account = "未払金"
        main_account = row[2].strip() if len(row) > 2 else ""
        if main_account != '未払金':
            continue

        date_str = row[1].strip()
        if not re.match(r'\d{4}/\d{1,2}/\d{1,2}', date_str):
            continue

        sub_account = row[3].strip()  # NTTグループカード... or 楽天カード...
        partner_account = row[5].strip()
        description = row[8].strip() if len(row) > 8 else ""

        debit_str = row[9].strip().replace(',', '').replace('"', '') if len(row) > 9 and row[9] else "0"
        credit_str = row[10].strip().replace(',', '').replace('"', '') if len(row) > 10 and row[10] else "0"

        try:
            debit = int(debit_str) if debit_str else 0
            credit = int(credit_str) if credit_str else 0
        except ValueError:
            continue

        # Identify card type from sub-account
        is_rakuten = '楽天カード' in sub_account or '楽天ゴールド' in sub_account
        card_type = '楽天カード' if is_rakuten else 'NTTカード'

        entry = {
            'date': date_str.replace('/', '-'),
            'description': description,
            'partner': partner_account,
            'sub_account': sub_account[:30],
            'card_type': card_type,
            'fy': fy,
        }

        if credit > 0:
            entry['amount'] = credit
            entry['side'] = '貸方(利用)'
            mf_charges.append(entry)
        if debit > 0:
            entry = dict(entry)
            entry['amount'] = debit
            entry['side'] = '借方(支払)'
            mf_payments.append(entry)

# Separate NTT charges from 楽天カード charges
mf_ntt_charges = [e for e in mf_charges if e['card_type'] == 'NTTカード']
mf_rakuten_charges = [e for e in mf_charges if e['card_type'] == '楽天カード']

mf_ntt_total = sum(e['amount'] for e in mf_ntt_charges)
mf_rakuten_total = sum(e['amount'] for e in mf_rakuten_charges)
mf_payment_total = sum(e['amount'] for e in mf_payments)

print(f"\n\n{'=' * 110}")
print("【2】MF未払金セクション — カード種別判定")
print("=" * 110)

print(f"\n■ NTTカード利用（正当な未払金）: {len(mf_ntt_charges)}件, 合計 {mf_ntt_total:,}円")
print(f"■ 楽天カード利用（誤分類の疑い）: {len(mf_rakuten_charges)}件, 合計 {mf_rakuten_total:,}円")
print(f"■ 支払い（銀行引落し）: {len(mf_payments)}件, 合計 {mf_payment_total:,}円")

if mf_rakuten_charges:
    print(f"\n  ★ 楽天カード誤分類の詳細:")
    for e in mf_rakuten_charges:
        print(f"    {e['date']} {e['amount']:>8,} {e['description']:<40} FY{e['fy']}")
    print(f"    → これらは「事業主借」であるべき（個人カードでの事業経費）")

# ============================================================
# Part 3: BQ/NocoDB NTT charges
# ============================================================
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

q_ntt = """
SELECT
    nocodb_id,
    usage_date,
    merchant_name,
    ABS(usage_amount) AS usage_amount,
    payment_date,
    payment_amount
FROM `main-project-477501.nocodb.ntt_finance_statements`
ORDER BY usage_date
"""
bq_rows = list(client.query(q_ntt).result())
bq_total = sum(r.usage_amount for r in bq_rows)

print(f"\n\n{'=' * 110}")
print(f"【3】データソース別合計")
print("=" * 110)
print(f"\n  NTT生明細(CSV):      {raw_total:>10,}円  ({len(all_raw_entries)}件) ※202307欠落")
print(f"  MF NTTカード利用:    {mf_ntt_total:>10,}円  ({len(mf_ntt_charges)}件)")
print(f"  MF 楽天カード誤混入: {mf_rakuten_total:>10,}円  ({len(mf_rakuten_charges)}件)")
print(f"  MF 未払金合計:       {mf_ntt_total + mf_rakuten_total:>10,}円")
print(f"  BQ/NocoDB:           {bq_total:>10,}円  ({len(bq_rows)}件)")

# ============================================================
# Part 4: 月別3点突合
# ============================================================
def month_key(date_str):
    return date_str[:7].replace('-', '')

raw_by_month = defaultdict(lambda: {'total': 0, 'entries': []})
for e in all_raw_entries:
    ym = month_key(e['date'])
    raw_by_month[ym]['total'] += e['amount']
    raw_by_month[ym]['entries'].append(e)

mf_by_month = defaultdict(lambda: {'total': 0, 'entries': []})
for e in mf_ntt_charges:
    ym = month_key(e['date'])
    mf_by_month[ym]['total'] += e['amount']
    mf_by_month[ym]['entries'].append(e)

bq_by_month = defaultdict(lambda: {'total': 0, 'entries': []})
for r in bq_rows:
    ym = str(r.usage_date)[:7].replace('-', '')
    bq_by_month[ym]['total'] += r.usage_amount
    bq_by_month[ym]['entries'].append(r)

all_months = sorted(set(list(raw_by_month.keys()) + list(mf_by_month.keys()) + list(bq_by_month.keys())))

print(f"\n\n{'=' * 110}")
print("【4】利用月別 3点突合 (NTTカードのみ、楽天カード除外)")
print("=" * 110)
print(f"\n{'利用月':>8} {'NTT生':>10} {'MF(NTT)':>10} {'BQ':>10} {'NTT-MF':>8} {'NTT-BQ':>8} {'MF-BQ':>8}")
print("-" * 75)

diff_months = []
for ym in all_months:
    r = raw_by_month[ym]['total']
    m = mf_by_month[ym]['total']
    b = bq_by_month[ym]['total']

    r_s = f"{r:>10,}" if r else "         -"
    m_s = f"{m:>10,}" if m else "         -"
    b_s = f"{b:>10,}" if b else "         -"

    d_rm = r - m
    d_rb = r - b
    d_mb = m - b

    drm = f"{d_rm:>8,}" if d_rm != 0 else "       ✓"
    drb = f"{d_rb:>8,}" if d_rb != 0 else "       ✓"
    dmb = f"{d_mb:>8,}" if d_mb != 0 else "       ✓"

    has_diff = (d_rm != 0 or d_rb != 0 or d_mb != 0)
    print(f"{ym:>8} {r_s} {m_s} {b_s} {drm} {drb} {dmb}")
    if has_diff:
        diff_months.append(ym)

print("-" * 75)
raw_sum = sum(raw_by_month[ym]['total'] for ym in all_months)
mf_sum = sum(mf_by_month[ym]['total'] for ym in all_months)
bq_sum = sum(bq_by_month[ym]['total'] for ym in all_months)
print(f"{'合計':>8} {raw_sum:>10,} {mf_sum:>10,} {bq_sum:>10,} {raw_sum-mf_sum:>8,} {raw_sum-bq_sum:>8,} {mf_sum-bq_sum:>8,}")

# ============================================================
# Part 5: 差異月の詳細
# ============================================================
print(f"\n\n{'=' * 110}")
print("【5】差異がある月の詳細比較")
print("=" * 110)

for ym in diff_months:
    r_ents = raw_by_month[ym]['entries']
    m_ents = mf_by_month[ym]['entries']
    b_ents = bq_by_month[ym]['entries']

    r_total = sum(e['amount'] for e in r_ents)
    m_total = sum(e['amount'] for e in m_ents)
    b_total = sum(e.usage_amount for e in b_ents)

    print(f"\n--- {ym} (NTT: {r_total:,}, MF: {m_total:,}, BQ: {b_total:,}) ---")

    # Match NTT vs MF by amount
    raw_set = [(e['date'], e['amount'], e['merchant']) for e in r_ents]
    mf_set = [(e['date'], e['amount'], e['description']) for e in m_ents]
    bq_set = [(str(e.usage_date), e.usage_amount, e.merchant_name) for e in b_ents]

    # NTT vs MF matching
    r_rem = list(raw_set)
    m_rem = list(mf_set)
    for item in list(raw_set):
        for mf_item in m_rem:
            if item[1] == mf_item[1]:
                if item in r_rem:
                    r_rem.remove(item)
                m_rem.remove(mf_item)
                break

    if r_rem:
        print(f"  NTT生にあるがMFにない:")
        for d, a, m in r_rem:
            print(f"    {d} {a:>8,} {m}")
    if m_rem:
        print(f"  MFにあるがNTT生にない:")
        for d, a, m in m_rem:
            print(f"    {d} {a:>8,} {m}")

    # NTT vs BQ matching
    r_rem2 = list(raw_set)
    b_rem = list(bq_set)
    for item in list(raw_set):
        for bq_item in b_rem:
            if item[1] == bq_item[1]:
                if item in r_rem2:
                    r_rem2.remove(item)
                b_rem.remove(bq_item)
                break

    if r_rem2:
        print(f"  NTT生にあるがBQにない:")
        for d, a, m in r_rem2:
            print(f"    {d} {a:>8,} {m}")
    if b_rem:
        print(f"  BQにあるがNTT生にない:")
        for d, a, m in b_rem:
            print(f"    {d} {a:>8,} {m}")

# ============================================================
# Part 6: 銀行支払いとの照合
# ============================================================
print(f"\n\n{'=' * 110}")
print("【6】銀行支払い（楽天銀行 NTT引落し）")
print("=" * 110)

q_bank = """
SELECT transaction_date, amount_jpy, counterparty_description
FROM `main-project-477501.nocodb.rakuten_bank_statements`
WHERE counterparty_description LIKE '%NTT%'
   OR counterparty_description LIKE '%ＮＴＴ%'
ORDER BY transaction_date
"""
bank_rows = list(client.query(q_bank).result())

print(f"\n{'日付':>12} {'金額':>10} {'摘要'}")
print("-" * 60)
bank_total = 0
for r in bank_rows:
    amt = abs(r.amount_jpy)
    bank_total += amt
    print(f"{r.transaction_date:>12} {amt:>10,} {r.counterparty_description}")
print(f"\n銀行支払い合計: {bank_total:,}円 ({len(bank_rows)}件)")

# ============================================================
# Part 7: 未払金残高の検証
# ============================================================
print(f"\n\n{'=' * 110}")
print("【7】未払金残高シミュレーション（正解はどれか？）")
print("=" * 110)

# A) NTT生明細ベース（202307欠落あり）
print(f"\n■ パターンA: NTT生明細(不完全) vs 銀行支払い")
print(f"  利用合計: {raw_total:>10,}")
print(f"  支払合計: {bank_total:>10,}")
print(f"  残高:     {raw_total - bank_total:>+10,}  ← 202307欠落のため不正確")

# B) MF NTTカードのみ vs 銀行支払い
print(f"\n■ パターンB: MF(NTTカードのみ) vs 銀行支払い")
print(f"  利用合計: {mf_ntt_total:>10,}")
print(f"  支払合計: {bank_total:>10,}")
print(f"  残高:     {mf_ntt_total - bank_total:>+10,}")

# C) MF全体（楽天カード含む） vs 銀行支払い
mf_all_total = mf_ntt_total + mf_rakuten_total
print(f"\n■ パターンC: MF全体(楽天カード含む) vs 銀行支払い")
print(f"  利用合計: {mf_all_total:>10,}")
print(f"  支払合計: {bank_total:>10,}")
print(f"  残高:     {mf_all_total - bank_total:>+10,}")

# D) BQ vs 銀行支払い
print(f"\n■ パターンD: BQ/NocoDB vs 銀行支払い")
print(f"  利用合計: {bq_total:>10,}")
print(f"  支払合計: {bank_total:>10,}")
print(f"  残高:     {bq_total - bank_total:>+10,}")

# ============================================================
# Part 8: 202307(6月利用分)の推定
# ============================================================
print(f"\n\n{'=' * 110}")
print("【8】202307請求月（6月利用分）の推定 — CSVなし、MFとBQから推測")
print("=" * 110)

# MF June entries under NTT
mf_june_ntt = [e for e in mf_ntt_charges if month_key(e['date']) == '202306']
bq_june = [r for r in bq_rows if str(r.usage_date)[:7] == '2023-06']

print(f"\nMF 2023年6月利用(NTTカード): {len(mf_june_ntt)}件")
for e in mf_june_ntt:
    print(f"  {e['date']} {e['amount']:>8,} {e['description']}")
mf_june_total = sum(e['amount'] for e in mf_june_ntt)
print(f"  合計: {mf_june_total:,}")

print(f"\nBQ 2023年6月利用: {len(bq_june)}件")
for r in bq_june:
    print(f"  {r.usage_date} {r.usage_amount:>8,} {r.merchant_name}")
bq_june_total = sum(r.usage_amount for r in bq_june)
print(f"  合計: {bq_june_total:,}")

# Also check May entries since the billing cycle may overlap
raw_may_june = [e for e in all_raw_entries if e['date'][:7] in ('2023-05', '2023-06')]
mf_may_june = [e for e in mf_ntt_charges if month_key(e['date']) in ('202305', '202306')]

print(f"\n5-6月のNTT生明細 vs MF比較:")
print(f"  NTT生5月: {sum(e['amount'] for e in all_raw_entries if e['date'][:7]=='2023-05'):,}")
print(f"  MF NTT 5月: {sum(e['amount'] for e in mf_ntt_charges if month_key(e['date'])=='202305'):,}")
print(f"  NTT生6月: {sum(e['amount'] for e in all_raw_entries if e['date'][:7]=='2023-06'):,}")
print(f"  MF NTT 6月: {sum(e['amount'] for e in mf_ntt_charges if month_key(e['date'])=='202306'):,}")

# The "missing" from NTT raw = MF June - NTT raw June
ntt_june_raw = sum(e['amount'] for e in all_raw_entries if e['date'][:7] == '2023-06')
missing_from_raw = mf_june_total - ntt_june_raw
print(f"\n  NTT生明細に欠落している6月分: {missing_from_raw:,}円")
print(f"  NTT生明細 + 欠落分 = {raw_total + missing_from_raw:,}円")
print(f"  銀行支払い合計 = {bank_total:,}円")
print(f"  推定残高 = {raw_total + missing_from_raw - bank_total:+,}円")

# Final summary
print(f"\n\n{'=' * 110}")
print("【まとめ】")
print("=" * 110)
print(f"""
1. NTT生明細CSVの202307(6月利用分)が欠落 → PDFから読み取り必要
2. MFの未払金に楽天カード(個人)が{len(mf_rakuten_charges)}件({mf_rakuten_total:,}円)混入
   → これらは事業主借であるべき
3. 「NTT生明細 + 6月欠落分(MF推定)」vs「銀行支払い」= {raw_total + missing_from_raw - bank_total:+,}円
4. MF(NTTカードのみ) vs 銀行支払い = {mf_ntt_total - bank_total:+,}円
5. BQ vs 銀行支払い = {bq_total - bank_total:+,}円 → BQにも欠落あり
""")
