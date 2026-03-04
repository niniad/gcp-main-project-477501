"""
NTTカード：月別利用額 vs 銀行引落しの1対1突合 v2
CSVパーサーをcsv moduleに変更（カンマ含む加盟店名対応）
"""
import sys, csv, os, glob
sys.stdout.reconfigure(encoding='utf-8')

csv_dir = "C:/Users/ninni/projects/rawdata/NTTファイナンスBizカード明細"
csv_files = sorted(glob.glob(os.path.join(csv_dir, "MYLINK_*.csv")))

monthly_csv = {}

for fpath in csv_files:
    fname = os.path.basename(fpath)
    billing_month = fname.replace("MYLINK_", "").replace(".csv", "")

    # Try each encoding
    rows = []
    for enc in ['utf-8-sig', 'cp932']:
        try:
            with open(fpath, encoding=enc, newline='') as f:
                reader = csv.reader(f)
                rows = list(reader)
            if len(rows) > 1:
                break
        except:
            rows = []
            continue

    if not rows:
        continue

    header = rows[0]

    # Detect format
    # Simple: 利用日,ご利用加盟店名・摘要,ご利用区分,ご利用金額,...
    # Detailed: カード名義,NTTファイナンスBizカード ...
    is_simple = header[0].strip().startswith('利用日')

    total = 0
    count = 0
    entries = []
    for row in rows[1:]:
        if not row or not row[0].strip():
            continue

        try:
            if is_simple:
                # Simple: col0=利用日, col1=加盟店, col2=利用区分, col3=利用金額
                date_str = row[0].strip()
                if not date_str.startswith('20'):
                    continue
                merchant = row[1].strip()
                amt = int(row[3].strip())
            else:
                # Detailed: col0=利用日(YYYYMMDD), col1=加盟店, col2=支払区分, col3=利用金額
                date_str = row[0].strip()
                if not date_str.startswith('20'):
                    continue
                merchant = row[1].strip()
                amt = int(row[3].strip())
            total += amt
            count += 1
            entries.append((date_str, merchant, amt))
        except (ValueError, IndexError):
            continue

    monthly_csv[billing_month] = (total, count, entries)

# Bank payments
bank_payments = [
    ("2023-04-25",    200, "202304"),
    ("2023-05-25", 14_300, "202305"),
    ("2023-06-26", 47_615, "202306"),
    ("2023-08-09", 408_219, "202307"),
    ("2023-08-25", 71_513, "202308"),
    ("2023-09-25", 85_144, "202309"),
    ("2023-10-25", 41_959, "202310"),
    ("2023-11-27", 28_928, "202311"),
    ("2023-12-25", 40_909, "202312"),
    ("2024-01-25", 41_444, "202401"),
    ("2024-02-26", 20_644, "202402"),
    ("2024-03-25", 20_363, "202403"),
    ("2024-04-25", 30_879, "202404"),
    ("2024-05-27", 39_675, "202405"),
    ("2024-06-25", 63_210, "202406"),
    ("2024-07-25", 39_910, "202407"),
    ("2024-08-26", 103_860, "202408"),
    ("2024-09-25", 54_377, "202409"),
    ("2024-10-25", 62_153, "202410"),
    ("2024-11-25", 28_044, "202411"),
    ("2024-12-25", 24_772, "202412"),
    ("2025-01-27", 34_355, "202501"),
    ("2025-02-25", 26_550, "202502"),
    ("2025-03-25", 20_395, "202503"),
    ("2025-04-25", 19_784, "202504"),
]

# MF personal data (NTT API)
mf_personal = {
    "202502": (26_550, 6),
    "202503": (20_395, 7),
    "202504": (32_784, 7),
}

print("=" * 105)
print("NTTカード 月別利用額 vs 銀行引落し — 完全突合 (v2: CSVパーサー修正)")
print("=" * 105)
print(f"\n{'請求月':>8} {'CSV合計':>10} {'件数':>4} {'銀行引落':>10} {'差額':>8} {'判定'}")
print("-" * 70)

all_match = True
for date, amount, billing in bank_payments:
    csv_data = monthly_csv.get(billing)
    mf_data = mf_personal.get(billing)

    if csv_data and csv_data[0] > 0:
        csv_total, csv_count, entries = csv_data
        diff = csv_total - amount
        if diff == 0:
            status = "✓"
        else:
            status = f"✗ ({diff:+,})"
            all_match = False
        print(f"{billing:>8} {csv_total:>10,} {csv_count:>4} {amount:>10,} {diff:>+8,} {status}")
        if diff != 0:
            print(f"         ▼ CSV明細:")
            for d, m, a in entries:
                print(f"           {d} {m[:30]:<30} {a:>8,}")
    elif mf_data:
        mf_total, mf_count = mf_data
        diff = mf_total - amount
        src = "MF家計簿"
        if diff == 0:
            status = f"✓ ({src})"
        else:
            status = f"✗ ({src}, {diff:+,})"
            all_match = False
        print(f"{billing:>8} {mf_total:>10,} {mf_count:>4} {amount:>10,} {diff:>+8,} {status}")
    else:
        print(f"{billing:>8} {'N/A':>10} {'':>4} {amount:>10,} {'':>8} CSVなし")

print("-" * 70)

# Summary
csv_total_all = sum(d[0] for d in monthly_csv.values() if d[0] > 0)
bank_total = sum(p[1] for p in bank_payments)
print(f"\nCSV合計（全月）: {csv_total_all:,}")
print(f"銀行引落し合計:  {bank_total:,}")
print(f"差額:            {csv_total_all - bank_total:+,}")

# Check 202304 specifically
print(f"\n{'=' * 105}")
print("202304: CSVなし、銀行引落し200円の分析")
print("=" * 105)
print("""
202304の銀行引落し200円は、NTTカード開設時の初回手数料または
前月の小額利用の可能性。CSVが存在しないため確認不可。
202305 CSVには2023-03-31利用のマネーフォワードクラウド(1,408円)が含まれており、
これは本来202304請求に含まれるべきだった可能性もある。
""")
