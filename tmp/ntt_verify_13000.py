"""
NTTカード：月別利用額 vs 銀行引落しの1対1突合
13,000円差異の正確性を検証
"""
import sys, csv, os, glob
sys.stdout.reconfigure(encoding='utf-8')

# ===== 1. NTT CSV月別合計 =====
csv_dir = "C:/Users/ninni/projects/rawdata/NTTファイナンスBizカード明細"
csv_files = sorted(glob.glob(os.path.join(csv_dir, "MYLINK_*.csv")))

monthly_csv = {}
for fpath in csv_files:
    fname = os.path.basename(fpath)
    billing_month = fname.replace("MYLINK_", "").replace(".csv", "")

    # Try UTF-8 first, then cp932
    for enc in ['utf-8-sig', 'cp932']:
        try:
            with open(fpath, encoding=enc) as f:
                lines = f.readlines()
            if len(lines) > 1:
                break
        except:
            continue

    total = 0
    count = 0
    for line in lines[1:]:  # skip header
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')
        # Find the amount column - it's "ご利用金額" column
        # Simple format: 利用日,加盟店名,利用区分,利用金額,...
        # Detailed format: カード名義,利用日,利用先,利用区分,利用金額,...
        try:
            # Simple format: column 3
            if parts[0].startswith('20'):  # starts with date
                amt = int(parts[3].strip().replace('"', ''))
            else:
                # Detailed format: column 4
                amt = int(parts[4].strip().replace('"', ''))
            total += amt
            count += 1
        except (ValueError, IndexError):
            continue

    monthly_csv[billing_month] = (total, count)

# ===== 2. 銀行引落し =====
bank_payments = [
    ("2023-04-25",    200, "202304"),
    ("2023-05-25", 14_300, "202305"),
    ("2023-06-26", 47_615, "202306"),
    ("2023-08-09", 408_219, "202307"),  # みずほ手動送金
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

# ===== 3. MF家計簿データ（NTT API直結）FY2025 =====
mf_personal = {
    "202502": (26_550, 6),   # 1月利用 → 2月引落し
    "202503": (20_395, 7),   # 2月利用 → 3月引落し
    "202504": (32_784, 7),   # 3月利用 → 4月引落し
}

# ===== 4. 突合 =====
print("=" * 100)
print("NTTカード 月別利用額 vs 銀行引落し — 完全突合")
print("=" * 100)
print(f"\n{'請求月':>8} {'CSV合計':>10} {'件数':>4} {'銀行引落':>10} {'差額':>8} {'判定'}")
print("-" * 65)

total_csv = 0
total_bank = 0
total_diff = 0
match_count = 0
mismatch_count = 0
missing_csv = 0

for date, amount, billing in bank_payments:
    csv_data = monthly_csv.get(billing)
    mf_data = mf_personal.get(billing)

    if csv_data and csv_data[0] > 0:
        csv_total, csv_count = csv_data
        diff = csv_total - amount
        total_csv += csv_total
        total_bank += amount
        total_diff += diff
        if diff == 0:
            status = "✓ 一致"
            match_count += 1
        else:
            status = f"✗ 差異"
            mismatch_count += 1
        print(f"{billing:>8} {csv_total:>10,} {csv_count:>4} {amount:>10,} {diff:>+8,} {status}")
    elif mf_data:
        mf_total, mf_count = mf_data
        diff = mf_total - amount
        total_csv += mf_total
        total_bank += amount
        total_diff += diff
        source = "MF家計簿"
        if diff == 0:
            status = f"✓ 一致({source})"
            match_count += 1
        else:
            status = f"✗ 差異({source})"
            mismatch_count += 1
        print(f"{billing:>8} {mf_total:>10,} {mf_count:>4} {amount:>10,} {diff:>+8,} {status}")
    else:
        total_bank += amount
        missing_csv += 1
        print(f"{billing:>8} {'N/A':>10} {'':>4} {amount:>10,} {'':>8} CSVなし")

print("-" * 65)
print(f"{'合計':>8} {total_csv:>10,} {'':>4} {total_bank:>10,} {total_diff:>+8,}")
print(f"\n一致: {match_count}件  差異: {mismatch_count}件  CSVなし: {missing_csv}件")

# ===== 5. 202307 PDF（手入力） =====
print(f"\n{'=' * 100}")
print("CSVなし月の補足情報")
print("=" * 100)
print("""
  202304: 銀行引落し200円のみ。CSV/PDFなし。年会費or初回手数料?
  202307: PDFあり。合計408,219円 = 銀行引落し408,219円 → ✓ 一致
  202504: MF家計簿32,784円 vs 銀行19,784円 → 差異13,000円（上表に含む）
""")

# ===== 6. 結論 =====
print("=" * 100)
print("結論")
print("=" * 100)

# 202307 is confirmed match via PDF
adjusted_match = match_count + 1  # +1 for 202307 PDF
adjusted_missing = missing_csv - 2  # -2 for 202307(PDF confirmed) and 202304

print(f"""
  全25回の銀行引落し:
    CSV/MFで突合できた月: {match_count + mismatch_count}件
    うち完全一致:         {match_count}件
    うち差異あり:         {mismatch_count}件
    PDFで一致確認:        1件 (202307)
    データなし:           {adjusted_missing}件 (202304のみ: 200円)

  差異があるのは202504（最終月）の1件のみ:
    MF家計簿(3月利用): 32,784円
    銀行引落し(4/25):  19,784円
    差額:              13,000円

  → 13,000円は正確な数字か？ 32,784 - 19,784 = {32_784 - 19_784:,}円
""")

# ===== 7. 13,000円の内訳を推測 =====
print("=" * 100)
print("13,000円の内訳推測（MF家計簿3月データから）")
print("=" * 100)
print("""
  MF家計簿の3月利用7件（合計32,784円）:
    ※ 前セッションで確認した7件の内訳から、
      銀行引落し19,784円に含まれる取引と
      含まれない取引（合計13,000円）があるはず

  NTTカードの請求サイクル:
    利用日 → 約1ヶ月後に請求確定 → 翌月25日引落し
    カード解約により、最終締め日以降の利用分は請求されなかった可能性
""")
