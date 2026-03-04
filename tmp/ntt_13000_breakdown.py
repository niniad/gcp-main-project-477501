"""
13,000円の内訳を特定する
MF FY2025の3月NTTカード利用7件から、どの組み合わせが13,000円になるか
"""
import sys
from itertools import combinations
sys.stdout.reconfigure(encoding='utf-8')

# MF総勘定元帳 FY2025 — 3月のNTTカード利用（未払金セクション）
march_entries = [
    ("2025/03/01", "ASC subscription",     9_900),
    ("2025/03/01", "GOOGLE*CLOUD 8GXXWS",    275),
    ("2025/03/02", "ESPRIM",               5_500),
    ("2025/03/05", "マネーフォワードクラウド",  1_848),
    ("2025/03/07", "ISTOCK.COM",           3_300),
    ("2025/03/09", "Microsoft365",         2_061),
    ("2025/03/29", "ASC subscription",     9_900),
]

total = sum(e[2] for e in march_entries)
bank_payment = 19_784
diff = total - bank_payment

print("=" * 80)
print("MF FY2025 3月NTTカード利用明細")
print("=" * 80)
for d, m, a in march_entries:
    print(f"  {d}  {m:<30} {a:>8,}")
print(f"  {'合計':>41} {total:>8,}")
print(f"  {'4/25 銀行引落し':>41} {bank_payment:>8,}")
print(f"  {'差額':>41} {diff:>8,}")

print(f"\n{'=' * 80}")
print(f"13,000円になる取引の組み合わせを全探索")
print("=" * 80)

amounts = [e[2] for e in march_entries]
found = False
for r in range(1, len(amounts) + 1):
    for combo in combinations(range(len(amounts)), r):
        s = sum(amounts[i] for i in combo)
        if s == diff:
            found = True
            print(f"\n  ✓ {r}件の組み合わせで13,000円:")
            for i in combo:
                d, m, a = march_entries[i]
                print(f"    {d} {m:<30} {a:>8,}")

if not found:
    print(f"\n  ✗ 13,000円ぴったりになる組み合わせは存在しない")

# 19,784円になる組み合わせも探索
print(f"\n{'=' * 80}")
print(f"19,784円（銀行引落し額）になる組み合わせを全探索")
print("=" * 80)

found2 = False
for r in range(1, len(amounts) + 1):
    for combo in combinations(range(len(amounts)), r):
        s = sum(amounts[i] for i in combo)
        if s == bank_payment:
            found2 = True
            print(f"\n  ✓ {r}件の組み合わせで19,784円:")
            for i in combo:
                d, m, a = march_entries[i]
                print(f"    {d} {m:<30} {a:>8,}")

if not found2:
    print(f"\n  ✗ 19,784円ぴったりになる組み合わせも存在しない")

# 近い組み合わせを探す
print(f"\n{'=' * 80}")
print(f"13,000に最も近い組み合わせ（差が500以内）")
print("=" * 80)
close_matches = []
for r in range(1, len(amounts) + 1):
    for combo in combinations(range(len(amounts)), r):
        s = sum(amounts[i] for i in combo)
        gap = abs(s - diff)
        if gap <= 500 and gap > 0:
            close_matches.append((gap, combo, s))
close_matches.sort()
for gap, combo, s in close_matches[:5]:
    items = ", ".join(f"{march_entries[i][1]}({march_entries[i][2]:,})" for i in combo)
    print(f"  合計{s:,} (差{gap:+,}): {items}")

# MFの未払金残高推移で確認
print(f"\n{'=' * 80}")
print(f"MF未払金残高推移（FY2025 NTTカード）")
print("=" * 80)
print("""
  ※ MF総勘定元帳の残高推移:
  ...
  03/09 Microsoft365 +2,061  → 残高 37,281
  03/25 銀行支払い   -20,395 → 残高 16,886  ← 3月引落し
  03/29 ASC          +9,900  → 残高 26,786
  04/25 銀行支払い   -19,784 → 残高  7,002  ← 最終引落し

  MF上の最終未払金残高: 7,002円（≠ 13,000円）

  理由: 13,000 = 3月利用合計(32,784) - 4月引落し(19,784) だが、
  3月25日の引落し(20,395)が3月利用の一部も含んでいる。

  つまり13,000円は「3月利用 - 4月引落し」という単純計算であり、
  実際の未払い残高は7,002円。
""")
