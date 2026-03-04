"""Reconstruct what OLD P/L was by simulating the old journal_entries VIEW"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# Hypothesis 1: Old VIEW used d.account_item_id = ai.nocodb_id (ALL NULL)
# → Old Amazon P/L = 0

# Hypothesis 2: Old VIEW used account_map.account_name_debug
# → Old Amazon P/L = large numbers

# Test: If old Amazon P/L = 0, what must non-Amazon P/L have been?
# FY2024: total = -1,088,882, Amazon = 0 → non-Amazon = -1,088,882
# Current non-Amazon FY2024:
q1 = """
SELECT fiscal_year,
  SUM(CASE WHEN source_table = 'amazon_settlement' THEN pl_contribution ELSE 0 END) as amazon_pl,
  SUM(CASE WHEN source_table != 'amazon_settlement' THEN pl_contribution ELSE 0 END) as non_amazon_pl,
  SUM(pl_contribution) as total_pl
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
"""
print('=== Current P/L breakdown ===')
print(f'  {"FY":<6} {"Amazon":>12} {"Non-Amazon":>12} {"Total":>12}')
for row in client.query(q1).result():
    a = row.amazon_pl or 0
    na = row.non_amazon_pl or 0
    t = row.total_pl or 0
    print(f'  FY{row.fiscal_year:<4} {a:>+12,} {na:>+12,} {t:>+12,}')

# If old Amazon was 0, old total = old non-Amazon
# We know old totals from mf_bq_reconciliation.md:
# FY2023: -1,340,610
# FY2024: -1,088,882
# So: old non-Amazon FY2023 = -1,340,610, old non-Amazon FY2024 = -1,088,882

# Current non-Amazon:
# FY2023: -1,396,517
# FY2024: -1,435,375

# DIFF FY2023: -1,396,517 - (-1,340,610) = -55,907
# DIFF FY2024: -1,435,375 - (-1,088,882) = -346,493

# These are EXACTLY the new Amazon P/L values (56,107 and 346,493)!
# Wait... -55,907 ≈ -56,107... not exactly. Off by 200 (the NTT fix!)

# So the math is:
# Old total = old non-Amazon + 0 (old Amazon)
# New total = new non-Amazon + new Amazon
# If new non-Amazon = old non-Amazon, then new total = old total + new Amazon
# FY2023: new total = -1,340,610 + 200 (NTT) + 56,107 (Amazon) = -1,284,303
# But actual new total = -1,340,410
# -1,284,303 ≠ -1,340,410. Off by -56,107 (= new Amazon PL)!

# This means: if old Amazon = 0 AND non-Amazon didn't change,
# new total should differ from old by exactly Amazon PL.
# But it DOESN'T differ that much.
# FY2024: expected diff = +346,493, actual diff = 0.
# This is impossible unless old Amazon ≠ 0 OR non-Amazon changed.

# Let me check if non-Amazon changed by checking each source table
print('\n=== Checking non-Amazon tables: expected vs actual ===')
print('If old Amazon PL = 0:')
print(f'  FY2023: old non-Amazon = -1,340,610, current non-Amazon = -1,396,517')
print(f'  FY2023: diff = {-1396517 - (-1340610):+,} (should be +200 from NTT fix only)')
print(f'  FY2024: old non-Amazon = -1,088,882, current non-Amazon = -1,435,375')
print(f'  FY2024: diff = {-1435375 - (-1088882):+,} (should be 0)')

print('\nConclusion: old Amazon PL was NOT 0. Checking what it was...')

# If non-Amazon didn't change (except NTT +200):
# FY2023: old Amazon = old total - old non-Amazon = -1,340,610 - (-1,340,610) = 0
#   BUT new non-Amazon might not equal old non-Amazon.
# Actually: old total = old Amazon + old non-Amazon
#           new total = new Amazon + new non-Amazon
# If non-Amazon changed only by NTT fix:
#   new non-Amazon = old non-Amazon + 200 (FY2023), +0 (FY2024)
# Then: new total - old total = (new Amazon - old Amazon) + (new non-Amazon - old non-Amazon)
# FY2023: -1,340,410 - (-1,340,610) = (new Amazon - old Amazon) + 200
#         +200 = (56,107 - old Amazon) + 200
#         old Amazon = 56,107 ← WAIT
# FY2024: -1,088,882 - (-1,088,882) = (346,493 - old Amazon) + 0
#         0 = 346,493 - old Amazon
#         old Amazon FY2024 = 346,493 ← Same as new!

print('\nMathematical derivation:')
print('  FY2023: old_Amazon = new_Amazon - (new_total - old_total - NTT_fix)')
print(f'  FY2023: old_Amazon = 56,107 - (-1,340,410 - (-1,340,610) - 200) = 56,107 - (-200) = 56,107')
print(f'  → Old Amazon FY2023 = +56,107 (SAME as new)')
print()
print(f'  FY2024: old_Amazon = new_Amazon - (new_total - old_total)')
print(f'  FY2024: old_Amazon = 346,493 - (-1,088,882 - (-1,088,882)) = 346,493')
print(f'  → Old Amazon FY2024 = +346,493 (SAME as new)')
print()
print('Conclusion: Old and new Amazon P/L are IDENTICAL.')
print('The architecture change was P/L-neutral for FY2023 and FY2024.')
print()
print('Summary:')
print('  FY2023: -1,340,610 → -1,340,410 (diff: +200 from NTT fix only)')
print('  FY2024: -1,088,882 → -1,088,882 (diff: 0)')
print('  FY2025: -550,091 → -489,429 (diff: +60,662 - needs investigation)')
