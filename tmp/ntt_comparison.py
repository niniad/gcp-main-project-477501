import sys, sqlite3, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')

# ============================================================
# 1. NocoDB NTT entries (all that could be FY2023 relevant)
# ============================================================
conn = sqlite3.connect('C:/Users/ninni/nocodb/noco.db')
cursor = conn.cursor()
cursor.execute('''SELECT id, 利用日, "ご利用加盟店", "ご利用金額", 摘要
FROM "nc_opau___NTTファイナンスBizカード明細"
WHERE 利用日 >= '2023-03-01' AND 利用日 <= '2024-03-31'
ORDER BY 利用日, id''')
noco_all = []
for row in cursor.fetchall():
    noco_all.append({
        'id': row[0], 'date': row[1], 'merchant': row[2],
        'amount': row[3], 'memo': row[4]
    })
conn.close()

print(f'NocoDB entries (2023-03 to 2024-03): {len(noco_all)}')

# ============================================================
# 2. MF entries
# ============================================================
mf = pd.read_csv(
    'C:/Users/ninni/projects/rawdata/マネーフォワード/MF_総勘定元帳_2023年度.csv',
    encoding='shift_jis'
)
mf_miharai = mf[mf['勘定科目'] == '未払金'].copy()
mf_ntt = mf_miharai[mf_miharai['補助科目'].str.contains('NTT', na=False)].copy()

# Credit entries = charges accrued
mf_ntt_cr = mf_ntt[mf_ntt['貸方金額'] > 0].copy()
# Debit entries = payments or refund reversals
mf_ntt_dr = mf_ntt[mf_ntt['借方金額'] > 0].copy()

print(f'MF NTT credit entries (charges): {len(mf_ntt_cr)}')
print(f'MF NTT debit entries: {len(mf_ntt_dr)}')

# Classify debit entries
mf_cr_list = []
for _, row in mf_ntt_cr.iterrows():
    date_str = row['取引日'].replace('/', '-')
    mf_cr_list.append({
        'no': row['取引No'],
        'date': date_str,
        'amount': int(row['貸方金額']),
        'memo': str(row['摘要']),
        'account': str(row['相手勘定科目']),
        'matched': False
    })

mf_dr_list = []
for _, row in mf_ntt_dr.iterrows():
    memo = str(row['摘要'])
    # Payment entries contain NTT and 支払/カード
    is_payment = ('NTT' in memo or 'みずほ' in memo) and ('支払' in memo or 'カード' in memo or 'NTTフアイナンス' in memo)
    date_str = row['取引日'].replace('/', '-')
    mf_dr_list.append({
        'no': row['取引No'],
        'date': date_str,
        'amount': int(row['借方金額']),
        'memo': memo,
        'account': str(row['相手勘定科目']),
        'is_payment': is_payment,
        'matched': False
    })

mf_payments = [d for d in mf_dr_list if d['is_payment']]
mf_refund_debits = [d for d in mf_dr_list if not d['is_payment']]

print(f'  - Payments to NTT: {len(mf_payments)}')
print(f'  - Refund reversals: {len(mf_refund_debits)}')

# ============================================================
# 3. Separate NocoDB into charges and refunds
# ============================================================
noco_charges = [e for e in noco_all if e['amount'] < 0]
noco_refunds = [e for e in noco_all if e['amount'] > 0]

print(f'\nNocoDB charges: {len(noco_charges)}')
print(f'NocoDB refunds: {len(noco_refunds)}')

# ============================================================
# 4. CHARGE MATCHING: NocoDB (charges) vs MF (credits)
# ============================================================
print('\n' + '='*80)
print('STEP 1: EXACT MATCH (date + amount)')
print('='*80)

matched_pairs = []

for nc in noco_charges:
    nc_date = nc['date']
    nc_amt = abs(nc['amount'])
    for mf_e in mf_cr_list:
        if not mf_e['matched'] and mf_e['amount'] == nc_amt and mf_e['date'] == nc_date:
            matched_pairs.append((nc, mf_e))
            mf_e['matched'] = True
            nc['matched'] = True
            break

unmatched_noco = [e for e in noco_charges if not e.get('matched')]
unmatched_mf = [e for e in mf_cr_list if not e['matched']]

print(f'Exact matched: {len(matched_pairs)}')
print(f'Unmatched NocoDB: {len(unmatched_noco)}')
print(f'Unmatched MF: {len(unmatched_mf)}')

# ============================================================
# 5. FUZZY MATCH: amount only
# ============================================================
print('\n' + '='*80)
print('STEP 2: AMOUNT-ONLY MATCH (for remaining)')
print('='*80)

fuzzy_matched = []
for nc in unmatched_noco:
    nc_amt = abs(nc['amount'])
    for mf_e in unmatched_mf:
        if not mf_e['matched'] and mf_e['amount'] == nc_amt:
            fuzzy_matched.append((nc, mf_e))
            mf_e['matched'] = True
            nc['fuzzy_matched'] = True
            print(f'  FUZZY: NocoDB id={nc["id"]} {nc["date"]} {nc["merchant"]} {nc_amt:,}')
            print(f'     <-> MF No.{mf_e["no"]} {mf_e["date"]} {mf_e["memo"]} {mf_e["amount"]:,}')
            break

still_unmatched_noco = [e for e in noco_charges if not e.get('matched') and not e.get('fuzzy_matched')]
still_unmatched_mf = [e for e in mf_cr_list if not e['matched']]

print(f'\nFuzzy matched: {len(fuzzy_matched)}')
print(f'Still unmatched NocoDB: {len(still_unmatched_noco)}')
print(f'Still unmatched MF: {len(still_unmatched_mf)}')

# ============================================================
# 6. REFUND MATCHING
# ============================================================
print('\n' + '='*80)
print('STEP 3: REFUND MATCHING')
print('='*80)

matched_refunds = []
for nc in noco_refunds:
    nc_date = nc['date']
    nc_amt = nc['amount']
    for mf_e in mf_refund_debits:
        if not mf_e['matched'] and mf_e['amount'] == nc_amt:
            if mf_e['date'] == nc_date:
                matched_refunds.append((nc, mf_e))
                mf_e['matched'] = True
                nc['matched'] = True
                break
    if not nc.get('matched'):
        for mf_e in mf_refund_debits:
            if not mf_e['matched'] and mf_e['amount'] == nc_amt:
                matched_refunds.append((nc, mf_e))
                mf_e['matched'] = True
                nc['matched'] = True
                print(f'  REFUND FUZZY: NocoDB id={nc["id"]} {nc["date"]} {nc["merchant"]} {nc_amt:,}')
                print(f'     <-> MF No.{mf_e["no"]} {mf_e["date"]} {mf_e["memo"]} {mf_e["amount"]:,}')
                break

unmatched_noco_ref = [e for e in noco_refunds if not e.get('matched')]
unmatched_mf_ref = [e for e in mf_refund_debits if not e['matched']]

print(f'Matched refunds: {len(matched_refunds)}')
print(f'Unmatched NocoDB refunds: {len(unmatched_noco_ref)}')
print(f'Unmatched MF refund debits: {len(unmatched_mf_ref)}')

# ============================================================
# 7. REPORT ALL UNMATCHED
# ============================================================
print('\n' + '='*80)
print('UNMATCHED NocoDB CHARGES (in NocoDB but NOT in MF)')
print('='*80)
for nc in still_unmatched_noco:
    print(f'  id={nc["id"]} | {nc["date"]} | {nc["merchant"]} | {abs(nc["amount"]):,} | memo={nc["memo"]}')

print('\n' + '='*80)
print('UNMATCHED MF CREDITS (in MF but NOT in NocoDB)')
print('='*80)
for mf_e in still_unmatched_mf:
    print(f'  No.{mf_e["no"]} | {mf_e["date"]} | {mf_e["amount"]:,} | {mf_e["memo"]} | {mf_e["account"]}')

print('\n' + '='*80)
print('UNMATCHED NocoDB REFUNDS')
print('='*80)
for nc in unmatched_noco_ref:
    print(f'  id={nc["id"]} | {nc["date"]} | {nc["merchant"]} | {nc["amount"]:,}')

print('\n' + '='*80)
print('UNMATCHED MF REFUND DEBITS')
print('='*80)
for mf_e in unmatched_mf_ref:
    print(f'  No.{mf_e["no"]} | {mf_e["date"]} | {mf_e["amount"]:,} | {mf_e["memo"]}')

# ============================================================
# 8. COMPLETE MATCHED PAIRS TABLE
# ============================================================
print('\n' + '='*80)
print('ALL MATCHED PAIRS (exact + fuzzy)')
print('='*80)
all_matched = matched_pairs + fuzzy_matched
all_matched.sort(key=lambda x: x[0]['date'])
for nc, mf_e in all_matched:
    match_type = 'EXACT' if nc.get('matched') else 'FUZZY'
    print(f'  {match_type} | NocoDB id={nc["id"]} {nc["date"]} {nc["merchant"][:20]:20s} {abs(nc["amount"]):>8,} | MF No.{mf_e["no"]:>3} {mf_e["date"]} {mf_e["memo"][:30]:30s} {mf_e["amount"]:>8,} | {mf_e["account"]}')

# ============================================================
# 9. TOTALS
# ============================================================
print('\n' + '='*80)
print('TOTALS')
print('='*80)

noco_charge_total = sum(abs(e['amount']) for e in noco_charges)
noco_refund_total = sum(e['amount'] for e in noco_refunds)
noco_net = noco_charge_total - noco_refund_total

mf_cr_total = sum(e['amount'] for e in mf_cr_list)
mf_ref_dr_total = sum(e['amount'] for e in mf_refund_debits)
mf_payment_total = sum(e['amount'] for e in mf_payments)
mf_net = mf_cr_total - mf_ref_dr_total

print(f'NocoDB charge total:  {noco_charge_total:>12,}')
print(f'NocoDB refund total:  {noco_refund_total:>12,}')
print(f'NocoDB net:           {noco_net:>12,}')
print()
print(f'MF credit total:      {mf_cr_total:>12,}')
print(f'MF refund debit total:{mf_ref_dr_total:>12,}')
print(f'MF net (cr - ref dr): {mf_net:>12,}')
print(f'MF payment total:     {mf_payment_total:>12,}')
print()
print(f'DIFFERENCE (NocoDB net - MF net): {noco_net - mf_net:>12,}')

# Unmatched totals
unmatched_noco_total = sum(abs(e['amount']) for e in still_unmatched_noco)
unmatched_mf_total = sum(e['amount'] for e in still_unmatched_mf)
print(f'\nUnmatched NocoDB total: {unmatched_noco_total:>12,}')
print(f'Unmatched MF total:     {unmatched_mf_total:>12,}')

# ============================================================
# 10. CSV CROSS-REFERENCE: Check for missing 202307 data
# ============================================================
print('\n' + '='*80)
print('CSV COVERAGE CHECK')
print('='*80)

import glob, os
csv_dir = 'C:/Users/ninni/projects/rawdata/NTTファイナンスBizカード明細/'
csv_files = sorted(glob.glob(os.path.join(csv_dir, 'MYLINK_*.csv')))
csv_months = [os.path.basename(f).replace('MYLINK_', '').replace('.csv', '') for f in csv_files]
print(f'Available CSV months: {csv_months}')

# Check which billing months are missing
all_months = []
for y in range(2023, 2025):
    for m in range(1, 13):
        ym = f'{y}{m:02d}'
        if ym >= '202305' and ym <= '202501':
            all_months.append(ym)

missing = [m for m in all_months if m not in csv_months]
print(f'Missing CSV months: {missing}')

# Check what transactions should be in 202307 billing
# 202307 billing = transactions from ~June (after 202306 cutoff)
print('\nNote: MYLINK_202307.csv is MISSING (only PDF exists)')
print('Expected content: transactions between approx 2023-06-05 and 2023-07-04')

# Check NocoDB entries in that range
noco_jul_range = [e for e in noco_all if e['date'] >= '2023-06-05' and e['date'] <= '2023-07-04']
print(f'\nNocoDB entries in 202307 billing range (2023-06-05 to 2023-07-04):')
for e in noco_jul_range:
    print(f'  id={e["id"]} | {e["date"]} | {e["merchant"]} | {e["amount"]:,}')

# ============================================================
# 11. MF PAYMENT ENTRIES DETAIL
# ============================================================
print('\n' + '='*80)
print('MF PAYMENT ENTRIES (NTTカード支払)')
print('='*80)
for mf_e in mf_payments:
    print(f'  No.{mf_e["no"]} | {mf_e["date"]} | {mf_e["amount"]:,} | {mf_e["memo"][:60]}')
