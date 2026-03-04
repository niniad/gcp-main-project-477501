"""
在庫元帳データ補完スクリプト
- 2023年: Excel FBA棚卸シート (Summary View) → TSV
- 2024年1-6月: CSV Detail View → Summary View集約 → TSV
出力先: tmp/ledger_tsv/ に月別TSV
"""
import sys, os, csv
from collections import defaultdict
from datetime import datetime
sys.stdout.reconfigure(encoding='utf-8')

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'ledger_tsv')
os.makedirs(OUTPUT_DIR, exist_ok=True)

TSV_HEADER = [
    "Date", "FNSKU", "ASIN", "MSKU", "Title", "Disposition",
    "Starting Warehouse Balance", "In Transit Between Warehouses",
    "Receipts", "Customer Shipments", "Customer Returns", "Vendor Returns",
    "Warehouse Transfer In/Out", "Found", "Lost", "Damaged", "Disposed",
    "Other Events", "Ending Warehouse Balance", "Unknown Events", "Location"
]

DISPOSITION_MAP = {
    '販売可': 'SELLABLE',
    '欠陥品・不良品': 'DEFECTIVE',
    '欠陥品': 'DEFECTIVE',
    '不良品': 'DEFECTIVE',
}

def quote(val):
    return f'"{val}"'

def write_tsv(filename, rows):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'w', encoding='utf-8', newline='') as f:
        # Header
        f.write('\t'.join(quote(h) for h in TSV_HEADER) + '\n')
        for row in rows:
            f.write('\t'.join(quote(str(v)) for v in row) + '\n')
    print(f'  Written: {filename} ({len(rows)} rows)')

# ================================================================
# Part 1: 2023年 Excel FBA棚卸 → TSV
# ================================================================
print('=== Part 1: 2023 Excel → TSV ===')

import openpyxl
excel_path = r'g:\マイドライブ\仕事\書類控え\2023年データ\20231231_2023年締めデータ_Amazon販売分析v5.xlsx'
wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
ws = wb['FBA棚卸']

# Read all data rows (skip first 3 info rows, row 4 = header)
data_2023 = []
for i, row in enumerate(ws.iter_rows(min_row=5, values_only=True)):
    # Columns: 日付(0), 画像(1), スナップショット日付(2), FNSKU(3), ASIN(4),
    #          出品者SKU(5), 商品名(6), 商品のステータス(7),
    #          開始時の倉庫の在庫(8), 倉庫間の輸送中(9), 受領(10),
    #          カスタマー発送(11), 返品(12), ベンダーの返品(13),
    #          倉庫転送イン/アウト(14), 発見済み(15), 紛失(16), 破損(17),
    #          廃棄済み(18), その他(19), 終了時の倉庫の在庫(20),
    #          不明なイベント(21), 場所(22), 払出数量(23)
    if row[2] is None:
        continue

    date_str = str(row[2])  # "01/2023" format
    disposition = DISPOSITION_MAP.get(str(row[7]), str(row[7]))

    tsv_row = [
        date_str,           # Date
        str(row[3]),         # FNSKU
        str(row[4]),         # ASIN
        str(row[5]),         # MSKU
        str(row[6]),         # Title
        disposition,         # Disposition
        int(row[8] or 0),    # Starting Warehouse Balance
        int(row[9] or 0),    # In Transit Between Warehouses
        int(row[10] or 0),   # Receipts
        int(row[11] or 0),   # Customer Shipments
        int(row[12] or 0),   # Customer Returns
        int(row[13] or 0),   # Vendor Returns
        int(row[14] or 0),   # Warehouse Transfer In/Out
        int(row[15] or 0),   # Found
        int(row[16] or 0),   # Lost
        int(row[17] or 0),   # Damaged
        int(row[18] or 0),   # Disposed
        int(row[19] or 0),   # Other Events
        int(row[20] or 0),   # Ending Warehouse Balance
        int(row[21] or 0),   # Unknown Events
        str(row[22] or 'JP'),  # Location
    ]
    data_2023.append(tsv_row)
wb.close()

# Group by month and write TSV files
months_2023 = defaultdict(list)
for row in data_2023:
    # Date is "MM/YYYY"
    parts = row[0].split('/')
    month_key = f'{parts[1]}{parts[0].zfill(2)}'  # "202301"
    months_2023[month_key].append(row)

for month_key in sorted(months_2023.keys()):
    write_tsv(f'{month_key}.tsv', months_2023[month_key])

print(f'  Total 2023: {len(data_2023)} rows across {len(months_2023)} months')

# Save Dec 2023 ending balances for 2024 starting balance
dec_2023_ending = {}
for row in months_2023.get('202312', []):
    key = (row[1], row[2], row[3], row[5])  # FNSKU, ASIN, MSKU, Disposition
    dec_2023_ending[key] = int(row[18])  # Ending Warehouse Balance
print(f'  Dec 2023 ending balances: {len(dec_2023_ending)} SKU-dispositions')

# ================================================================
# Part 2: 2024年1-6月 CSV Detail View → Summary View → TSV
# ================================================================
print('\n=== Part 2: 2024 CSV Detail View → Summary View TSV ===')

csv_path = r'g:\マイドライブ\仕事\data\Amazon_在庫元帳.csv'

# Event type → TSV column mapping
EVENT_COL_MAP = {
    'Shipments': 'Customer Shipments',
    'CustomerReturns': 'Customer Returns',
    'VendorReturns': 'Vendor Returns',
    'Receipts': 'Receipts',
    'WhseTransfers': 'Warehouse Transfer In/Out',
}

# Adjustment reason → TSV column
ADJ_REASON_MAP = {
    'F': 'Found',
    'M': 'Lost',
    'D': 'Damaged',
    'E': 'Disposed',
}

# Read CSV and aggregate
# Key: (month_str "MM/YYYY", FNSKU, ASIN, MSKU, Title, Disposition)
# Values: dict of column_name → sum(quantity)
agg = defaultdict(lambda: defaultdict(int))
titles = {}

with open(csv_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        date_str = row['スナップショット日付']  # "MM/DD/YYYY"
        try:
            dt = datetime.strptime(date_str, '%m/%d/%Y')
        except ValueError:
            continue

        # Only 2024-01 to 2024-06
        if not (dt.year == 2024 and 1 <= dt.month <= 6):
            continue

        month_str = f'{dt.month:02d}/{dt.year}'  # "01/2024"
        fnsku = row['フルフィルメントネットワークSKU（FNSKU）']
        asin = row['ASIN']
        msku = row['出品者SKU']
        title = row['商品名']
        disp_jp = row['商品のステータス']
        disposition = DISPOSITION_MAP.get(disp_jp, disp_jp)
        event_type = row['イベントタイプ']
        qty = int(row['数量'] or 0)
        reason = row.get('理由', '')

        key = (month_str, fnsku, asin, msku, disposition)
        titles[key] = title

        # Map event to column
        if event_type in EVENT_COL_MAP:
            col = EVENT_COL_MAP[event_type]
            agg[key][col] += qty
        elif event_type == 'Adjustments':
            col = ADJ_REASON_MAP.get(reason, 'Other Events')
            agg[key][col] += qty
        else:
            agg[key]['Other Events'] += qty

# Build monthly data with Starting/Ending balances
# Sort keys by month to process sequentially
all_keys = sorted(agg.keys())

# Track running balance per (FNSKU, ASIN, MSKU, Disposition)
running_balance = dict(dec_2023_ending)  # Initialize from Dec 2023

# Process month by month
months_2024 = defaultdict(list)
processed_months = sorted(set(k[0] for k in all_keys))

for month_str in processed_months:
    month_keys = [k for k in all_keys if k[0] == month_str]

    # Also include SKUs that had balance but no events this month
    active_skus = set()
    for k in month_keys:
        active_skus.add(k[1:])  # (FNSKU, ASIN, MSKU, Disposition)

    for bal_key, bal in running_balance.items():
        if bal != 0:
            sku_key = bal_key  # (FNSKU, ASIN, MSKU, Disposition)
            full_key = (month_str,) + sku_key
            if sku_key not in active_skus:
                active_skus.add(sku_key)
                # Create zero-event entry
                if full_key not in agg:
                    agg[full_key] = defaultdict(int)

    # Build rows for this month
    for sku_key in sorted(active_skus):
        full_key = (month_str,) + sku_key
        events = agg.get(full_key, defaultdict(int))

        bal_key = sku_key  # (FNSKU, ASIN, MSKU, Disposition)
        starting = running_balance.get(bal_key, 0)

        # Sum all events
        event_sum = sum(events.values())
        ending = starting + event_sum

        title = titles.get(full_key, '')
        if not title:
            # Find title from any month
            for k, t in titles.items():
                if k[1:] == sku_key:
                    title = t
                    break

        tsv_row = [
            month_str,
            sku_key[0],  # FNSKU
            sku_key[1],  # ASIN
            sku_key[2],  # MSKU
            title,
            sku_key[3],  # Disposition
            starting,
            events.get('In Transit Between Warehouses', 0),
            events.get('Receipts', 0),
            events.get('Customer Shipments', 0),
            events.get('Customer Returns', 0),
            events.get('Vendor Returns', 0),
            events.get('Warehouse Transfer In/Out', 0),
            events.get('Found', 0),
            events.get('Lost', 0),
            events.get('Damaged', 0),
            events.get('Disposed', 0),
            events.get('Other Events', 0),
            ending,
            0,  # Unknown Events
            'JP',
        ]

        # Parse month for file naming
        parts = month_str.split('/')
        file_key = f'{parts[1]}{parts[0]}'
        months_2024[file_key].append(tsv_row)

        # Update running balance
        running_balance[bal_key] = ending

for month_key in sorted(months_2024.keys()):
    write_tsv(f'{month_key}.tsv', months_2024[month_key])

total_rows = sum(len(v) for v in months_2024.values())
print(f'  Total 2024 H1: {total_rows} rows across {len(months_2024)} months')

# ================================================================
# Verification
# ================================================================
print('\n=== Verification ===')
# Check continuity: Dec 2023 ending = Jan 2024 starting
if '202312' in months_2023 and '202401' in months_2024:
    for row_dec in months_2023['202312']:
        fnsku = row_dec[1]
        disp = row_dec[5]
        ending = int(row_dec[18])
        # Find matching Jan 2024
        for row_jan in months_2024['202401']:
            if row_jan[1] == fnsku and row_jan[5] == disp:
                starting = int(row_jan[6])
                if ending != starting:
                    print(f'  MISMATCH: {fnsku}/{disp} Dec ending={ending} vs Jan starting={starting}')
                break
    print('  Dec 2023→Jan 2024 continuity check done')

# Check Jun 2024 ending vs Jul 2024 GCS starting
print('  (Jul 2024 GCS comparison should be done after upload)')

# List output files
print(f'\nOutput files in {OUTPUT_DIR}:')
for f in sorted(os.listdir(OUTPUT_DIR)):
    size = os.path.getsize(os.path.join(OUTPUT_DIR, f))
    print(f'  {f} ({size:,} bytes)')
