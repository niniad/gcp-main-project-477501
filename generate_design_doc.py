
import pandas as pd
import os

# Define output path
OUTPUT_FILE = 'C:/Users/ninni/Documents/projects/gcp-main-project-477501/database_design.xlsx'

# Create sample data for each table

# 1. agency_ledger (The Wallet)
df_ledger = pd.DataFrame([
    {'id': 'L001', 'date': '2024-01-01', 'category': 'DEPOSIT', 'description': 'Initial Deposit', 'expense_cny': 0, 'income_cny': 50000, 'po_number': '', 'rate_snapshot': 0},
    {'id': 'L002', 'date': '2024-01-05', 'category': 'PAYMENT', 'description': 'Payment for PO-001', 'expense_cny': 10000, 'income_cny': 0, 'po_number': 'PO-001', 'rate_snapshot': 20.5},
    {'id': 'L003', 'date': '2024-01-10', 'category': 'FEE', 'description': 'Monthly Storage', 'expense_cny': 500, 'income_cny': 0, 'po_number': '', 'rate_snapshot': 20.6}
])

# 2. deposit_log (For Rate Calculation - Derived from Ledger DEPOSIT rows or explicit log)
# User wants Ledger to be the truth, so we use Ledger DEPOSIT rows. But for clarity:
df_deposits = pd.DataFrame([
    {'date': '2024-01-01', 'sent_jpy': 1025000, 'received_cny': 50000, 'real_rate': 20.5}
])

# 3. po_details (The SKU Breakdown)
df_po = pd.DataFrame([
    {'po_number': 'PO-001', 'sku': 'ITEM-A', 'qty': 100, 'unit_cny': 50},
    {'po_number': 'PO-001', 'sku': 'ITEM-B', 'qty': 200, 'unit_cny': 25}
])

# 4. external_payments (Duties paid separately)
df_ext = pd.DataFrame([
    {'date': '2024-01-15', 'category': 'DUTY', 'amount_jpy': 15000, 'link_key': 'PO-001'}
])

# 5. fba_inventory (Snapshot from Amazon)
df_fba = pd.DataFrame([
    {'snapshot_date': '2024-01-31', 'sku': 'ITEM-A', 'qty': 80, 'location': 'FBA-JP'},
    {'snapshot_date': '2024-01-31', 'sku': 'ITEM-B', 'qty': 190, 'location': 'FBA-JP'}
])

# 6. amazon_settlements (Sales)
df_settlement = pd.DataFrame([
    {'posted_date': '2024-01-20', 'order_id': '111-222', 'sku': 'ITEM-A', 'amount_type': 'ItemPrice', 'amount': 2000},
    {'posted_date': '2024-01-20', 'order_id': '111-222', 'sku': 'ITEM-A', 'amount_type': 'FBA Fee', 'amount': -500},
    {'posted_date': '2024-01-20', 'order_id': '111-222', 'sku': 'ITEM-A', 'amount_type': 'Commission', 'amount': -200}
])

# 7. cost_calculation_result (BigQuery View Output)
df_cost = pd.DataFrame([
    {'po_number': 'PO-001', 'sku': 'ITEM-A', 'qty': 100, 'landed_cost_unit_jpy': 1200, 'total_cost_jpy': 120000},
    {'po_number': 'PO-001', 'sku': 'ITEM-B', 'qty': 200, 'landed_cost_unit_jpy': 600, 'total_cost_jpy': 120000}
])

# 8. pnl_5stage (Management Report)
df_pnl = pd.DataFrame([
    {'month': '2024-01', 'sku': 'ITEM-A', 'sales': 40000, 'cogs': 24000, 'gross_profit': 16000, 'amz_fees': 10000, 'net_gross': 6000, 'ads': 2000, 'sales_profit': 4000}
])

# Create directory if not exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Write to Excel
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    df_ledger.to_excel(writer, sheet_name='input_agency_ledger', index=False)
    df_po.to_excel(writer, sheet_name='input_po_details', index=False)
    df_ext.to_excel(writer, sheet_name='input_external_payments', index=False)
    df_settlement.to_excel(writer, sheet_name='input_amazon_settlements', index=False)
    df_fba.to_excel(writer, sheet_name='input_fba_inventory', index=False)
    df_cost.to_excel(writer, sheet_name='view_landed_cost', index=False)
    df_pnl.to_excel(writer, sheet_name='rpt_pnl_5stage', index=False)

print(f"Created design document at {OUTPUT_FILE}")
