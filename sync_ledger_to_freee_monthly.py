
import os
import sys
import datetime
from google.cloud import bigquery
# Assuming freee client is similar to previous script or we use requests directly
import requests
import json

# Configuration
FREEE_COMPANY_ID = os.environ.get('FREEE_COMPANY_ID', 'YOUR_COMPANY_ID')
# Access Token should be retrieved securely (e.g. from token.json refresh logic)
# For simplicity in this structure, we assume a helper or env var.
# In "100% Perfect" design, we should reuse the auth module from 'google-workspace' or 'freee' skill.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Check if we can import local auth modules.
# We will use the existing token.json logic from the 'freee' skill if available.

def get_bq_data(month_str):
    client = bigquery.Client()
    query = f"""
        SELECT month, sum(total_cogs_jpy) as monthly_cogs
        FROM `main-project-477501.analytics.rpt_freee_journal`
        WHERE month = '{month_str}'
        GROUP BY month
    """
    query_job = client.query(query)
    results = list(query_job.result())
    if not results:
        return 0
    return results[0].monthly_cogs

def create_journal_entry(month_str, amount):
    # Construct Journal Entry for COGS
    # Dr: Cost of Goods Sold (売上原価)
    # Cr: Inventory (棚卸資産)
    
    # Dates: Post at end of month or beginning of next? Usually End of Month.
    # Logic to get last day of month_str
    y, m = map(int, month_str.split('-'))
    next_month = m + 1 if m < 12 else 1
    next_year = y if m < 12 else y + 1
    last_date = (datetime.date(next_year, next_month, 1) - datetime.timedelta(days=1)).isoformat()

    print(f"Preparing Journal Entry for {month_str} (Date: {last_date})")
    print(f"Amount: {int(amount)} JPY")
    print(f"Dr: 売上原価 / Cr: 棚卸資産")
    
    # In a real run, we would POST to freee API here.
    # payload = {
    #   "company_id": FREEE_COMPANY_ID,
    #   "issue_date": last_date,
    #   "details": [
    #       {"account_item_id": "ID_COGS", "tax_code": 108, "amount": int(amount), "entry_side": "debit"},
    #       {"account_item_id": "ID_INVENTORY", "tax_code": 108, "amount": int(amount), "entry_side": "credit"}
    #   ]
    # }
    # requests.post(..., json=payload)
    print(">> DRY RUN: Skipping API Call. Please implement specific ID lookup.")

if __name__ == "__main__":
    # Default to last month if no arg
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    target_month = last_month.strftime("%Y-%m")
    
    if len(sys.argv) > 1:
        target_month = sys.argv[1]

    print(f"Processing COGS for {target_month}...")
    cogs = get_bq_data(target_month)
    
    if cogs and cogs > 0:
        create_journal_entry(target_month, cogs)
    else:
        print("No COGS found for this month.")
