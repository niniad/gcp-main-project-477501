import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'C:/Users/ninni/.claude/skills/freee/scripts')
from auth import get_access_token, get_company_id, get_headers, FREEE_API_BASE
import requests

token = get_access_token()
cid = get_company_id(token)
headers = get_headers(token)

# FY2024 振替伝票の全件数（ページング）
url = f"{FREEE_API_BASE}/manual_journals"
total = 0
offset = 0
while True:
    params = {"company_id": cid, "start_issue_date": "2024-01-01", "end_issue_date": "2024-12-31",
              "limit": 100, "offset": offset}
    res = requests.get(url, headers=headers, params=params)
    mj_list = res.json().get("manual_journals", [])
    total += len(mj_list)
    if len(mj_list) < 100:
        break
    offset += 100

print(f"FY2024 振替伝票 合計: {total}件")

# trial_pl FY2024
url2 = f"{FREEE_API_BASE}/reports/trial_pl"
params2 = {"company_id": cid, "fiscal_year": 2024}
res2 = requests.get(url2, headers=headers, params=params2)
if res2.status_code == 200:
    data = res2.json().get("trial_pl", {})
    balances = data.get("balances", [])
    for b in balances:
        if b.get("account_item_name") in ("当期純利益（損失）", "当期純損益"):
            print(f"FY2024 当期純損益: ¥{b.get('closing_balance', 'N/A'):,}")
            break
    # 売上高と仕入を探す
    for b in balances:
        name = b.get("account_item_name", "")
        if name in ("売上高", "仕入高", "荷造運賃"):
            print(f"  {name}: ¥{b.get('closing_balance', 0):,}")
else:
    print(f"trial_pl FY2024: {res2.status_code} {res2.text[:200]}")
