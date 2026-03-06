"""2023年 月次COGS ブレ原因調査"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

# 2023年 SKU別 標準原価の有無（2023-06-01時点）
print('=== 2023年 SKU別 標準原価（2023-06-01時点）===')
q = """
SELECT pm.amazon_sku, pm.name,
  sch.effective_start_date, sch.effective_end_date,
  sch.standard_cost, sch.justification_memo
FROM `main-project-477501.nocodb.product_master` pm
LEFT JOIN `main-project-477501.nocodb.standard_cost_history` sch
  ON pm.nocodb_id = sch.products_id
  AND SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_start_date) <= DATE(2023, 6, 1)
  AND (SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_end_date) >= DATE(2023, 6, 1)
       OR sch.effective_end_date IS NULL)
ORDER BY pm.amazon_sku
"""
for r in client.query(q).result():
    if r.standard_cost is None:
        status = '【原価未設定】'
    elif int(r.standard_cost) == 0:
        status = '【原価=0】'
    else:
        status = f'Y{int(r.standard_cost)}'
    memo = (r.justification_memo or '')[:40]
    print(f'  {r.amazon_sku}: {status}  {memo}')

# 2023年 月別出荷COGS内訳
print()
print('=== 2023年 月別 出荷×原価 内訳 ===')
q2 = """
SELECT
  CAST(SUBSTR(l.Date, STRPOS(l.Date,'/')+1) AS INT64) AS year,
  CAST(SUBSTR(l.Date, 1, STRPOS(l.Date,'/')-1) AS INT64) AS month,
  l.MSKU,
  ABS(SUM(CAST(l.Customer_Shipments AS INT64))) AS ship_qty,
  CAST(COALESCE(sch.standard_cost, 0) AS INT64) AS std_cost,
  ABS(SUM(CAST(l.Customer_Shipments AS INT64)))
    * CAST(COALESCE(sch.standard_cost, 0) AS INT64) AS cogs
FROM `main-project-477501.sp_api_external.ledger-summary-view-data` l
JOIN `main-project-477501.nocodb.product_master` pm ON l.MSKU = pm.amazon_sku
LEFT JOIN `main-project-477501.nocodb.standard_cost_history` sch
  ON pm.nocodb_id = sch.products_id
  AND SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_start_date) <= LAST_DAY(DATE(
      CAST(SUBSTR(l.Date, STRPOS(l.Date,'/')+1) AS INT64),
      CAST(SUBSTR(l.Date, 1, STRPOS(l.Date,'/')-1) AS INT64), 1))
  AND (SAFE.PARSE_DATE('%Y-%m-%d', sch.effective_end_date) >= LAST_DAY(DATE(
      CAST(SUBSTR(l.Date, STRPOS(l.Date,'/')+1) AS INT64),
      CAST(SUBSTR(l.Date, 1, STRPOS(l.Date,'/')-1) AS INT64), 1))
       OR sch.effective_end_date IS NULL)
WHERE l.Disposition = 'SELLABLE'
  AND CAST(SUBSTR(l.Date, STRPOS(l.Date,'/')+1) AS INT64) = 2023
GROUP BY 1, 2, 3, 5
HAVING ABS(SUM(CAST(l.Customer_Shipments AS INT64))) > 0
ORDER BY 2, 3
"""
prev_month = 0
month_total = 0
for r in client.query(q2).result():
    if r.month != prev_month:
        if prev_month:
            print(f'    -> 月計: Y{month_total:,}')
        print(f'  --- {r.month}月 ---')
        prev_month = r.month
        month_total = 0
    warn = ' !!原価ゼロ!!' if r.std_cost == 0 else ''
    print(f'    {r.MSKU}: {r.ship_qty}個 x Y{r.std_cost} = Y{r.cogs:,}{warn}')
    month_total += r.cogs
if prev_month:
    print(f'    -> 月計: Y{month_total:,}')
