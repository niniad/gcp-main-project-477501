"""
accounting.esprime_balance_view を作成
ESPRIME 元残高を外貨金額の累積和から自動計算し、手動入力値と比較できるVIEW
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')
VIEW_ID = 'main-project-477501.accounting.esprime_balance_view'

view_sql = r"""
WITH esprime AS (
  SELECT
    nocodb_id,
    SAFE.PARSE_DATE('%Y-%m-%d', transaction_date) AS transaction_date,
    memo,
    amount_foreign,
    balance_foreign   AS manual_balance_cny,
    exchange_rate,
    balance_jpy       AS manual_balance_jpy,
    payment_account,
    `振替_id`         AS transfer_id
  FROM `main-project-477501.nocodb.agency_transactions`
  WHERE payment_account = 'ESPRIME'
)
SELECT
  nocodb_id,
  transaction_date,
  memo,
  amount_foreign,
  SUM(COALESCE(amount_foreign, 0)) OVER (
    ORDER BY transaction_date, nocodb_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                                           AS calc_balance_cny,
  manual_balance_cny,
  ROUND(
    SUM(COALESCE(amount_foreign, 0)) OVER (
      ORDER BY transaction_date, nocodb_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) - COALESCE(manual_balance_cny, 0), 6
  )                                           AS balance_diff,
  exchange_rate,
  ROUND(
    SUM(COALESCE(amount_foreign, 0)) OVER (
      ORDER BY transaction_date, nocodb_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) * exchange_rate, 0
  )                                           AS current_jpy_value,
  manual_balance_jpy,
  transfer_id
FROM esprime
ORDER BY transaction_date, nocodb_id
"""

view = bigquery.Table(VIEW_ID)
view.view_query = view_sql

try:
    client.delete_table(VIEW_ID)
    print(f'既存VIEW削除: {VIEW_ID}')
except Exception:
    pass

created = client.create_table(view)
print(f'VIEW作成完了: {created.table_id}')

# 直近15件確認
print('\n=== 直近15件（差異チェック）===')
q_check = """
SELECT
  nocodb_id,
  transaction_date,
  SUBSTR(memo, 1, 35) AS memo,
  amount_foreign,
  ROUND(calc_balance_cny, 3)   AS calc_cny,
  ROUND(manual_balance_cny, 3) AS manual_cny,
  balance_diff,
  exchange_rate,
  current_jpy_value,
  manual_balance_jpy
FROM `main-project-477501.accounting.esprime_balance_view`
ORDER BY transaction_date DESC, nocodb_id DESC
LIMIT 15
"""
print(f'{"id":>4} {"日付":12} {"外貨金額":>10} {"計算元残":>12} {"手動元残":>12} {"差異":>8} {"rate":>6} {"計算円残":>10} {"手動円残":>10}')
for r in client.query(q_check).result():
    diff_mark = ' !!!' if r.balance_diff and abs(r.balance_diff) > 0.01 else ''
    m_cny = r.manual_balance_cny or 0
    m_jpy = r.manual_balance_jpy or 0
    print(f'{r.nocodb_id:>4} {str(r.transaction_date):12} {(r.amount_foreign or 0):>10.2f} '
          f'{r.calc_cny:>12.3f} {m_cny:>12.3f} {r.balance_diff:>8.3f} '
          f'{r.exchange_rate:>6.2f} {r.current_jpy_value:>10,.0f} {m_jpy:>10,.0f}{diff_mark}')

# 最新残高
print('\n=== 最新残高サマリー ===')
q_latest = """
SELECT
  calc_balance_cny, manual_balance_cny, balance_diff,
  exchange_rate, current_jpy_value, manual_balance_jpy, transaction_date
FROM `main-project-477501.accounting.esprime_balance_view`
ORDER BY transaction_date DESC, nocodb_id DESC
LIMIT 1
"""
for r in client.query(q_latest).result():
    print(f'  最終取引日: {r.transaction_date}')
    print(f'  計算元残高: {r.calc_balance_cny:.3f} 元')
    print(f'  手動元残高: {(r.manual_balance_cny or 0):.3f} 元')
    print(f'  差異:       {r.balance_diff:.6f} 元')
    print(f'  適用レート: {r.exchange_rate:.4f} 円/元')
    print(f'  計算円換算: ¥{r.current_jpy_value:,.0f}  (自動)')
    print(f'  手動円残高: ¥{(r.manual_balance_jpy or 0):,.0f}  (NocoDB入力値)')

# 差異のある行をすべて表示
print('\n=== 差異ありの行（balance_diff != 0）===')
q_diff = """
SELECT nocodb_id, transaction_date, SUBSTR(memo,1,40) AS memo,
  amount_foreign, calc_balance_cny, manual_balance_cny, balance_diff
FROM `main-project-477501.accounting.esprime_balance_view`
WHERE ABS(balance_diff) > 0.01
ORDER BY transaction_date, nocodb_id
"""
cnt = 0
for r in client.query(q_diff).result():
    print(f'  id={r.nocodb_id} {r.transaction_date} calc={r.calc_balance_cny:.3f} manual={r.manual_balance_cny:.3f} diff={r.balance_diff:.3f}')
    cnt += 1
if cnt == 0:
    print('  差異なし ✅')
