"""
セラーセントラル支払い履歴 vs BQ仕訳の完全突合
Amazon出品アカウントの残高推移を時系列で追跡
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

print("=" * 100)
print("【1】Settlement別 精算額一覧（セラセン「支払い」画面と同じデータ）")
print("=" * 100)

q1 = """
WITH settlement_net AS (
  SELECT
    s.settlement_id,
    DATE(s.issue_date) AS deposit_date,
    SUM(CASE
      WHEN d.account_item_id = 1008403397 AND d.entry_side = 'debit' THEN d.amount
      WHEN d.account_item_id = 1008403397 AND d.entry_side = 'credit' THEN -d.amount
      ELSE 0
    END) AS net_to_bank,
    SUM(CASE
      WHEN d.account_item_id = 1319647981 THEN d.amount  -- 売上高
      WHEN d.account_item_id = 1321253559 THEN d.amount  -- 雑収入
      ELSE 0
    END) AS revenue,
    SUM(CASE
      WHEN d.entry_side = 'debit'
           AND d.account_item_id NOT IN (1008403397, 1321253735)
           THEN d.amount
      ELSE 0
    END) AS expenses
  FROM `main-project-477501.accounting.settlement_journal_payload_view` s
  CROSS JOIN UNNEST(s.json_details) AS d
  WHERE EXTRACT(YEAR FROM s.issue_date) = 2023
  GROUP BY s.settlement_id, s.issue_date
)
SELECT * FROM settlement_net
ORDER BY deposit_date
"""

rows1 = list(client.query(q1).result())
print(f"\n{'No':>3} {'入金日':>12} {'settlement_id':>16} {'精算額':>10} {'収入':>10} {'経費':>10}")
print("-" * 75)
total_net = 0
for i, r in enumerate(rows1, 1):
    total_net += r.net_to_bank
    print(f"{i:3d} {str(r.deposit_date):>12} {r.settlement_id:>16} {r.net_to_bank:>10,} {r.revenue:>10,} {r.expenses:>10,}")
print("-" * 75)
print(f"{'合計':>35} {total_net:>10,}")

print("\n" + "=" * 100)
print("【2】Amazon出品アカウント 全仕訳と残高推移")
print("    （freee現預金レポートと同じ内容）")
print("=" * 100)

# accounting.journal_entries は entry_side / account_name 形式
# Amazon出品アカウントに対するdebit=入金(残高+), credit=出金(残高-)
q2 = """
WITH amazon_entries AS (
  -- Amazon出品アカウントが借方(入金)の仕訳: settlement精算時
  SELECT
    je.journal_date,
    je.source_table,
    je.source_id,
    je.description,
    je.amount_jpy AS balance_effect,
    'settlement精算' AS entry_type
  FROM `main-project-477501.accounting.journal_entries` je
  WHERE je.account_name = 'Amazon出品アカウント'
    AND je.entry_side = 'debit'
    AND je.fiscal_year = 2023

  UNION ALL

  -- Amazon出品アカウントが貸方(出金)の仕訳: 楽天銀行入金時
  SELECT
    je.journal_date,
    je.source_table,
    je.source_id,
    je.description,
    -je.amount_jpy AS balance_effect,
    '楽天銀行入金' AS entry_type
  FROM `main-project-477501.accounting.journal_entries` je
  WHERE je.account_name = 'Amazon出品アカウント'
    AND je.entry_side = 'credit'
    AND je.fiscal_year = 2023
)
SELECT * FROM amazon_entries
ORDER BY journal_date, entry_type DESC
"""

rows2 = list(client.query(q2).result())
print(f"\n{'日付':>12} {'増減':>10} {'残高':>10} {'種別':>14} {'摘要'}")
print("-" * 100)
balance = 0
zero_count = 0
settlement_entries = []
bank_entries = []

for r in rows2:
    balance += r.balance_effect
    marker = ""
    if balance == 0:
        zero_count += 1
        marker = " ★残高ゼロ"

    desc = (r.description or "")[:45]
    print(f"{str(r.journal_date):>12} {r.balance_effect:>+10,} {balance:>10,} {r.entry_type:>14} {desc}{marker}")

    if r.entry_type == 'settlement精算':
        settlement_entries.append(r)
    else:
        bank_entries.append(r)

print("-" * 100)
print(f"最終残高: {balance:,}円  |  残高ゼロ到達: {zero_count}回")
print(f"精算仕訳: {len(settlement_entries)}件  |  銀行入金仕訳: {len(bank_entries)}件")

print("\n" + "=" * 100)
print("【3】セラーセントラル精算 ↔ BQ仕訳 ↔ 楽天銀行入金 の3点突合")
print("=" * 100)

# セラセンの生データ(stg_sp_settlement)からsettlement毎のtotalを算出
q3 = """
WITH raw_settlement AS (
  SELECT
    settlement_id,
    DATE(deposit_date) AS deposit_date,
    SUM(amount) AS raw_total
  FROM `main-project-477501.analytics.stg_sp_settlement`
  WHERE EXTRACT(YEAR FROM deposit_date) = 2023
  GROUP BY settlement_id, deposit_date
),
journal_net AS (
  SELECT
    s.settlement_id,
    SUM(CASE
      WHEN d.account_item_id = 1008403397 AND d.entry_side = 'debit' THEN d.amount
      WHEN d.account_item_id = 1008403397 AND d.entry_side = 'credit' THEN -d.amount
      ELSE 0
    END) AS bq_net
  FROM `main-project-477501.accounting.settlement_journal_payload_view` s
  CROSS JOIN UNNEST(s.json_details) AS d
  WHERE EXTRACT(YEAR FROM s.issue_date) = 2023
  GROUP BY s.settlement_id
),
bank_entries AS (
  SELECT
    journal_date,
    amount_jpy,
    source_id,
    description
  FROM `main-project-477501.accounting.journal_entries`
  WHERE source_table = 'rakuten_bank'
    AND account_name = 'Amazon出品アカウント'
    AND entry_side = 'credit'
    AND fiscal_year = 2023
)
SELECT
  rs.settlement_id,
  rs.deposit_date,
  rs.raw_total AS sellercentral_amount,
  jn.bq_net AS bq_journal_amount,
  be.amount_jpy AS bank_received,
  be.journal_date AS bank_date,
  CASE
    WHEN jn.bq_net IS NULL THEN '仕訳なし'
    WHEN rs.raw_total = jn.bq_net THEN '✓'
    ELSE CONCAT('差異:', CAST(rs.raw_total - jn.bq_net AS STRING))
  END AS sc_vs_bq,
  CASE
    WHEN jn.bq_net > 0 AND be.amount_jpy IS NOT NULL AND jn.bq_net = be.amount_jpy THEN '✓入金済'
    WHEN jn.bq_net > 0 AND be.amount_jpy IS NULL THEN '未入金?'
    WHEN jn.bq_net <= 0 THEN 'マイナス精算'
    ELSE CONCAT('差異:', CAST(COALESCE(jn.bq_net,0) - COALESCE(be.amount_jpy,0) AS STRING))
  END AS bank_status
FROM raw_settlement rs
LEFT JOIN journal_net jn ON CAST(rs.settlement_id AS STRING) = CAST(jn.settlement_id AS STRING)
LEFT JOIN bank_entries be ON jn.bq_net = be.amount_jpy
  AND ABS(DATE_DIFF(rs.deposit_date, be.journal_date, DAY)) <= 14
ORDER BY rs.deposit_date
"""

rows3 = list(client.query(q3).result())
print(f"\n{'入金日':>12} {'settlement_id':>16} {'セラセン':>10} {'BQ仕訳':>10} {'銀行入金':>10} {'SC↔BQ':>8} {'銀行照合':>12}")
print("-" * 100)
for r in rows3:
    sc = f"{r.sellercentral_amount:>10,}"
    bq = f"{r.bq_journal_amount:>10,}" if r.bq_journal_amount is not None else "      N/A"
    bank = f"{r.bank_received:>10,}" if r.bank_received is not None else "         -"
    print(f"{str(r.deposit_date):>12} {r.settlement_id:>16} {sc} {bq} {bank} {r.sc_vs_bq:>8} {r.bank_status:>12}")

print("\n" + "=" * 100)
print("【4】精算1件の仕訳内訳例: settlement 11490122433")
print("    （セラセン: 2023/8/28～2023/9/11、精算額 ¥13,240）")
print("=" * 100)

q4 = """
SELECT
  d.entry_side,
  d.amount,
  d.tax_code,
  d.description
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) AS d
WHERE s.settlement_id = 11490122433
ORDER BY
  CASE d.entry_side WHEN 'debit' THEN 0 ELSE 1 END,
  d.amount DESC
"""

rows4 = list(client.query(q4).result())
print(f"\n{'借/貸':>6} {'金額':>10} {'税区分':>8} {'摘要'}")
print("-" * 80)
debit_total = 0
credit_total = 0
for r in rows4:
    side = "借方" if r.entry_side == 'debit' else "貸方"
    if r.entry_side == 'debit':
        debit_total += r.amount
    else:
        credit_total += r.amount
    desc = (r.description or "")[:50]
    print(f"{side:>6} {r.amount:>10,} {r.tax_code:>8} {desc}")
print("-" * 80)
print(f"借方合計: {debit_total:,}  貸方合計: {credit_total:,}  差額: {debit_total - credit_total:,}")

print("\n" + "=" * 100)
print("【5】マイナス精算の仕組み（繰越金が発生するケース）")
print("=" * 100)

q5 = """
SELECT
  s.settlement_id,
  DATE(s.issue_date) AS deposit_date,
  SUM(CASE
    WHEN d.account_item_id = 1008403397 AND d.entry_side = 'debit' THEN d.amount
    WHEN d.account_item_id = 1008403397 AND d.entry_side = 'credit' THEN -d.amount
    ELSE 0
  END) AS net_amount
FROM `main-project-477501.accounting.settlement_journal_payload_view` s
CROSS JOIN UNNEST(s.json_details) AS d
WHERE EXTRACT(YEAR FROM s.issue_date) = 2023
GROUP BY s.settlement_id, s.issue_date
HAVING SUM(CASE
    WHEN d.account_item_id = 1008403397 AND d.entry_side = 'debit' THEN d.amount
    WHEN d.account_item_id = 1008403397 AND d.entry_side = 'credit' THEN -d.amount
    ELSE 0
  END) < 0
ORDER BY deposit_date
"""

rows5 = list(client.query(q5).result())
print(f"\nマイナス精算（経費が売上を上回った回）: {len(rows5)}件")
print(f"{'入金日':>12} {'settlement_id':>16} {'精算額':>10}")
print("-" * 45)
neg_total = 0
for r in rows5:
    neg_total += r.net_amount
    print(f"{str(r.deposit_date):>12} {r.settlement_id:>16} {r.net_amount:>10,}")
print("-" * 45)
print(f"マイナス合計: {neg_total:,}円")
print(f"\nこれらは「経費 > 売上」のため振込なし。マイナス分は次回精算に繰り越し。")
print(f"セラセンでは「繰越金額」として表示されます。")
