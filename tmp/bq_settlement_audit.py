"""
FY2023 Amazon Settlement Audit - BigQuery
全settlement関連データを取得して全体像を把握する
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

def run_query(title, sql):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")
    try:
        result = client.query(sql).result()
        rows = list(result)
        if not rows:
            print("  (結果なし)")
            return rows
        # Print header
        fields = [f.name for f in result.schema]
        print("  " + " | ".join(fields))
        print("  " + "-" * (len(" | ".join(fields)) + 10))
        for row in rows:
            vals = []
            for f in fields:
                v = row[f]
                if v is None:
                    vals.append("NULL")
                else:
                    vals.append(str(v))
            print("  " + " | ".join(vals))
        print(f"\n  ({len(rows)} 行)")
        return rows
    except Exception as e:
        print(f"  エラー: {e}")
        return []


# Step 0: テーブル・ビュー一覧（settlement/amazon/journal関連）
print("\n" + "#"*80)
print("# STEP 0: settlement/amazon/journal 関連テーブル・ビュー一覧")
print("#"*80)

run_query("nocodb データセット - settlement/amazon 関連テーブル", """
SELECT table_name, table_type
FROM `main-project-477501.nocodb.INFORMATION_SCHEMA.TABLES`
WHERE LOWER(table_name) LIKE '%settle%' OR LOWER(table_name) LIKE '%amazon%'
ORDER BY table_name
""")

run_query("accounting データセット - 全テーブル/ビュー", """
SELECT table_name, table_type
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name
""")

run_query("analytics データセット - settlement 関連", """
SELECT table_name, table_type
FROM `main-project-477501.analytics.INFORMATION_SCHEMA.TABLES`
WHERE LOWER(table_name) LIKE '%settle%' OR LOWER(table_name) LIKE '%stg_sp%'
ORDER BY table_name
""")

run_query("sp_api_external データセット - テーブル一覧", """
SELECT table_name, table_type
FROM `main-project-477501.sp_api_external.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name
""")


# Step 1: stg_sp_settlement（精算レポート生データ）のFY2023サマリ
print("\n" + "#"*80)
print("# STEP 1: stg_sp_settlement FY2023 サマリ")
print("#"*80)

run_query("settlement_id別のサマリ（FY2023 posted_date基準）", """
SELECT
  settlement_id,
  MIN(posted_date) AS min_posted_date,
  MAX(posted_date) AS max_posted_date,
  COUNT(*) AS row_count,
  SUM(amount) AS total_amount,
  COUNT(DISTINCT transaction_type) AS distinct_tx_types,
  COUNT(DISTINCT amount_type) AS distinct_amount_types
FROM `main-project-477501.analytics.stg_sp_settlement`
WHERE posted_date >= '2023-01-01' AND posted_date < '2024-01-01'
GROUP BY settlement_id
ORDER BY min_posted_date
""")

run_query("transaction_type × amount_type 別集計（FY2023）", """
SELECT
  transaction_type,
  amount_type,
  amount_description,
  COUNT(*) AS row_count,
  SUM(amount) AS total_amount,
  SUM(quantity_purchased) AS total_qty
FROM `main-project-477501.analytics.stg_sp_settlement`
WHERE posted_date >= '2023-01-01' AND posted_date < '2024-01-01'
GROUP BY transaction_type, amount_type, amount_description
ORDER BY transaction_type, amount_type, amount_description
""")


# Step 2: settlement_journal 関連ビューの確認
print("\n" + "#"*80)
print("# STEP 2: accounting.journal_entries から amazon_settlement の FY2023 仕訳")
print("#"*80)

run_query("amazon_settlement 仕訳 - 勘定科目別集計（FY2023）", """
SELECT
  account_name,
  entry_side,
  COUNT(*) AS row_count,
  SUM(amount_jpy) AS total_amount
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'amazon_settlement'
  AND fiscal_year = 2023
GROUP BY account_name, entry_side
ORDER BY account_name, entry_side
""")

run_query("amazon_settlement 仕訳 - 月別合計（FY2023）", """
SELECT
  FORMAT_DATE('%Y-%m', journal_date) AS year_month,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) AS total_debit,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS total_credit,
  COUNT(*) AS row_count
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'amazon_settlement'
  AND fiscal_year = 2023
GROUP BY year_month
ORDER BY year_month
""")

run_query("amazon_settlement 仕訳 - source_id別（settlement単位、FY2023）", """
SELECT
  source_id,
  MIN(journal_date) AS journal_date,
  SUM(CASE WHEN entry_side = 'debit' THEN amount_jpy ELSE 0 END) AS total_debit,
  SUM(CASE WHEN entry_side = 'credit' THEN amount_jpy ELSE 0 END) AS total_credit,
  COUNT(*) AS row_count
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'amazon_settlement'
  AND fiscal_year = 2023
GROUP BY source_id
ORDER BY journal_date
""")


# Step 3: settlement_journal_payload_view があれば確認
print("\n" + "#"*80)
print("# STEP 3: settlement_journal_payload_view（FY2023）")
print("#"*80)

run_query("settlement_journal_payload_view FY2023 存在確認", """
SELECT table_name, table_type
FROM `main-project-477501.accounting.INFORMATION_SCHEMA.TABLES`
WHERE LOWER(table_name) LIKE '%settlement%journal%'
""")

# Try to query it
run_query("settlement_journal_payload_view（FY2023、先頭20件）", """
SELECT *
FROM `main-project-477501.accounting.settlement_journal_payload_view`
WHERE EXTRACT(YEAR FROM issue_date) = 2023
ORDER BY issue_date
LIMIT 20
""")


# Step 4: sp_api_external の精算レポート生データ
print("\n" + "#"*80)
print("# STEP 4: sp_api_external 精算レポート生データ（FY2023 deposit_date基準）")
print("#"*80)

run_query("sp_api_external settlement テーブルのカラム確認", """
SELECT column_name, data_type
FROM `main-project-477501.sp_api_external.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name LIKE '%settlement%'
ORDER BY table_name, ordinal_position
LIMIT 50
""")

# Try common table names
for tbl in ['settlement-report-flat-file-settlement-7', 'settlement_report', 'settlements']:
    run_query(f"sp_api_external.`{tbl}` deposit_date別サマリ（FY2023）", f"""
    SELECT
      settlement_id,
      deposit_date,
      total_amount,
      COUNT(*) AS row_count
    FROM `main-project-477501.sp_api_external.`{tbl}``
    WHERE deposit_date >= '2023-01-01' AND deposit_date < '2024-01-01'
    GROUP BY settlement_id, deposit_date, total_amount
    ORDER BY deposit_date
    LIMIT 30
    """)


# Step 5: P&L への影響確認
print("\n" + "#"*80)
print("# STEP 5: amazon_settlement の P&L 影響（FY2023）")
print("#"*80)

run_query("amazon_settlement P&L寄与（FY2023）", """
SELECT
  account_name,
  small_category,
  entry_side,
  SUM(amount_jpy) AS total_amount,
  SUM(pl_contribution) AS total_pl_contribution
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE source_table = 'amazon_settlement'
  AND fiscal_year = 2023
GROUP BY account_name, small_category, entry_side
ORDER BY account_name, entry_side
""")

run_query("P&L合計 - amazon_settlement（FY2023）", """
SELECT
  SUM(pl_contribution) AS total_pl_from_amazon
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE source_table = 'amazon_settlement'
  AND fiscal_year = 2023
""")

run_query("P&L合計 - 全ソース比較（FY2023）", """
SELECT
  source_table,
  SUM(pl_contribution) AS total_pl,
  COUNT(*) AS row_count
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = 2023
GROUP BY source_table
ORDER BY total_pl DESC
""")


# Step 6: 楽天銀行のAmazon入金との照合
print("\n" + "#"*80)
print("# STEP 6: 楽天銀行 Amazon入金 vs Settlement deposit（FY2023）")
print("#"*80)

run_query("楽天銀行 Amazon関連入金（FY2023）", """
SELECT
  journal_date,
  description,
  amount_jpy,
  source_id
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'rakuten_bank'
  AND fiscal_year = 2023
  AND entry_side = 'debit'
  AND (LOWER(description) LIKE '%amazon%' OR LOWER(description) LIKE '%ａｍａ%' OR LOWER(description) LIKE '%アマゾン%')
ORDER BY journal_date
""")


# Step 7: 全仕訳データの amazon_settlement 詳細（先頭50行）
print("\n" + "#"*80)
print("# STEP 7: amazon_settlement 仕訳詳細（FY2023、先頭50行）")
print("#"*80)

run_query("amazon_settlement 仕訳詳細（先頭50行）", """
SELECT
  source_id,
  journal_date,
  entry_side,
  account_name,
  amount_jpy,
  description
FROM `main-project-477501.accounting.journal_entries`
WHERE source_table = 'amazon_settlement'
  AND fiscal_year = 2023
ORDER BY journal_date, source_id, entry_side, account_name
LIMIT 50
""")


print("\n" + "#"*80)
print("# 完了")
print("#"*80)
