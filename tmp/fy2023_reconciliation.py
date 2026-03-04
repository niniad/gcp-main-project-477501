import sys
sys.stdout.reconfigure(encoding='utf-8')

from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

queries = {
    "1. All BS account balances at FY2023 year-end": """
SELECT account_name, SUM(signed_amount) as balance
FROM accounting.general_ledger
WHERE fiscal_year = 2023
GROUP BY account_name
ORDER BY account_name
""",
    "2. ESPRIME detail transactions FY2023": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = 'ESPRIME' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "3. 事業主借 detail transactions FY2023": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '事業主借' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "4. 未払金 detail transactions FY2023": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '未払金' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "5. 楽天銀行 detail transactions FY2023": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '楽天銀行' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "6. 売上高 detail": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '売上高' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "7. 雑収入 detail": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '雑収入' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "8. Manual journal entries (all 6)": """
SELECT * FROM nocodb.manual_journal_entries ORDER BY nocodb_id
""",
    "9. 雑費 detail": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '雑費' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "10. 消耗品費 detail": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '消耗品費' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "11. 支払手数料 detail": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '支払手数料' AND fiscal_year = 2023
ORDER BY journal_date
""",
    "12. 為替差損益 detail": """
SELECT journal_date, entry_side, amount_jpy, description, source_table
FROM accounting.general_ledger
WHERE account_name = '為替差損益' AND fiscal_year = 2023
ORDER BY journal_date
""",
}

for title, sql in queries.items():
    print("=" * 80)
    print(f"  {title}")
    print("=" * 80)
    try:
        result = client.query(sql).result()
        rows = list(result)
        if not rows:
            print("  (no rows)")
        else:
            # Print header
            headers = [field.name for field in result.schema]
            print("  " + " | ".join(headers))
            print("  " + "-" * (len(" | ".join(headers)) + 10))
            for row in rows:
                vals = []
                for h in headers:
                    v = row[h]
                    if v is None:
                        vals.append("NULL")
                    else:
                        vals.append(str(v))
                print("  " + " | ".join(vals))
            print(f"\n  ({len(rows)} rows)")
    except Exception as e:
        print(f"  ERROR: {e}")
    print()
