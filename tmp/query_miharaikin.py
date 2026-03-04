# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery
client = bigquery.Client(project='main-project-477501')

print('=== 未払金 journal_entries detail (FY2023) ===')
q5 = """SELECT journal_date, entry_side, amount_jpy, description, source_table, source_id
FROM accounting.general_ledger
WHERE account_name = '未払金' AND fiscal_year = 2023
ORDER BY journal_date, source_id"""

total_dr = 0
total_cr = 0
for row in client.query(q5).result():
    if row.entry_side == 'debit':
        total_dr += row.amount_jpy
    else:
        total_cr += row.amount_jpy
    print(f'  {row.journal_date} | {row.entry_side} | {row.amount_jpy:,} | {row.description} | {row.source_table}:{row.source_id}')
print(f'  Total debit: {total_dr:,}, Total credit: {total_cr:,}, Balance(Cr-Dr): {total_cr - total_dr:,}')
