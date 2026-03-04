"""
Amazon unlinked DEPOSIT records detail check
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from google.cloud import bigquery

client = bigquery.Client(project='main-project-477501')

# 1. unlinked DEPOSIT records detail
print('=== Amazon unlinked DEPOSIT detail ===')
q1 = r"""
SELECT
  a.nocodb_id,
  a.transaction_date,
  a.entry_type,
  a.amount,
  a.description,
  a.`freee勘定科目_id`,
  ac.account_name,
  a.`振替_id`
FROM `main-project-477501.nocodb.amazon_account_statements` a
LEFT JOIN `main-project-477501.nocodb.account_items` ac
  ON a.`freee勘定科目_id` = ac.nocodb_id
WHERE a.`振替_id` IS NULL
  AND a.entry_type = 'DEPOSIT'
  AND a.`freee勘定科目_id` IS NOT NULL
ORDER BY a.transaction_date
"""
rows = client.query(q1).result()
total = 0
entries = []
for r in rows:
    desc = (r.description or '')[:60]
    print(f"  id={r.nocodb_id:>3} {r.transaction_date} {r.entry_type} {r.amount:>10,}  acct={r['freee勘定科目_id']}({r.account_name})  {desc}")
    total += r.amount
    entries.append(r.nocodb_id)
print(f"\n  total: {total:,}  count: {len(entries)}")
print(f"  ids: {entries}")

# 2. Check journal_entries for self-referencing Amazon entries
print('\n=== journal_entries: debit=credit (self-reference) ===')
q2 = r"""
SELECT
  je.entry_id,
  je.entry_date,
  je.debit_account,
  je.credit_account,
  je.amount
FROM `main-project-477501.accounting.journal_entries` je
WHERE je.debit_account = je.credit_account
  AND je.entry_id LIKE 'amazon_%'
ORDER BY je.entry_date
"""
try:
    rows2 = client.query(q2).result()
    cnt = 0
    total2 = 0
    for r in rows2:
        print(f"  {r.entry_id} {r.entry_date} Dr=Cr={r.debit_account} {r.amount:,}")
        cnt += 1
        total2 += r.amount
    print(f"\n  count: {cnt}, total: {total2:,}")
except Exception as e:
    print(f"  error: {e}")

# 3. P/L effect if we null these out
print('\n=== P/L impact of nulling these 16 entries ===')
print('  These are self-referencing wash entries -> P/L impact = 0')
print('  (debit=Amazon, credit=Amazon -> net zero)')
print(f'  Amazon balance change if excluded: {total:+,}')
