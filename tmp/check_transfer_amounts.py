"""振替テーブルの金額整合性チェック

各振替レコードについて、リンク元テーブルの金額と照合する。
- 楽天銀行: 入出金_円_ (abs value)
- PayPay銀行: お預かり金額 (abs value)
- Amazon出品アカウント明細: 金額 (abs value)
- 代行会社: ROUND(外貨金額 * 為替レート) (abs value)
- NTTファイナンスBizカード明細: SUM(ご利用金額) per batch (abs value)
"""
import sys
import sqlite3

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "C:/Users/ninni/nocodb/noco.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# ============================================================
# 1. Load all transfer records
# ============================================================
cur.execute('SELECT id, 振替日, 金額, メモ FROM "nc_opau___振替" ORDER BY id')
transfers = {r["id"]: dict(r) for r in cur.fetchall()}

# ============================================================
# 2. Load linked amounts from each source table
# ============================================================

# --- 楽天銀行 ---
cur.execute('''
    SELECT id, 取引日, "入出金_円_" AS amount, "nc_opau___振替_id" AS tid
    FROM "nc_opau___楽天銀行ビジネス口座入出金明細"
    WHERE "nc_opau___振替_id" IS NOT NULL
''')
rakuten_links = {}
for r in cur.fetchall():
    tid = r["tid"]
    rakuten_links.setdefault(tid, []).append({
        "id": r["id"], "date": r["取引日"], "amount": r["amount"], "table": "楽天銀行"
    })

# --- PayPay銀行 ---
cur.execute('''
    SELECT id, 操作日, "お預かり金額" AS amount, "nc_opau___振替_id" AS tid
    FROM "nc_opau___PayPay銀行入出金明細"
    WHERE "nc_opau___振替_id" IS NOT NULL
''')
paypay_links = {}
for r in cur.fetchall():
    tid = r["tid"]
    paypay_links.setdefault(tid, []).append({
        "id": r["id"], "date": r["操作日"], "amount": r["amount"], "table": "PayPay銀行"
    })

# --- Amazon出品アカウント明細 ---
cur.execute('''
    SELECT id, 取引日, 金額 AS amount, "nc_opau___振替_id" AS tid, entry_type
    FROM "nc_opau___Amazon出品アカウント明細"
    WHERE "nc_opau___振替_id" IS NOT NULL
''')
amazon_links = {}
for r in cur.fetchall():
    tid = r["tid"]
    amazon_links.setdefault(tid, []).append({
        "id": r["id"], "date": r["取引日"], "amount": r["amount"],
        "entry_type": r["entry_type"], "table": "Amazon"
    })

# --- 代行会社 ---
cur.execute('''
    SELECT id, 発生日, 外貨金額, 為替レート, 円残高, "nc_opau___振替_id" AS tid
    FROM "nc_opau___代行会社"
    WHERE "nc_opau___振替_id" IS NOT NULL
''')
agency_links = {}
for r in cur.fetchall():
    tid = r["tid"]
    fx = r["外貨金額"]
    rate = r["為替レート"]
    calc_amount = round(fx * rate) if fx is not None and rate is not None else None
    agency_links.setdefault(tid, []).append({
        "id": r["id"], "date": r["発生日"], "amount": calc_amount,
        "fx": fx, "rate": rate, "円残高": r["円残高"], "table": "代行会社"
    })

# --- NTTファイナンスBizカード明細 (batch: SUM per 振替_id) ---
cur.execute('''
    SELECT "nc_opau___振替_id" AS tid,
           SUM(ご利用金額) AS sum_usage,
           COUNT(*) AS cnt
    FROM "nc_opau___NTTファイナンスBizカード明細"
    WHERE "nc_opau___振替_id" IS NOT NULL
    GROUP BY "nc_opau___振替_id"
''')
ntt_links = {}
for r in cur.fetchall():
    tid = r["tid"]
    ntt_links[tid] = {
        "sum_usage": r["sum_usage"], "cnt": r["cnt"], "table": "NTT(batch)"
    }

conn.close()

# ============================================================
# 3. Compare amounts for each transfer
# ============================================================

mismatches = []
summary = {"total": 0, "matched": 0, "mismatched": 0, "single_link": 0, "no_link": 0}

print("=" * 100)
print("振替テーブル 金額整合性チェック")
print("=" * 100)
print()

for tid in sorted(transfers.keys()):
    t = transfers[tid]
    t_amount = t["金額"]
    t_date = t["振替日"]
    t_memo = t["メモ"] or ""

    # Collect all linked amounts
    linked = []

    # 楽天銀行 (1 record per transfer expected)
    for rec in rakuten_links.get(tid, []):
        linked.append({
            "source": "楽天銀行",
            "source_id": rec["id"],
            "amount": abs(rec["amount"]),
            "raw_amount": rec["amount"],
        })

    # PayPay銀行 (1 record per transfer expected)
    for rec in paypay_links.get(tid, []):
        linked.append({
            "source": "PayPay銀行",
            "source_id": rec["id"],
            "amount": abs(rec["amount"]),
            "raw_amount": rec["amount"],
        })

    # Amazon (1 record per transfer expected, DEPOSIT entries)
    for rec in amazon_links.get(tid, []):
        linked.append({
            "source": "Amazon",
            "source_id": rec["id"],
            "amount": abs(rec["amount"]),
            "raw_amount": rec["amount"],
        })

    # 代行会社 (1 record per transfer; amount = round(外貨金額 * 為替レート))
    for rec in agency_links.get(tid, []):
        linked.append({
            "source": "代行会社",
            "source_id": rec["id"],
            "amount": abs(rec["amount"]) if rec["amount"] is not None else None,
            "raw_amount": rec["amount"],
            "detail": f"fx={rec['fx']}, rate={rec['rate']}, calc={rec['amount']}",
        })

    # NTT (batch: sum of ご利用金額)
    if tid in ntt_links:
        ntt = ntt_links[tid]
        linked.append({
            "source": f"NTT(batch, {ntt['cnt']}件)",
            "source_id": None,
            "amount": abs(ntt["sum_usage"]),
            "raw_amount": ntt["sum_usage"],
        })

    summary["total"] += 1

    if len(linked) == 0:
        summary["no_link"] += 1
        continue

    if len(linked) < 2:
        summary["single_link"] += 1
        # Still check single link matches transfer amount
        single = linked[0]
        if single["amount"] is not None and single["amount"] != t_amount:
            mismatches.append({
                "transfer_id": tid, "date": t_date, "transfer_amount": t_amount,
                "memo": t_memo, "linked": linked, "type": "single_link_mismatch"
            })
            summary["mismatched"] += 1
        else:
            summary["matched"] += 1
        continue

    # 2+ links: check all amounts match transfer amount
    all_match = True
    for link in linked:
        if link["amount"] is None or link["amount"] != t_amount:
            all_match = False
            break

    if all_match:
        summary["matched"] += 1
    else:
        summary["mismatched"] += 1
        mismatches.append({
            "transfer_id": tid, "date": t_date, "transfer_amount": t_amount,
            "memo": t_memo, "linked": linked, "type": "amount_mismatch"
        })

# ============================================================
# 4. Report
# ============================================================

print(f"--- サマリー ---")
print(f"  振替レコード総数: {summary['total']}")
print(f"  リンクなし:       {summary['no_link']}")
print(f"  単一リンク:       {summary['single_link']}")
print(f"  金額一致:         {summary['matched']}")
print(f"  金額不一致:       {summary['mismatched']}")
print()

if mismatches:
    print("=" * 100)
    print(f"*** 金額不一致: {len(mismatches)} 件 ***")
    print("=" * 100)
    for m in mismatches:
        print()
        print(f"振替 id={m['transfer_id']}  日付={m['date']}  金額={m['transfer_amount']:,}  "
              f"メモ={m['memo'][:60]}")
        print(f"  タイプ: {m['type']}")
        for link in m["linked"]:
            raw = link["raw_amount"]
            abs_amt = link["amount"]
            diff = abs_amt - m["transfer_amount"] if abs_amt is not None else "N/A"
            detail = link.get("detail", "")
            flag = " *** MISMATCH ***" if abs_amt != m["transfer_amount"] else " OK"
            print(f"  {link['source']:<20} id={str(link['source_id']):<6} "
                  f"raw={raw:<12} abs={abs_amt:<12} diff={diff}{flag}")
            if detail:
                print(f"    {detail}")
else:
    print("全ての振替レコードの金額が一致しています。")

print()
print("=" * 100)
print("チェック完了")
