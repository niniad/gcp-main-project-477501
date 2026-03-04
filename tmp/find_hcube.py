"""Search NocoDB for エイチキューブ (H CUBE / セールモンスター) bank deposits."""
import sqlite3
import sys

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "C:/Users/ninni/nocodb/noco.db"

# Search patterns (half-width kana, full-width kana, romaji variations)
PATTERNS = [
    "%エイチキ%",
    "%ｴｲﾁｷ%",
    "%CUBE%",
    "%cube%",
    "%H CUBE%",
    "%セールモンスター%",
    "%エイチキユ%",  # katakana variant seen in data
]


def build_like_clause(column: str) -> str:
    """Build OR chain of LIKE clauses for all patterns."""
    clauses = " OR ".join(f'"{column}" LIKE ?' for _ in PATTERNS)
    return f"({clauses})"


def fiscal_year(date_str: str) -> str:
    """Return fiscal year label from date string (calendar year = fiscal year)."""
    if not date_str:
        return "unknown"
    year = date_str[:4]
    return f"FY{year}"


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    all_results = []

    # ─── 1. 楽天銀行ビジネス口座入出金明細 ───
    print("=" * 80)
    print("■ 楽天銀行ビジネス口座入出金明細")
    print("=" * 80)

    rakuten_table = "nc_opau___楽天銀行ビジネス口座入出金明細"
    # Search in 入出金先内容 and 品目
    where = f'{build_like_clause("入出金先内容")} OR {build_like_clause("品目")}'
    params = PATTERNS * 2  # once for each column

    sql = f"""
        SELECT
            r.id,
            r."取引日" AS transaction_date,
            r."入出金_円_" AS amount,
            r."入出金先内容" AS description,
            r."品目" AS item,
            r."nc_opau___freee勘定科目_id" AS account_id,
            a."勘定科目" AS account_name,
            r."nc_opau___振替_id" AS transfer_id,
            r."税区分" AS tax_category
        FROM "{rakuten_table}" r
        LEFT JOIN "nc_opau___freee勘定科目" a ON r."nc_opau___freee勘定科目_id" = a.id
        WHERE {where}
        ORDER BY r."取引日"
    """
    cur.execute(sql, params)
    rows = cur.fetchall()

    if not rows:
        print("  該当なし\n")
    else:
        print(f"  {len(rows)} 件ヒット\n")
        print(f"  {'ID':>4}  {'取引日':<12} {'金額':>10}  {'入出金先内容':<30} {'勘定科目':<20} {'振替ID':>6}  {'税区分'}")
        print(f"  {'─'*4}  {'─'*12} {'─'*10}  {'─'*30} {'─'*20} {'─'*6}  {'─'*10}")
        for r in rows:
            tid = r["transfer_id"] if r["transfer_id"] else "-"
            acct = r["account_name"] if r["account_name"] else "-"
            tax = r["tax_category"] if r["tax_category"] else "-"
            print(f"  {r['id']:>4}  {r['transaction_date']:<12} {r['amount']:>10,}  {(r['description'] or ''):<30} {acct:<20} {str(tid):>6}  {tax}")
            all_results.append(("楽天銀行", r["transaction_date"], r["amount"], r["description"], acct, tid))

    # ─── 2. PayPay銀行入出金明細 ───
    print()
    print("=" * 80)
    print("■ PayPay銀行入出金明細")
    print("=" * 80)

    paypay_table = "nc_opau___PayPay銀行入出金明細"
    # Search in 摘要 and メモ
    where = f'{build_like_clause("摘要")} OR {build_like_clause("メモ")}'
    params = PATTERNS * 2

    sql = f"""
        SELECT
            p.id,
            p."操作日" AS transaction_date,
            p."お預かり金額" AS amount,
            p."摘要" AS description,
            p."メモ" AS memo,
            p."nc_opau___freee勘定科目_id" AS account_id,
            a."勘定科目" AS account_name,
            p."nc_opau___振替_id" AS transfer_id,
            p."税区分" AS tax_category
        FROM "{paypay_table}" p
        LEFT JOIN "nc_opau___freee勘定科目" a ON p."nc_opau___freee勘定科目_id" = a.id
        WHERE {where}
        ORDER BY p."操作日"
    """
    cur.execute(sql, params)
    rows = cur.fetchall()

    if not rows:
        print("  該当なし\n")
    else:
        print(f"  {len(rows)} 件ヒット\n")
        print(f"  {'ID':>4}  {'操作日':<12} {'金額':>10}  {'摘要':<30} {'勘定科目':<20} {'振替ID':>6}  {'税区分'}")
        print(f"  {'─'*4}  {'─'*12} {'─'*10}  {'─'*30} {'─'*20} {'─'*6}  {'─'*10}")
        for r in rows:
            tid = r["transfer_id"] if r["transfer_id"] else "-"
            acct = r["account_name"] if r["account_name"] else "-"
            tax = r["tax_category"] if r["tax_category"] else "-"
            print(f"  {r['id']:>4}  {r['transaction_date']:<12} {r['amount']:>10,}  {(r['description'] or ''):<30} {acct:<20} {str(tid):>6}  {tax}")
            all_results.append(("PayPay銀行", r["transaction_date"], r["amount"], r["description"], acct, tid))

    # ─── 3. 年度別集計 ───
    print()
    print("=" * 80)
    print("■ 年度別集計 (エイチキューブ入金)")
    print("=" * 80)

    fy_totals: dict[str, dict] = {}
    for bank, date, amount, desc, acct, tid in all_results:
        fy = fiscal_year(date)
        if fy not in fy_totals:
            fy_totals[fy] = {"count": 0, "total": 0, "banks": {}}
        fy_totals[fy]["count"] += 1
        fy_totals[fy]["total"] += (amount or 0)
        bank_key = bank
        if bank_key not in fy_totals[fy]["banks"]:
            fy_totals[fy]["banks"][bank_key] = {"count": 0, "total": 0}
        fy_totals[fy]["banks"][bank_key]["count"] += 1
        fy_totals[fy]["banks"][bank_key]["total"] += (amount or 0)

    print(f"\n  {'年度':<8} {'件数':>4} {'合計金額':>12}  内訳")
    print(f"  {'─'*8} {'─'*4} {'─'*12}  {'─'*40}")
    for fy in sorted(fy_totals.keys()):
        data = fy_totals[fy]
        banks_str = ", ".join(
            f"{b}: {d['count']}件 {d['total']:,}円"
            for b, d in sorted(data["banks"].items())
        )
        print(f"  {fy:<8} {data['count']:>4} {data['total']:>12,}  {banks_str}")

    grand_total = sum(d["total"] for d in fy_totals.values())
    grand_count = sum(d["count"] for d in fy_totals.values())
    print(f"  {'─'*8} {'─'*4} {'─'*12}")
    print(f"  {'合計':<8} {grand_count:>4} {grand_total:>12,}")

    conn.close()


if __name__ == "__main__":
    main()
