"""代行会社 (agency_transactions) テーブルの詳細調査"""
import sys
import sqlite3
import json

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "C:/Users/ninni/nocodb/noco.db"
TABLE_NAME = "nc_opau___代行会社"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# ============================================================
# 1. PRAGMA table_info — 物理スキーマ
# ============================================================
print("=" * 80)
print("1. PRAGMA table_info (物理スキーマ)")
print("=" * 80)
cur.execute(f"PRAGMA table_info('{TABLE_NAME}')")
rows = cur.fetchall()
print(f"{'cid':<5} {'name':<40} {'type':<15} {'notnull':<8} {'dflt':<10} {'pk'}")
print("-" * 80)
for r in rows:
    print(f"{r['cid']:<5} {r['name']:<40} {r['type']:<15} {r['notnull']:<8} {str(r['dflt_value']):<10} {r['pk']}")

# ============================================================
# 2. nc_columns_v2 メタデータ（全カラム）
# ============================================================
print("\n" + "=" * 80)
print("2. nc_columns_v2 メタデータ")
print("=" * 80)

# まず fk_model_id を取得
cur.execute("SELECT id FROM nc_models_v2 WHERE table_name = ?", (TABLE_NAME,))
model_row = cur.fetchone()
model_id = model_row["id"]
print(f"Model ID: {model_id}\n")

cur.execute("""
    SELECT id, column_name, title, uidt, dt, meta, cdf, system, "order" as col_order
    FROM nc_columns_v2
    WHERE fk_model_id = ?
    ORDER BY col_order
""", (model_id,))
col_rows = cur.fetchall()
print(f"{'title':<25} {'column_name':<40} {'uidt':<15} {'dt':<15} {'system'}")
print("-" * 100)
for r in col_rows:
    title = r["title"] or "(none)"
    col_name = r["column_name"] or "(virtual)"
    uidt = r["uidt"] or ""
    dt = r["dt"] or ""
    system = r["system"] or 0
    print(f"{title:<25} {col_name:<40} {uidt:<15} {dt:<15} {system}")

# Formula / Lookup / Rollup / LinkToAnotherRecord の詳細
print("\n--- Formula / Computed カラム詳細 ---")
for r in col_rows:
    uidt = r["uidt"] or ""
    if uidt in ("Formula", "Lookup", "Rollup", "LinkToAnotherRecord", "Links"):
        title = r["title"] or "(none)"
        meta_raw = r["meta"]
        meta = {}
        if meta_raw:
            try:
                meta = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
            except:
                meta = {"raw": str(meta_raw)}
        print(f"\n  [{uidt}] {title}")
        print(f"    column_name: {r['column_name']}")
        print(f"    id: {r['id']}")
        # Formula の場合 nc_formula_v2 テーブルも参照
        if uidt == "Formula":
            cur.execute("SELECT formula, formula_raw FROM nc_col_formula_v2 WHERE fk_column_id = ?", (r["id"],))
            formula_row = cur.fetchone()
            if formula_row:
                print(f"    formula: {formula_row['formula']}")
                print(f"    formula_raw: {formula_row['formula_raw']}")
        # Lookup の場合
        if uidt == "Lookup":
            cur.execute("SELECT fk_relation_column_id, fk_lookup_column_id FROM nc_col_lookup_v2 WHERE fk_column_id = ?", (r["id"],))
            lookup_row = cur.fetchone()
            if lookup_row:
                print(f"    fk_relation_column_id: {lookup_row['fk_relation_column_id']}")
                print(f"    fk_lookup_column_id: {lookup_row['fk_lookup_column_id']}")
        # Rollup の場合
        if uidt == "Rollup":
            cur.execute("SELECT fk_relation_column_id, fk_rollup_column_id, rollup_function FROM nc_col_rollup_v2 WHERE fk_column_id = ?", (r["id"],))
            rollup_row = cur.fetchone()
            if rollup_row:
                print(f"    fk_relation_column_id: {rollup_row['fk_relation_column_id']}")
                print(f"    fk_rollup_column_id: {rollup_row['fk_rollup_column_id']}")
                print(f"    rollup_function: {rollup_row['rollup_function']}")
        # Links / LTAR の場合
        if uidt in ("LinkToAnotherRecord", "Links"):
            cur.execute("""
                SELECT type, fk_child_column_id, fk_parent_column_id,
                       fk_mm_model_id, fk_related_model_id, fk_mm_child_column_id, fk_mm_parent_column_id
                FROM nc_col_relations_v2
                WHERE fk_column_id = ?
            """, (r["id"],))
            rel_row = cur.fetchone()
            if rel_row:
                print(f"    relation_type: {rel_row['type']}")
                print(f"    fk_child_column_id: {rel_row['fk_child_column_id']}")
                print(f"    fk_parent_column_id: {rel_row['fk_parent_column_id']}")
                if rel_row["fk_related_model_id"]:
                    cur.execute("SELECT table_name, title FROM nc_models_v2 WHERE id = ?", (rel_row["fk_related_model_id"],))
                    rel_model = cur.fetchone()
                    if rel_model:
                        print(f"    related_table: {rel_model['title']} ({rel_model['table_name']})")

# ============================================================
# 3. サンプルデータ（全カラム、最初の10行）
# ============================================================
print("\n" + "=" * 80)
print("3. サンプルデータ（全カラム、先頭10行）")
print("=" * 80)

# 物理カラム名のみ取得
cur.execute(f"PRAGMA table_info('{TABLE_NAME}')")
phys_cols = [r["name"] for r in cur.fetchall()]

cur.execute(f"SELECT * FROM \"{TABLE_NAME}\" ORDER BY id LIMIT 10")
sample_rows = cur.fetchall()
for i, row in enumerate(sample_rows):
    print(f"\n--- Row {i+1} (id={row['id']}) ---")
    for col in phys_cols:
        val = row[col]
        if val is not None and str(val).strip() != "":
            print(f"  {col}: {val}")

# ============================================================
# 4. 金額（円換算）の調査 — 外貨金額 * 為替レート が端数になるケース
# ============================================================
print("\n" + "=" * 80)
print("4. 金額調査（外貨金額 * 為替レート の端数ケース、最大20行）")
print("=" * 80)

# 金額 = 外貨金額 * 為替レート のはず（Formula列）
# 物理カラムにある decimal 列で調査
decimal_cols = ["外貨金額", "為替レート", "元残高", "円残高", "数量"]
existing_decimal_cols = [c for c in decimal_cols if c in phys_cols]
print(f"調査対象 decimal カラム: {existing_decimal_cols}")

# 外貨金額 * 為替レート の計算結果が整数でないケース
cur.execute(f"""
    SELECT *, (CAST(外貨金額 AS REAL) * CAST(為替レート AS REAL)) as 計算円金額
    FROM \"{TABLE_NAME}\"
    WHERE 外貨金額 IS NOT NULL AND 為替レート IS NOT NULL
    AND CAST(外貨金額 AS REAL) * CAST(為替レート AS REAL) != 0
    ORDER BY id
    LIMIT 20
""")
calc_rows = cur.fetchall()
print(f"\n外貨金額 * 為替レート のサンプル（最大20行）:")
for row in calc_rows:
    calc = row["計算円金額"]
    is_round = abs(calc - round(calc)) < 0.01
    marker = "" if is_round else " ← 端数あり"
    print(f"  id={row['id']}: 外貨={row['外貨金額']}, レート={row['為替レート']}, "
          f"計算円金額={calc:,.2f}{marker}")

# 端数ありのみ
cur.execute(f"""
    SELECT *, (CAST(外貨金額 AS REAL) * CAST(為替レート AS REAL)) as 計算円金額
    FROM \"{TABLE_NAME}\"
    WHERE 外貨金額 IS NOT NULL AND 為替レート IS NOT NULL
    AND ABS((CAST(外貨金額 AS REAL) * CAST(為替レート AS REAL)) - ROUND(CAST(外貨金額 AS REAL) * CAST(為替レート AS REAL))) > 0.01
    ORDER BY id
    LIMIT 20
""")
frac_rows = cur.fetchall()
print(f"\n端数あり行: {len(frac_rows)} 件")
for row in frac_rows:
    print(f"  id={row['id']}: 外貨={row['外貨金額']}, レート={row['為替レート']}, "
          f"計算円金額={row['計算円金額']:,.2f}, "
          f"元残高={row['元残高']}, 円残高={row['円残高']}")

# ============================================================
# 5. 総行数・金額統計
# ============================================================
print("\n" + "=" * 80)
print("5. 統計情報")
print("=" * 80)

cur.execute(f"SELECT COUNT(*) as cnt FROM \"{TABLE_NAME}\"")
print(f"総行数: {cur.fetchone()['cnt']}")

# 各 decimal / numeric カラムの統計
numeric_candidates = ["外貨金額", "為替レート", "元残高", "円残高", "数量"]
for c in numeric_candidates:
    if c in phys_cols:
        cur.execute(f"""
            SELECT COUNT(*) as cnt,
                   SUM(CAST(\"{c}\" AS REAL)) as total,
                   MIN(CAST(\"{c}\" AS REAL)) as min_val,
                   MAX(CAST(\"{c}\" AS REAL)) as max_val,
                   AVG(CAST(\"{c}\" AS REAL)) as avg_val
            FROM \"{TABLE_NAME}\"
            WHERE \"{c}\" IS NOT NULL AND \"{c}\" != ''
        """)
        s = cur.fetchone()
        if s["cnt"] > 0:
            print(f"\n  [{c}]")
            print(f"    行数={s['cnt']}, 合計={s['total']:,.2f}, 最小={s['min_val']:,.2f}, 最大={s['max_val']:,.2f}, 平均={s['avg_val']:,.2f}")

# 計算円金額の統計
cur.execute(f"""
    SELECT COUNT(*) as cnt,
           SUM(CAST(外貨金額 AS REAL) * CAST(為替レート AS REAL)) as total,
           MIN(CAST(外貨金額 AS REAL) * CAST(為替レート AS REAL)) as min_val,
           MAX(CAST(外貨金額 AS REAL) * CAST(為替レート AS REAL)) as max_val
    FROM \"{TABLE_NAME}\"
    WHERE 外貨金額 IS NOT NULL AND 為替レート IS NOT NULL
""")
calc_stats = cur.fetchone()
print(f"\n  [計算円金額 = 外貨金額 * 為替レート]")
print(f"    行数={calc_stats['cnt']}, 合計={calc_stats['total']:,.2f}, 最小={calc_stats['min_val']:,.2f}, 最大={calc_stats['max_val']:,.2f}")

# ============================================================
# 6. 代行会社名の一覧（もしあれば）
# ============================================================
print("\n" + "=" * 80)
print("6. 代行会社名（ユニーク値）")
print("=" * 80)

# 代行会社名を含むカラムを探す
for c in phys_cols:
    if "代行" in c or "会社" in c or "名" in c or "agency" in c.lower() or "name" in c.lower():
        cur.execute(f"SELECT DISTINCT \"{c}\" FROM \"{TABLE_NAME}\" WHERE \"{c}\" IS NOT NULL ORDER BY \"{c}\"")
        vals = [r[0] for r in cur.fetchall()]
        print(f"  {c}: {vals}")

# nc_columns_v2 の title から探す
name_cols = []
for r in col_rows:
    title = r["title"] or ""
    if any(kw in title for kw in ["代行", "会社", "名前", "摘要", "備考", "メモ"]):
        name_cols.append((r["title"], r["column_name"]))

if name_cols:
    print("\n  関連カラム（nc_columns_v2 title から）:")
    for title, col_name in name_cols:
        if col_name:
            cur.execute(f"SELECT DISTINCT \"{col_name}\" FROM \"{TABLE_NAME}\" WHERE \"{col_name}\" IS NOT NULL ORDER BY \"{col_name}\"")
            vals = [r[0] for r in cur.fetchall()]
            print(f"    {title} ({col_name}): {vals}")

# ============================================================
# 7. TEXT / varchar カラムの distinct values
# ============================================================
print("\n" + "=" * 80)
print("7. TEXT/varchar カラムの distinct values")
print("=" * 80)
text_cols = ["税区分", "備考", "決済口座", "商品カテゴリ", "原価区分"]
for c in text_cols:
    if c in phys_cols:
        cur.execute(f"SELECT DISTINCT \"{c}\", COUNT(*) as cnt FROM \"{TABLE_NAME}\" GROUP BY \"{c}\" ORDER BY cnt DESC")
        vals = cur.fetchall()
        print(f"\n  [{c}]:")
        for v in vals:
            print(f"    {v[0]!r}: {v[1]}件")

# ============================================================
# 8. FK参照先の確認（freee勘定科目、振替）
# ============================================================
print("\n" + "=" * 80)
print("8. FK参照先のID分布")
print("=" * 80)
fk_cols = [c for c in phys_cols if c.startswith("nc_opau___") and c.endswith("_id")]
for c in fk_cols:
    cur.execute(f"SELECT \"{c}\", COUNT(*) as cnt FROM \"{TABLE_NAME}\" GROUP BY \"{c}\" ORDER BY cnt DESC")
    vals = cur.fetchall()
    print(f"\n  [{c}]:")
    for v in vals:
        print(f"    {v[0]}: {v[1]}件")

# ============================================================
# 9. 振替フラグの分布
# ============================================================
print("\n" + "=" * 80)
print("9. 振替フラグの分布")
print("=" * 80)
cur.execute(f"SELECT 振替, COUNT(*) as cnt FROM \"{TABLE_NAME}\" GROUP BY 振替 ORDER BY cnt DESC")
for r in cur.fetchall():
    print(f"  振替={r[0]}: {r[1]}件")

conn.close()
print("\n--- 完了 ---")
