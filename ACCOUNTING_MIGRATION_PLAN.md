# 会計データ整備計画：freee全年度反映

**作成日:** 2026-03-05（最終更新: 2026-03-05）
**対象:** 引き継ぎAI または 作業再開時の参照
**ゴール:** FY2023・FY2024・FY2025のfreee仕訳を完成させ、確定申告書・P/L・BSを一致させる

---

## ✅ 完了済み作業（本日2026-03-05）

| 作業 | 状態 |
|------|------|
| 代行会社 → 仕入高(109) に変更（三分法） | ✅ |
| inventory_journal_view → 月次三分法（SP-API在庫変動ベース）| ✅ |
| NocoDB 手動仕訳: FY2023/2024 期末棚卸調整 | ✅ |
| BQ sync（全21テーブル） | ✅ |
| BQ P/L検証: FY2023=-1,340,610 / FY2024=-1,088,882 | ✅ |
| BQ 商品残高: FY2023=¥93,389 / FY2024=¥0 | ✅ |
| freee FY2023 再同期（492件 + oc_128手動） | ✅ |

---

## 残タスク（優先順）

| # | 作業 | 方法 |
|---|------|------|
| **A** | freee FY2023 試算表確認 | freee skill または Web UI |
| **B** | freee **FY2024 会計期間作成** | **freee管理画面 手動操作** |
| **C** | freee FY2024 仕訳同期 | `tmp/freee_sync_fy2024.py` 新規作成 |
| **D** | freee **FY2025 会計期間作成** | **freee管理画面 手動操作** |
| **E** | freee FY2025 仕訳同期 | `tmp/freee_sync_fy2025.py` 新規作成 |
| **F** | 確定申告書 数値確認・手動修正 | 以下の正しい数値を参照 |

---

## 重要パス・ID

| リソース | 値 |
|---------|---|
| NocoDB SQLite | `C:/Users/ninni/nocodb/noco.db` |
| BQ sync | `cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py` |
| BQ プロジェクト | `main-project-477501` |
| freee Company ID | `11078943` |
| Python実行コマンド | `uv run --with requests --with google-cloud-secret-manager --with google-auth --with google-cloud-bigquery python` |
| freee skill | `C:/Users/ninni/.claude/skills/freee/` |

---

## 会計方式（確定）

### 現在の構成（月次三分法）
```
代行会社支払 → 仕入高（NocoDB freee勘定科目_id=109）
inventory_journal_view → 月次在庫変動（商品↑↓/仕入高↓↑）
手動仕訳 → FY2023/FY2024 期末棚卸調整
```

### inventory_journal_view のロジック（`tmp/deploy_sanpunpo_and_adjust.py`）
- SP-API `ledger-summary-view-data` の月末在庫数 × 標準原価 = 月末在庫金額
- 月次在庫増加: Dr.商品 / Cr.仕入高
- 月次在庫減少: Dr.仕入高 / Cr.商品
- FY2023/2024 の年次調整は 手動仕訳 で対応

### 手動仕訳（FY2023/2024 調整済み）
| 日付 | 借方 | 貸方 | 金額 | 摘要 |
|------|------|------|------|------|
| 2023-12-31 | 商品(17) | 仕入高(109) | ¥4,936 | FY2023期末棚卸調整（MF確定値¥93,389との差額） |
| 2024-12-31 | 仕入高(109) | 商品(17) | ¥515,454 | FY2024期末棚卸調整（MF確定値¥0との差額） |

### FY2025以降
- SP-APIデータに基づく月次三分法のみ（調整なし）
- 年度末にsanpunpo的な手動調整は基本不要（ただし標準原価が大幅にズレた場合は追加）

---

## BQ 検証クエリ（作業後に必ず確認）

### P/L
```sql
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='credit' THEN amount_jpy ELSE 0 END) -
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE 0 END) AS net_pl
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name NOT IN (
  '楽天銀行','PayPay銀行','Amazon出品アカウント','未払金',
  'THE直行便','ESPRIME','YP','セールモンスター','事業主借','開業費','商品'
)
GROUP BY 1 ORDER BY 1
-- 期待値: FY2023=-1,340,610 / FY2024=-1,088,882
```

### 商品残高
```sql
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS net
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = '商品'
GROUP BY 1 ORDER BY 1
-- 期待値: FY2023累積=+93,389 / FY2024累積=0
```

---

## 確定申告書の正しい数値（freee自動生成は使えない→手動入力）

freeeの自動生成では「当期商品仕入高」欄に仕入高勘定残高が入り、
さらに商品勘定期末残高が期末棚卸高として引かれるため二重調整になる。
以下の数値で手動入力すること。

| 項目 | FY2023 | FY2024 |
|------|--------|--------|
| 期首商品棚卸高 | ¥0 | ¥93,389 |
| 当期商品仕入高 | ¥266,373 | ¥817,680 |
| 期末商品棚卸高 | ¥93,389 | ¥0 |
| **売上原価** | **¥172,984** | **¥911,069** |
| 純損益 | -¥1,340,610 | -¥1,088,882 |

※ 当期商品仕入高の内訳:
- FY2023: 支出¥296,970 - 返金¥30,597 = ¥266,373
- FY2024: 支出¥818,330 - 返金¥650 = ¥817,680

---

## freee FY2024/2025 同期スクリプト作成方法

`tmp/freee_sync_fy2023.py` を参考に年度だけ変更して作成。

変更箇所:
```python
FISCAL_YEAR = 2023  # → 2024 or 2025
```
実行:
```bash
uv run --with requests --with google-cloud-secret-manager --with google-auth --with google-cloud-bigquery python tmp/freee_sync_fy2024.py
```

**前提:** freeeに対象年度の会計期間が存在すること（管理画面で手動作成）。

---

## FY2024 期待値（同期後に確認）
- 仕入高（売上原価）: ¥911,069
- 商品（BS）期末残: ¥0
- 純損益: -¥1,088,882

## FY2025 期待値（参考値、年度途中）
- BQ現在値: -¥653,779（2026/02まで）
- 月次調整なし（年度末に必要なら手動仕訳追加）

---

## NocoDB 勘定科目ID（主要）

| nocodb_id | account_name |
|-----------|-------------|
| 6 | 楽天銀行 |
| 8 | PayPay銀行 |
| 9 | Amazon出品アカウント |
| 17 | 商品（棚卸資産）|
| 109 | 仕入高 |
| 166 | セールモンスター |

---

## 進捗チェックリスト

- [x] BQ inventory_journal_view 月次三分法デプロイ
- [x] NocoDB 代行会社 → 仕入高(109)
- [x] NocoDB 手動仕訳: FY2023調整(¥4,936) / FY2024調整(¥515,454)
- [x] BQ sync 完了
- [x] BQ P/L・商品残高 検証 OK
- [x] freee FY2023 同期完了（492+1件）
- [ ] freee FY2023 試算表確認
- [ ] freee FY2024 会計期間作成（管理画面）
- [ ] freee FY2024 同期（スクリプト要作成）
- [ ] freee FY2025 会計期間作成（管理画面）
- [ ] freee FY2025 同期（スクリプト要作成）
- [ ] 確定申告書 数値手動確認・修正
