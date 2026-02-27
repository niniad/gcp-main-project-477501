# EC事業 システム設計書
*最終更新: 2026-02-25*

---

## 概要

個人EC事業（Amazon.co.jp出品 + セールモンスター）の会計データを管理するシステム。
NocoDB（手動入力・マスタデータ管理）と BigQuery（データ統合・P&L計算）を中心とした構成。

**システムの目的:**
- Amazonの精算データ・銀行明細・経費をすべて BigQuery に集約し、正確な損益（P&L）を計算する
- 年次確定申告用の財務データを生成する
- 2023/2024年度: マネーフォワード確定申告値との照合完了（一致確認済み）
- **2025年以降: このシステムが唯一の会計記録**

---

## 1. システム構成（コンポーネント一覧）

| コンポーネント | 用途 | 場所 |
|---|---|---|
| **Amazon SP-API** | 精算レポート・売上データ（自動取得） | Amazonマーケットプレイス |
| **Amazon Ads API** | 広告費データ（自動取得） | Amazonマーケットプレイス |
| **Google Cloud Storage (GCS)** | API取得データの一次保存 | GCP: main-project-477501 |
| **BigQuery** | 全データの統合・仕訳計算・P&L分析 | GCP: main-project-477501 |
| **NocoDB** | 手動入力データ管理（銀行明細・経費・商品マスタ等） | ローカル: localhost:8080, `C:/Users/ninni/nocodb/noco.db` |
| **Cloud Run** | SP-API/Ads APIデータの定期取得ジョブ | GCP: main-project-477501 |

---

## 2. データフロー全体図

```
【自動取得】
  Amazon SP-API ──────→ Cloud Run (spapi-to-gcs-daily)
  Amazon Ads API ─────→ Cloud Run (amazon-ads-to-gcs-daily)
                              │
                              ▼
                        GCS バケット ──→ BQ (sp_api_external, amazon_ads_external)

【手動入力 → NocoDB】
  銀行明細 (CSV) ────→ NocoDB: 楽天銀行/PayPay銀行入出金明細
  カード明細 (CSV) ──→ NocoDB: NTTファイナンスBizカード明細
  代行会社帳票 ──────→ NocoDB: 振替テーブル（ESPRIME/YP/THE直行便）
  手動仕訳 ──────────→ NocoDB: 手動仕訳（差異調整・事業主借経費）
  開業費 ──────────────→ NocoDB: 開業費（繰延資産、読み取り専用）
  商品マスタ ────────→ NocoDB: 製品マスタ・標準原価履歴等

【NocoDB → BigQuery 定期同期（nocodb-to-bq/main.py）】
  NocoDB の全テーブル ──→ BQ データセット nocodb（WRITE_TRUNCATE）
  20テーブル対象

【BigQuery 内部処理（VIEW による仕訳統合）】
  BQ: accounting.journal_entries VIEW
    ├─ Amazon精算仕訳 (settlement_journal_payload_view)
    ├─ PayPay銀行明細 (nocodb.paypay_bank_statements)
    ├─ 楽天銀行明細 (nocodb.rakuten_bank_statements)
    ├─ NTTファイナンス明細 (nocodb.ntt_finance_statements)
    ├─ 代行会社取引 (nocodb.agency_transactions)
    ├─ セールモンスター売上 (nocodb.sale_monster_reports)
    ├─ 手動仕訳 (nocodb.manual_journal_entries)
    ├─ 事業主借 (nocodb.owner_contribution_entries)
    └─ 棚卸仕訳 (accounting.inventory_journal_view) ★2025〜
                              │
                              ▼
  BQ: accounting.pl_journal_entries VIEW（pl_contribution付き）
                              │
                              ▼
                        P&L分析・確定申告用データ
```

---

## 3. NocoDB テーブル構成

NocoDB は `http://localhost:8080` で稼働するローカルのノーコードデータベース。
SQLite ファイル: `C:/Users/ninni/nocodb/noco.db`
API ベース URL: `http://localhost:8080/api/v2`

### 3.1 会計・財務系テーブル（BQ同期対象）

| テーブル名 | 用途 | BQ同期先テーブル |
|---|---|---|
| 手動仕訳 | 差異調整仕訳（複式簿記、5件） | nocodb.manual_journal_entries |
| 事業主借 | 個人資金拠出経費（複式簿記、117件） | nocodb.owner_contribution_entries |
| 開業費 | ~~archived~~ 空テーブル（事業主借テーブルに移行済み） | ~~nocodb.startup_cost_entries~~ |
| 楽天銀行ビジネス口座入出金明細 | CSV手動インポート（2023〜） | nocodb.rakuten_bank_statements |
| PayPay銀行入出金明細 | CSV手動インポート（2025〜） | nocodb.paypay_bank_statements |
| NTTファイナンスBizカード明細 | CSV手動インポート | nocodb.ntt_finance_statements |
| 振替 | 代行会社への預け金入出金（全費用の正本） | nocodb.agency_transactions |
| セールモンスター売上レポート | CSV手動インポート | nocodb.sale_monster_reports |
| freee勘定科目 | 勘定科目マスタ（account_name/small_category等） | nocodb.account_items |

### 3.2 商品マスタ系テーブル

| テーブル名 | 用途 | BQ同期 |
|---|---|---|
| 製品マスタ | SKU/ASINマスタ | 必要（nocodb.dim_products） |
| 標準原価履歴 | SKU別標準原価（年次手動更新） | 必要 |
| 購入商品マスタ | 中国仕入れ部品マスタ（PRD/MAT） | 不要 |
| 発注ロットマスタ | PO単位ヘッダ | 不要 |
| 発注明細 | POの品目・数量・外貨単価 | 不要（標準原価は年1回手動更新） |
| 輸入ロットマスタ | 船便単位ヘッダ | 不要 |
| 輸入明細 | 製品別輸入数量（参照用のみ） | 不要 |

### 3.3 事業主借テーブルの詳細

個人資金で支払った事業経費を管理する複式簿記テーブル。元は開業費テーブル（67件）と手動仕訳テーブル（50件）に分散していたものを統合。

**カラム構成:**

| カラム | 型 | 説明 |
|---|---|---|
| Id | 連番 | NocoDB内部ID（BQ同期時は nocodb_id） |
| 仕訳日 | Date | 仕訳の日付（YYYY-MM-DD） |
| 借方科目 | LinkToAnotherRecord | freee勘定科目テーブルへのリンク |
| 貸方科目 | LinkToAnotherRecord | freee勘定科目テーブルへのリンク（基本的に85=事業主借） |
| 金額 | Number | 仕訳金額（円） |
| 摘要 | Text | 仕訳内容の説明 |
| ソース | SingleLineText | 移行元追跡（開業費 / 事業主借 / 未払金経由→事業主借） |
| 元支出日 | Date | 実際の支出日（任意） |

**ソース別内訳（117件、¥995,069）:**

| ソース | 件数 | 金額 | 内容 |
|---|---|---|---|
| 開業費 | 67件 | ¥600,736 | 2022年開業準備費用（Dr=開業費/Cr=事業主借） |
| 事業主借 | 45件 | ¥371,258 | 楽天カード等で支払った事業経費 |
| 未払金経由→事業主借 | 5件 | ¥23,075 | 個人クレカ支払い（旧Cr=未払金→Cr=事業主借に修正） |

### 3.4 手動仕訳テーブルの詳細

差異調整専用の仕訳テーブル（5件）。

| Id | 仕訳区分 | 内容 |
|---|---|---|
| 188 | 事業主借経費 | OCS国際送料 Dr=研究開発費/Cr=楽天銀行 ¥1,500 |
| 190 | 差異調整（残差） | 2023年度 MF-BQ差異調整 Dr=消耗品費/Cr=事業主借 ¥7,874 |
| 191 | 差異調整（識別済み） | 期首残高調整 Dr=事業主借/Cr=雑収入 ¥24,106 |
| 192 | 差異調整（残差） | 2024年度 MF-BQ差異調整 Dr=事業主借/Cr=雑収入 ¥6,340 |
| 193 | 差異調整（識別済み） | deposit_date基準移行調整 Dr=事業主借/Cr=雑収入 ¥62,501 |

---

## 4. BigQuery データセット構成

| データセット | 内容 | 状態 |
|---|---|---|
| `sp_api_external` | SP-API raw data（精算レポート・売上・FBA在庫等） | 稼働中 |
| `amazon_ads_external` | 広告API raw data | 稼働中 |
| `nocodb` | NocoDB 全テーブルのエクスポート先（20テーブル） | 稼働中 |
| `accounting` | 仕訳ビュー・P&L計算・勘定科目マッピング等 | 稼働中 |
| `analytics` | 管理会計分析ビュー（rpt_pnl_5stage等） | 稼働中 |
| `assets` | Obsidian画像カタログ | 稼働中 |

---

## 5. accounting データセットの設計

### 5.1 accounting.journal_entries VIEW

**目的:** 9つのデータソースを統一した複式簿記の仕訳帳フォーマットで統合

**カラム構成:**

| カラム | 型 | 説明 |
|---|---|---|
| source_id | STRING | ソースごとのユニークID |
| journal_date | DATE | 仕訳日 |
| fiscal_year | INTEGER | 会計年度 |
| entry_side | STRING | `debit`（借方）or `credit`（貸方） |
| account_name | STRING | 勘定科目名 |
| amount_jpy | INTEGER | 金額（円） |
| tax_code | INTEGER | 税コード（Amazon精算のみ） |
| description | STRING | 摘要 |
| source_table | STRING | データソース識別子 |

**9つのデータソースと仕訳日の基準:**

| source_table | データソース | 仕訳日の基準 |
|---|---|---|
| amazon_settlement | Amazon精算レポート | **deposit_date**（銀行入金日） |
| paypay_bank | PayPay銀行入出金明細 | 取引日 |
| rakuten_bank | 楽天銀行入出金明細 | 取引日 |
| ntt_finance | NTTファイナンスBizカード明細 | 利用日 |
| agency_transactions | 代行会社取引（振替テーブル） | 取引日 |
| sale_monster | セールモンスター売上レポート | 売上日 |
| manual_journal | 手動仕訳テーブル（5件） | 仕訳日 |
| owner_contribution | 事業主借テーブル（117件） | 仕訳日 |
| inventory_adjustment | 棚卸仕訳（期首・期末） | 1/1（期首）, 12/31（期末） |

**複式簿記の展開方式（各ソース共通）:**
各トランザクションは借方（debit）と貸方（credit）の 2 行に展開される。例:
- 楽天銀行「入金 ¥10,000」 → `(debit: 楽天銀行 ¥10,000) + (credit: 売上高等 ¥10,000)`
- Amazon精算 → account_map で各費用科目に分解された多数の行（精算レポート1件が複数仕訳行になる）

**代行会社（楽天銀行→ESPRIME/THE直行便）の特殊処理:**
銀行明細の代行会社向け送金は `is_transfer=TRUE` だが資産勘定として計上が必要なため、VIEW 内で特別処理している:
- 楽天銀行 Cr（出金）+ 代行会社（ESPRIME/THE直行便）Dr（預け金増加）

### 5.2 accounting.inventory_journal_view VIEW

**目的:** FBA月次在庫データ × 標準原価から年度ごとの棚卸仕訳を自動生成

**データソース:**
- `sp_api_external.ledger-summary-view-data` — 12月末のSELLABLE在庫数量
- `nocodb.standard_cost_history` — SKU別標準原価（年次設定）
- `nocodb.product_master` — MSKU → products_id の紐付け

**仕訳パターン（FY Y）:**
- **期首（Y/1/1）:** Dr. 仕入高 / Cr. 商品 — 前年末在庫を売上原価に振替
- **期末（Y/12/31）:** Dr. 商品 / Cr. 仕入高 — 当年末在庫を売上原価から控除

**原価評価ルール:** FY Y の期首・期末ともに FY Y の標準原価で評価（effective_start_date が FY Y に含まれるコスト）

**年度選定:** 期首・期末の両方のデータが存在する年度のみ生成（過年度P&L保護）

**現在の出力（2025年度）:**
| エントリ | 金額 |
|---------|------|
| 期首（2025/1/1） | ¥483,968（12/2024在庫 × 2025原価） |
| 期末（2025/12/31） | ¥502,320（12/2025在庫 × 2025原価） |
| P&L影響 | +¥18,352（仕入高純減） |

### 5.3 accounting.freee_account_mapping テーブル

**目的:** journal_entries の `account_name` を freee API の `account_item_id` + `tax_code` にマッピング

**カラム:** account_name, freee_account_item_id (INT64), tax_code (INT64), notes

**税区分ルール:**
| 科目種別 | tax_code | freee表示 |
|---------|----------|----------|
| 課税売上（売上高等） | 129 | 課税売上10% |
| 課対仕入（経費・仕入） | 136 | 課対仕入10% |
| 非課税・対象外（銀行口座・BS等） | 2 | 対象外 |

### 5.4 accounting.freee_journal_payload_view VIEW

**目的:** journal_entries（Amazon精算除外）を freee manual_journals API 形式に変換

**出力:** source_id, issue_date, json_details (ARRAY<STRUCT: entry_side, account_item_id, tax_code, amount, description>)

**対象:** fiscal_year = 2025, source_table != 'amazon_settlement'（Amazon精算は settlement_journal_payload_view で処理）

### 5.5 accounting.account_map テーブル

Amazon精算レポートの `account_item_id`（freee の科目ID）を `account_name`（勘定科目名）に変換するマッピングテーブル。

### 5.6 accounting.merchant_account_rules テーブル

NTTファイナンスカードの加盟店名に対して勘定科目をオーバーライドするルールテーブル。
例: 特定の加盟店名 → `研究開発費`、`地代家賃` 等に変換。

### 5.7 nocodb.account_items テーブル（freee勘定科目）

NocoDB の `freee勘定科目` テーブルが BQ に同期されたもの。
会計分類の最重要マスタ。

**主要カラム:**

| カラム | 内容 | 例 |
|---|---|---|
| nocodb_id | NocoDB内部ID（手動仕訳の借方/貸方科目_idと対応） | 149 |
| account_name | 勘定科目名 | 外注費 |
| small_category | P&L計算に使う小分類 | 経費 / 収入金額 / 繰延資産 等 |
| large_category | 大分類 | 損益 / 資産 / 負債 等 |

**small_category の値とP&L計算への影響:**

| small_category | P&L計算 | 内容 |
|---|---|---|
| 収入金額 | 対象（収益） | 売上高、雑収入、為替差損益等 |
| 経費 | 対象（費用） | 外注費、通信費、広告宣伝費等 |
| 売上原価 | 対象（費用） | 仕入高 |
| 製品売上原価 | 対象（費用） | （現在未使用） |
| 繰入額等 | 対象（費用） | （現在未使用） |
| 繰延資産 | **非対象** | 開業費（P&L影響なし） |
| 資産・負債等 | **非対象** | 銀行口座、預け金、事業主借等 |

### 5.8 accounting.pl_journal_entries VIEW

**目的:** `journal_entries` に `pl_contribution`（P&L寄与額）を加えた分析用ビュー

**SQL定義:**
```sql
CREATE OR REPLACE VIEW `main-project-477501.accounting.pl_journal_entries` AS
SELECT
  je.*,
  ai.small_category,
  ai.large_category,
  CASE
    WHEN ai.small_category = '収入金額' AND je.entry_side = 'credit' THEN  je.amount_jpy
    WHEN ai.small_category = '収入金額' AND je.entry_side = 'debit'  THEN -je.amount_jpy
    WHEN ai.small_category IN ('経費','売上原価','製品売上原価','繰入額等')
         AND je.entry_side = 'debit'  THEN -je.amount_jpy
    WHEN ai.small_category IN ('経費','売上原価','製品売上原価','繰入額等')
         AND je.entry_side = 'credit' THEN  je.amount_jpy
    ELSE NULL
  END AS pl_contribution
FROM `main-project-477501.accounting.journal_entries` je
LEFT JOIN `main-project-477501.nocodb.account_items` ai
  ON je.account_name = ai.account_name
```

**pl_contribution のルール:**

| 科目分類 | entry_side | pl_contribution | 意味 |
|---|---|---|---|
| 収入金額 | credit | +amount | 収益増加 |
| 収入金額 | debit | -amount | 収益減少（返品・値引き・為替損失） |
| 経費・売上原価等 | debit | -amount | 費用増加 |
| 経費・売上原価等 | credit | +amount | 費用減少（費用戻し） |
| 繰延資産・資産・負債等 | either | NULL | P&L 非対象 |

**P&L合計の計算:**
```sql
SELECT fiscal_year, SUM(pl_contribution) AS net_income
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year
ORDER BY fiscal_year
```

---

## 6. P&L検証サマリ

| 年度 | BQ計算値（純損益） | MF確定申告値 | 状態 |
|---|---|---|---|
| 2023 | **-¥1,433,999** | -¥1,433,999 | ✅ 照合完了 |
| 2024 | **-¥995,493** | -¥995,493 | ✅ 照合完了 |
| 2025 | **-¥413,292** | （確定申告前） | 棚卸調整済み（期首¥483,968 / 期末¥502,320） |

**P&L検証クエリ:**
```sql
SELECT fiscal_year, SUM(pl_contribution) AS net_income
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year IN (2023, 2024)
GROUP BY 1 ORDER BY 1;
-- 期待値: 2023=-1433999, 2024=-995493
```

> 科目別P&L詳細・MFとの差異分析・差異調整仕訳の記録 → [mf_bq_reconciliation.md](mf_bq_reconciliation.md)
> 会計方針（仕訳日基準・為替差損益・開業費・原価計算等） → [accounting_policies.md](accounting_policies.md)

---

## 7. NocoDB → BQ 同期パイプライン

**スクリプト:** `C:/Users/ninni/projects/nocodb-to-bq/main.py`
**実行方式:** 手動実行（要定期化）
**同期方式:** WRITE_TRUNCATE（全件入れ替え）

**SKIP_COLUMNS（同期除外カラム）:**
```python
SKIP_COLUMNS = {
    "created_by", "updated_by", "nc_order",
    "_e_X_g__",  # 不明な内部列
}
```

> ⚠️ **重要:** `nc_opau___freee勘定科目_id` は SKIP_COLUMNS に含めてはいけない。
> このカラムは paypay_bank, rakuten_bank, ntt_finance, agency_transactions の各テーブルで
> `freee勘定科目_id` として `accounting.journal_entries` VIEW から参照されているため、
> SKIP_COLUMNS に追加すると journal_entries VIEW が破損する。

---

## 8. Cloud Run ジョブ

| ジョブ名 | 内容 | 頻度 |
|---|---|---|
| spapi-to-gcs-daily | SP-API各種レポート（精算・売上・FBA在庫等）→ GCS | 日次 |
| amazon-ads-to-gcs-daily | 広告APIデータ → GCS | 日次 |
| fetch-customs-exchange-rate | 税関公示レート → GCS（参考値として保持） | 週次 |

---

## 9. 原価計算・会計方針

→ 詳細は [accounting_policies.md](accounting_policies.md) を参照

---

## 10. 今後の残作業

| 作業 | 優先度 | 内容 |
|---|---|---|
| nocodb-to-bq 定期実行化 | 中 | 現状手動 → Cloud Run Job / スケジューラ化 |
| 月次棚卸ビュー作成 | 中 | FBA在庫 × 標準原価 = 棚卸高 |
| 標準原価の年次更新 | 中 | 2025年度分の標準原価を算出・登録 |
| freee連携（振替伝票インポート） | 低 | BQ仕訳 → freee API `POST /api/1/manual_journals` |

**完了済み（2026-02-25）:**
- ✅ accounting.pl_journal_entries VIEW 作成
- ✅ settlement基準を deposit_date に変更（差異調整仕訳 Id=193 追加）
- ✅ 為替差損益 small_category を 経費 に変更
- ✅ 仕訳区分カラム追加・バックフィル
- ✅ 借方科目/貸方科目の LinkToAnotherRecord 設定

**完了済み（2026-02-26）:**
- ✅ 事業主借テーブル新規作成（開業費67件＋手動仕訳50件を統合、117件）
- ✅ 開業費テーブル→空（重複34件+調整2件を削除、67件は事業主借に移行）
- ✅ 手動仕訳テーブル整理（55件→5件、差異調整のみ残す）
- ✅ 未払金→事業主借 修正（5件、¥23,075）→ BS未払金残高ゼロ化
- ✅ 開業費残高 ¥720,295 に修正（MF確定申告値と一致）
- ✅ journal_entries VIEW更新（⑧startup_cost削除、⑩owner_contribution追加）
- ✅ 全年度で貸借バランス imbalance=0 確認済み

---

## 11. 参照ドキュメント・重要ファイル

| 種別 | 場所 |
|---|---|
| 会計方針（仕訳日基準・為替・開業費・原価計算） | [accounting_policies.md](accounting_policies.md) |
| MF vs BQ 照合記録（2023/2024） | [mf_bq_reconciliation.md](mf_bq_reconciliation.md) |
| ゴール定義 | [goal.md](goal.md) |
| freee再構築計画 | [../freee/freee_rebuild_plan.md](../freee/freee_rebuild_plan.md) |
| BQ-NocoDB 同期スクリプト | `C:/Users/ninni/projects/nocodb-to-bq/main.py` |
| 手動仕訳操作スクリプト群 | `C:/Users/ninni/projects/tmp/*.py` |
| NocoDB SQLite DB | `C:/Users/ninni/nocodb/noco.db` |
| NocoDB 自動バックアップ | `G:/マイドライブ/backup/nocodb/` |
| GCP プロジェクト | main-project-477501 |
