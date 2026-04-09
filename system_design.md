# EC事業 システム設計書
*最終更新: 2026-03-06*

---

## 概要

個人EC事業（Amazon.co.jp出品 + セールモンスター）の会計データを管理するシステム。
NocoDB（手動入力・マスタデータ管理）→ BigQuery（データ統合・P&L計算）→ freee（確定申告）の3層構成。

**システムの目的:**
- Amazonの精算データ・銀行明細・経費をBigQueryに集約し、正確な損益（P&L）を計算する
- 年次確定申告用の財務データを生成する
- 2023/2024年度: マネーフォワード確定申告値との照合完了（完全一致確認済み）
- **2025年以降: このシステムが唯一の会計記録**

---

## 1. システム構成

| コンポーネント | 用途 | 場所 |
|---|---|---|
| **Amazon SP-API** | 精算レポート・売上データ（自動取得） | Amazon マーケットプレイス |
| **Amazon Ads API** | 広告費データ（自動取得） | Amazon マーケットプレイス |
| **Google Cloud Storage** | API取得データの一次保存 | GCP: main-project-477501 |
| **BigQuery** | 全データの統合・仕訳計算・P&L分析 | GCP: main-project-477501 |
| **NocoDB** | 手動入力データ管理（銀行明細・経費・商品マスタ等） | ローカル: `C:/Users/ninni/nocodb/noco.db` |
| **Cloud Run** | SP-API/Ads APIデータの定期取得ジョブ | GCP: main-project-477501 |
| **freee** | 確定申告・試算表生成 | freee クラウド（会社ID: 11078943） |

---

## 2. データフロー全体図

```
【自動取得（Cloud Run 日次）】
  Amazon SP-API ──→ GCS ──→ BQ: sp_api_external, amazon_ads_external

【手動入力 → NocoDB】
  銀行明細CSV ────→ 楽天銀行/PayPay銀行入出金明細
  カード明細CSV ──→ NTTファイナンスBizカード明細
  代行会社帳票 ───→ 代行会社テーブル + 振替テーブル
  手動調整仕訳 ───→ 手動仕訳テーブル（差異調整・為替差損益等）
  事業主個人経費 ─→ 事業主借テーブル（楽天カード等で支払った事業費）
  商品マスタ ────→ 製品マスタ・標準原価履歴等

【NocoDB → BigQuery 同期（手動実行）】
  C:/Users/ninni/infra/nocodb-to-bq/main.py
  全21テーブル WRITE_TRUNCATE で同期

【BigQuery 内部処理（VIEWによる仕訳統合）】
  accounting.journal_entries VIEW
    ├─ Amazon出品アカウント明細（amazon_account_statements）
    ├─ PayPay銀行明細（paypay_bank_statements）
    ├─ 楽天銀行明細（rakuten_bank_statements）
    ├─ NTTファイナンス明細（ntt_finance_statements）
    ├─ 代行会社取引（agency_transactions）
    ├─ セールモンスター売上（sale_monster_reports）
    ├─ 手動仕訳（manual_journal_entries）
    ├─ 事業主借（owner_contribution_entries）
    └─ 棚卸仕訳（inventory_journal_view）
                    ↓
  freee 同期スクリプト（年次確定申告時）
  C:/Users/ninni/infra/nocodb-to-bq/.venv/Scripts/python.exe
      tmp/freee_sync_fy2025.py
```

---

## 3. NocoDB テーブル構成

**NocoDB**: `http://localhost:8080` | SQLite: `C:/Users/ninni/nocodb/noco.db`

### 3.1 会計・財務系テーブル（BQ同期対象・21テーブル中の主要テーブル）

| テーブル名 | 行数 | 用途 | BQ同期先 |
|---|---|---|---|
| **楽天銀行ビジネス口座入出金明細** | 140 | CSV手動インポート。2023〜2025-06まで主力 | `nocodb.rakuten_bank_statements` |
| **PayPay銀行入出金明細** | 129 | CSV手動インポート。2025-06〜現在の主力 | `nocodb.paypay_bank_statements` |
| **Amazon出品アカウント明細** | 694 | Amazon精算の口座視点明細（自動生成はしない・別途管理） | `nocodb.amazon_account_statements` |
| **NTTファイナンスBizカード明細** | 219 | CSV手動インポート | `nocodb.ntt_finance_statements` |
| **代行会社** | 226 | ESPRIME/YP/THE直行便との取引 | `nocodb.agency_transactions` |
| **振替** | 111 | 口座間資金移動のリンクテーブル | `nocodb.transfer_records` |
| **手動仕訳** | 7 | 差異調整・為替差損益等の特殊仕訳 | `nocodb.manual_journal_entries` |
| **事業主借** | 127 | 個人資金で支払った事業経費（楽天カード等） | `nocodb.owner_contribution_entries` |
| **freee勘定科目** | 166 | 勘定科目マスタ（nocodb_id ↔ freee account_item_id） | `nocodb.account_items` |
| **セールモンスター売上レポート** | 119 | CSV手動インポート | `nocodb.sale_monster_reports` |

> ⚠️ **開業費テーブルは削除済み（2026-03-06）**。開業費エントリは事業主借テーブル内（source='開業費'）で管理。

### 3.2 商品マスタ系テーブル（BQ同期対象）

| テーブル名 | 用途 |
|---|---|
| 製品マスタ（product_master） | MSKU/ASINマスタ |
| 標準原価履歴（standard_cost_history） | SKU別標準原価（年次手動更新） |
| 購入商品マスタ・発注ロット/明細・輸入ロット/明細 | 仕入れ追跡（会計への直接影響なし） |
| セット構成マスタ | セット商品の構成品リスト |

---

## 4. NocoDB 勘定科目ID（nocodb_id）頻出一覧

| nocodb_id | account_name | 分類 | freee account_item_id |
|-----------|-------------|------|-----------------------|
| 3 | THE直行便 | BS（代行会社預け金） | 1007507685 |
| 5 | ESPRIME | BS（代行会社預け金） | 1007511503 |
| 6 | 楽天銀行 | BS（現金預金） | 1007579001 |
| 7 | YP | BS（代行会社預け金） | 1007511655 |
| 8 | PayPay銀行 | BS（現金預金） | 1007592863 |
| 9 | Amazon出品アカウント | BS（売掛金的口座） | 1008403397 |
| 12 | 売掛金 | BS（売掛金） | 786598200 |
| 17 | 商品 | BS（棚卸資産） | 786598202 |
| 70 | 未払金 | BS（負債） | 786598249 |
| 85 | 事業主借 | BS（純資産） | 786598262 |
| 99 | 売上高 | PL（収益） | 786598267 |
| 104 | 雑収入 | PL（収益） | 786598277 |
| 105 | 為替差損益 | PL（収益/費用） | 1007603892 |
| 109 | 仕入高 | PL（売上原価） | 786598280 |
| 119 | 荷造運賃 | PL（経費） | 786598290 |
| 124 | 通信費 | PL（経費） | 786598297 |
| 125 | 広告宣伝費 | PL（経費） | 786598298 |
| 126 | 販売手数料 | PL（経費） | 786598349 |
| 146 | 地代家賃 | PL（経費） | 786598329 |
| 148 | 支払手数料 | PL（経費） | 786598332 |
| 156 | 諸会費 | PL（経費） | 786598354 |
| 162 | 雑費 | PL（経費） | 786598367 |
| 166 | セールモンスター | BS（売掛金的口座） | 1024091121 |

---

## 5. BigQuery データセット構成

| データセット | 内容 | リージョン |
|---|---|---|
| `nocodb` | NocoDB 全21テーブルのエクスポート先 | us-central1 |
| `accounting` | 仕訳ビュー・P&L計算・勘定科目マッピング等 | us-central1 |
| `sp_api_external` | SP-API raw data（精算・売上・FBA在庫等） | us |
| `amazon_ads_external` | 広告API raw data | us |
| `analytics` | 管理会計分析ビュー | us-central1 |

---

## 6. accounting データセットの主要VIEW/テーブル

### 6.1 accounting.journal_entries VIEW（最重要）

9つのデータソースを統一した複式簿記フォーマットで統合するVIEW。

**出力カラム:**

| カラム | 型 | 説明 |
|---|---|---|
| source_id | STRING | ソースごとのユニークID |
| journal_date | DATE | 仕訳日 |
| fiscal_year | INTEGER | 会計年度（journal_dateのYEAR） |
| entry_side | STRING | `debit`（借方）or `credit`（貸方） |
| account_name | STRING | 勘定科目名 |
| amount_jpy | INTEGER | 金額（円） |
| tax_code | INTEGER | 税コード |
| description | STRING | 摘要 |
| source_table | STRING | データソース識別子 |

**9つのデータソース:**

| source_table | データソース | 仕訳日基準 | 振替フィルタ |
|---|---|---|---|
| amazon_settlement | Amazon精算（settlement_journal_view経由） | booking_date | 振替_id付エントリは除外 |
| amazon_nocodb | Amazon出品アカウント明細（NocoDB） | 取引日 | 振替_id IS NULL |
| paypay_bank | PayPay銀行明細 | 取引日 | 振替_id IS NULL（例外: 相手科目が5,6,9） |
| rakuten_bank | 楽天銀行明細 | 取引日 | 振替_id IS NULL（例外: 相手科目が3,5,6,7,8,9,70） |
| ntt_finance | NTTファイナンスBizカード | 利用日 | is_transfer IS FALSE |
| agency_transactions | 代行会社取引 | 取引日 | 振替_id IS NULL |
| sale_monster | セールモンスター売上レポート | 売上日 | なし |
| manual_journal | 手動仕訳テーブル | 仕訳日 | なし |
| owner_contribution | 事業主借テーブル | 仕訳日 | なし |
| inventory_adjustment | 棚卸仕訳（inventory_journal_view） | 1/1（期首）/ 12/31（期末） | なし |

**振替フィルタの詳細設計（重要）:**

振替テーブル（transfer_records）は口座間の資金移動を1対1でリンクする。
各口座セクションは `振替_id IS NULL` で振替行を除外するが、以下の例外がある:

| 振替パターン | 仕訳計上 | 理由 |
|---|---|---|
| 楽天→PayPay振替（振替_id=17,18,19） | 楽天側のみ計上 | PayPay側は例外リストに楽天(id=6)がないため除外 |
| PayPay→ESPRIME振替（振替_id=11-14） | PayPay側で計上 | 代行会社側は除外 |
| Amazon→楽天DEPOSIT（振替_id=1-93中多数） | 楽天側で計上 | Amazon口座側は除外 |
| Amazon→PayPay DEPOSIT（振替_id=94-108） | PayPay側で計上 | Amazon口座側は除外 |
| PayPay不足金支払→Amazon（振替_id=115,117等） | PayPay側で計上（Dr.Amazon口座） | Amazon明細側は除外 |

> **NTTの振替_id**: is_transferフラグで管理（月次支払バッチへのリンク用で振替除外フラグとは別概念）

### 6.2 accounting.inventory_journal_view VIEW

FBA月次在庫データ × 標準原価から棚卸仕訳を自動生成するVIEW。

- データソース: `sp_api_external.ledger-summary-view-data`（12月末SELLABLE在庫）× `nocodb.standard_cost_history`
- 期首（Y/1/1）: Dr.仕入高 / Cr.商品（前年末在庫を原価振替）
- 期末（Y/12/31）: Dr.商品 / Cr.仕入高（当年末在庫を控除）
- **FY2025の特殊処理**: Jan 2025のみ起点を¥0に固定（FY2024末ゼロ化の二重計上防止）

**現在の出力値（FY2025）:**
| | 金額 |
|---|---|
| 期首（2025/1/1） | ¥483,968 |
| 期末（2025/12/31） | ¥502,320（= SP-API一致） |

### 6.3 accounting.pl_journal_entries VIEW

`journal_entries` に `pl_contribution`（P&L寄与額）を付加した分析用VIEW。

```sql
-- P/L合計クエリ
SELECT fiscal_year, SUM(pl_contribution) AS net_income
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY fiscal_year ORDER BY fiscal_year
```

### 6.4 accounting.settlement_journal_view VIEW

Amazon SP-API精算データを仕訳形式に変換するVIEW。各精算期間のnetamountを計算する。

### 6.5 accounting.freee_account_mapping テーブル

`account_name` → freee `account_item_id` + `tax_code` のマッピングテーブル。

| 科目種別 | tax_code | 備考 |
|---|---|---|
| 課税売上（売上高等） | 129 | 課税売上10% |
| 課対仕入（経費・仕入） | 136 | 課対仕入10% |
| 非課税・対象外（BS科目等） | 2 | 対象外 |

### 6.6 accounting.merchant_account_rules テーブル

NTTカードの加盟店名 → 勘定科目のオーバーライドルール。

### 6.7 nocodb.agency_account_balances VIEW

代行会社（ESPRIME/YP/THE直行便）の残高をウィンドウ関数で自動計算するVIEW。

---

## 7. 振替テーブル（transfer_records）の設計

口座間の資金移動を1対1でリンクするテーブル。

| カラム | 型 | 説明 |
|---|---|---|
| id | INTEGER | 振替ID |
| 振替日 | DATE | 資金移動日 |
| 金額 | INTEGER | 移動金額（円） |
| memo | TEXT | 摘要 |

**振替リンクの種別:**
- Amazon→楽天 DEPOSIT（振替_id 1〜93の多数）
- Amazon→PayPay DEPOSIT（振替_id 94〜108, 2025-07以降）
- 楽天→PayPay 振替（振替_id 17,18,19）
- PayPay→ESPRIME 送金（振替_id 11〜14）
- PayPay不足金→Amazon（振替_id 115,117等）

---

## 8. P/L照合サマリ（確定値）

| 年度 | freee | MF確定申告 | 状態 |
|---|---|---|---|
| FY2023 | **-¥1,340,610** | -¥1,340,610 | ✅ 照合完了・申告済み |
| FY2024 | **-¥1,088,882** | -¥1,088,882 | ✅ 照合完了・申告済み |
| FY2025 | **-¥155,186** | （確定申告前） | freee同期完了・申告準備完了 |

**FY2025 BS残高（2025-12-31確定）:**

| 科目 | 残高 |
|---|---|
| 楽天銀行 | ¥0 |
| PayPay銀行 | ¥171,134 |
| Amazon出品アカウント | ¥62,866（2026/1/5入金済み・FY2026で消込） |
| ESPRIME | ¥8,759（CNY約391元）|
| 商品（棚卸資産） | ¥502,320 |
| セールモンスター | ¥14,300 |
| 工具器具備品 | ¥0（即時費用化済み） |
| 未払金 | ¥0 |
| 開業費（繰延資産） | ¥720,295 |

---

## 9. freee 同期設計

**会社ID:** 11078943
**同期方式:** BQ `journal_entries` → freee 振替伝票（manual_journals）として一括登録

**同期スクリプト（年度別）:**
```
C:/Users/ninni/projects/gcp-main-project-477501/tmp/freee_sync_fy2023.py
C:/Users/ninni/projects/gcp-main-project-477501/tmp/freee_sync_fy2024.py
C:/Users/ninni/projects/gcp-main-project-477501/tmp/freee_sync_fy2025.py
```

**実行方法（重要）:**
```
# ⚠️ uv run は長時間でバックグラウンド化するため使用禁止
# nocodb-to-bq の venv を直接使用する
cd C:/Users/ninni/projects/gcp-main-project-477501
C:/Users/ninni/infra/nocodb-to-bq/.venv/Scripts/python.exe tmp/freee_sync_fy2025.py
```

**freee の勘定科目マッピング（ACCOUNT_MAP）は freee_sync_fy2025.py 内に定義。**

**注意事項:**
- freee は FY2023 のみ会計期間が正式設定済み（trial_bs/trial_pl API が使える）
- FY2025 は振替伝票登録のみ（API経由での残高確認不可）
- 負金額仕訳（oc_126等）: freee登録時に借貸反転して正金額で登録する

---

## 10. NocoDB → BQ 同期パイプライン

**スクリプト:** `C:/Users/ninni/infra/nocodb-to-bq/main.py`
**Python環境:** `C:/Users/ninni/infra/nocodb-to-bq/.venv/Scripts/python.exe`
**実行コマンド:**
```
cd C:/Users/ninni/infra/nocodb-to-bq && uv run python main.py
```
**同期方式:** WRITE_TRUNCATE（全件入れ替え）

**重要な注意事項:**
> `nc_opau___freee勘定科目_id` は SKIP_COLUMNS に含めてはいけない。
> このカラムは journal_entries VIEW から参照されており、除外すると VIEW が破損する。

**同期後の確認クエリ:**
```sql
-- Amazon出品アカウント FY別残高確認
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name = 'Amazon出品アカウント'
GROUP BY 1 ORDER BY 1
-- 期待値: FY2023=0, FY2024=0, FY2025=62866
```

---

## 11. Cloud Run ジョブ（自動）

| ジョブ名 | 内容 | 頻度 |
|---|---|---|
| spapi-to-gcs-daily | SP-API各種レポート（精算・売上・FBA在庫等）→ GCS | 日次 |
| amazon-ads-to-gcs-daily | 広告APIデータ → GCS | 日次 |
| fetch-customs-exchange-rate | 税関公示レート → GCS（参考値） | 週次 |

---

## 12. 主要な会計ルール（詳細は accounting_policies.md 参照）

### 資金移動ルール
- THE直行便→YPの直接振込は存在しない（必ず楽天銀行経由）
- Amazon振込先: 〜2025-06は楽天銀行、2025-07以降はPayPay銀行
- PayPay→ESPRIME送金（4件、¥900,000）は paypay_bank_statements に記録済み

### 事業主借ルール
- 楽天カード（個人用）= 事業主借として処理
- freee勘定科目 nocodb_id=85, freee_account_item_id=786598262

### Amazon出品アカウント明細の符号規則
- 正（+）: 売上収入・受取配送料
- 負（-）: 手数料・費用・銀行送金（DEPOSIT）・不足金支払いの記帳エントリ
- 振替_id付き行はjournal_entries VIEWから除外（相手口座側で記帳）

### 棚卸方式
- 三分法（購入時→商品↑、期末棚卸調整→仕入高↑/商品↓）
- 代行会社の仕入れ = freee科目17（商品）で計上

---

## 13. 参照ドキュメント

| 内容 | 場所 |
|---|---|
| 会計方針（仕訳日基準・為替・原価計算） | `accounting_policies.md` |
| MF vs BQ 照合記録（FY2023/2024） | `mf_bq_reconciliation.md` |
| 月次・年次締め作業フロー（AI主導） | `monthly_closing_workflow.md` |
| NocoDB→BQ 同期スクリプト | `C:/Users/ninni/infra/nocodb-to-bq/main.py` |
| freee 同期スクリプト | `C:/Users/ninni/projects/gcp-main-project-477501/tmp/freee_sync_fy20XX.py` |
| NocoDB SQLite DB | `C:/Users/ninni/nocodb/noco.db` |
| GCP プロジェクト | main-project-477501 |
| freee 会社ID | 11078943 |
