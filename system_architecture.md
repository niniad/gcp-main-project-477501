# システムアーキテクチャ

## 概要

本書は、Amazon、BigQuery、Google Sheets、Freee、および各種外部サービスを連携するシステムアーキテクチャを可視化したものです。

## 全体構成図 (Mermaid)

```mermaid
graph TD
    %% 外部プラットフォーム
    subgraph "External Platforms"
        AMZ_SP[Amazon SP-API]
        AMZ_ADS[Amazon Ads API]
        FREEE[Freee 会計]
    end

    %% 物流・サプライヤー
    subgraph "External Suppliers"
        AGENCY["輸入代行会社<br/>（ESPRIME, YP, THE直行便）"]
    end

    %% 社内 / GCPプロジェクト
    subgraph "GCP Project: main-project-477501"
        
        %% 社内運用（快適物販）
        subgraph "Internal Operations （Kaiteki Buppan）"
            GS_MASTER["Google Sheets<br/>（商品マスタ, 原価元帳）"]
            SYNC_SCRIPT["freee/scripts/sync_settlements.py"]
            FETCH_RATES_SCRIPT["fetch-customs-exchange-rate"]
        end

        subgraph "Google Cloud Platform"
            subgraph "Ingestion （データ取得）"
                INGEST_SP[spapi-to-gcs-daily<br/>（Cloud Run/Jobs）]
                INGEST_ADS[amazon-ads-to-gcs-daily<br/>（Cloud Run/Jobs）]
            end

            subgraph "Storage (GCS)"
                GCS_RAW["Raw Data Bucket<br/>（JSON/CSV）"]
            end

            subgraph "Data Warehouse (BigQuery)"
                BQ_EXT["sp_api_external<br/>amazon_ads"]
                BQ_SHEETS["google_sheets<br/>（外部テーブル連携）"]
                BQ_ANALYTICS["analytics<br/>（分析・集計ビュー）"]
            end
        end
    end
    
    %% データフロー
    AMZ_SP -->|注文, 決済, 在庫レポート| INGEST_SP
    AMZ_ADS -->|広告キャンペーン, KW実績| INGEST_ADS
    
    INGEST_SP -->|JSON/CSV 保存| GCS_RAW
    INGEST_ADS -->|JSON/CSV 保存| GCS_RAW
    
    GCS_RAW -->|外部テーブル / ロード| BQ_EXT
    
    AGENCY -->|請求書 / 船積書類| GS_MASTER
    
    GS_MASTER -->|フェデレーテッドクエリ| BQ_SHEETS
    
    BQ_EXT & BQ_SHEETS -->|SQL変換・集計| BQ_ANALYTICS
    
    GS_MASTER -->|SKUマッピング, 標準原価| SYNC_SCRIPT
    BQ_ANALYTICS -->|決済データ, 原価計算| SYNC_SCRIPT
    
    SYNC_SCRIPT -->|振替伝票 / 取引作成| FREEE
    
    FETCH_RATES_SCRIPT -->|税関公示レート| GCS_RAW
```

## コンポーネント詳細

### 1. データソース
| ソース | 説明 | 連携方法 |
| :--- | :--- | :--- |
| **Amazon SP-API** | 売上、在庫、決済レポートなど。 | `spapi-to-gcs-daily` (Cloud Run) が毎日/毎週データを取得。 |
| **Amazon Ads API** | 広告パフォーマンスデータ。 | `amazon-ads-to-gcs-daily` (Cloud Run) が取得。 |
| **輸入代行会社** | ESPRIME, YP, THE直行便 からの発注・輸送コスト情報。 | Google Sheets (`agency_ledger`, `po_details`) で管理。 |
| **快適物販 (自社)** | 自社運営（在庫管理、出荷指示など）。 | Google Sheets およびローカルスクリプトで管理。 |
| **Google Sheets** | 商品マスタ (`dim_products`)、マッピング定義、手動入力元帳。 | BigQuery外部テーブルまたはPythonスクリプトから直接参照。 |

### 2. データウェアハウス (BigQuery)
* **sp_api_external / amazon_ads**: GCSからマッピングされた未加工データ層（Raw Data）。
* **google_sheets**: Google Sheetsを直接参照する外部テーブル。
  * `dim_products`: 商品マスタ（SKUマッピング、標準原価）。
  * `agency_ledger`: 代行会社からの原価データ。
* **analytics**: データ変換・集計層。
  * `stg_average_unit_cost`: 発注データ(PO)から算出した平均単価。
  * `settlement_journal_view`: Freee向けに前処理された決済データ（借方・貸方）。

### 3. アプリケーション / スクリプト (Freee連携)
* **freee/scripts/sync_settlements.py**:
  * `dim_products` からSKUマッピングと標準原価を取得。
  * BigQueryから `settlement_journal_view` (決済データ) を取得。
  * 売上原価 (COGS) を計算。
  * Freee API を通じて振替伝票 (Manual Journal) を作成・登録。
* **fetch-customs-exchange-rate**:
  * 週間税関公示レートを取得。
  * GCSにアップロード（BigQueryでの原価計算に使用）。

### 4. Freee 会計システム
* 決済データ（振替伝票）および発注データ（取引）を受信。
* 必要に応じて取引先・品目マスタを同期。

## 主要なデータフロー

1. **売上原価 (COGS) フロー**:
    * 輸入代行会社 -> `agency_ledger` (Sheet) -> BigQuery -> `analytics.stg_average_unit_cost`
    * (現在の実装): `dim_products` (Sheet) -> `sync_settlements.py` (標準原価を利用) -> Freee
2. **売上・決済フロー**:
    * Amazon -> SP-API -> GCS -> BigQuery -> `sync_settlements.py` -> Freee
