# EC事業者向けデータ分析基盤 設計提案書

**プロジェクト**: main-project-477501
**対象**: 個人事業主のAmazon EC事業者
**作成日**: 2026-01-17

---

## 📊 現状分析

### データセット構成
現在、BigQueryには以下の6つのデータセットが存在します:

1. **sp_api_external** (12テーブル) - Amazon SP-API生データ
2. **amazon_ads_external** (8テーブル) - Amazon広告API生データ
3. **amazon_ads_v1_external** (4テーブル) - Amazon広告API新版
4. **native_table** (20テーブル) - 処理済みファクト・ディメンションテーブル
5. **analytics** (9テーブル) - 分析用マートテーブル
6. **gss_connected** (2テーブル) - GS1商品マスター連携

### データ量
- **総テーブル数**: 55テーブル
- **総データ量**: 約20MB
- **総レコード数**: 約120,000行
- **主要データソース**: Amazon SP-API、Amazon Ads API

### 既存の強み
✅ **データ収集基盤**: Amazon APIからの自動データ取得が確立
✅ **データ変換**: 外部テーブルからネイティブテーブルへの変換処理済み
✅ **分析マート**: 日次/週次/月次の集計テーブルが構築済み
✅ **商品マスター**: GS1連携による商品データ管理

### 課題
⚠️ **データ鮮度**: 外部テーブル(amazon_ads_external等)が主に0行
⚠️ **データ統合**: native_tableとanalyticsの役割分担が不明瞭
⚠️ **運用負荷**: 個人事業主が管理するには複雑すぎる可能性
⚠️ **可視化**: BIツールとの連携が不明

---

## 🎯 設計目標

個人事業主が**日々の意思決定に活用できる**データ分析基盤を構築します。

### 重要指標 (KPI)
1. **売上・利益指標**
   - 日次/週次/月次売上
   - 粗利益率
   - 広告費対効果 (ROAS)
   - 真の利益 (売上 - 広告費 - FBA手数料 - 仕入原価)

2. **在庫・物流指標**
   - 在庫回転率
   - 在庫日数
   - FBA在庫状況
   - 欠品リスク商品

3. **広告パフォーマンス**
   - キーワード別ROAS
   - 検索語パフォーマンス
   - 広告費トレンド
   - 費用対効果の低いキャンペーン

4. **顧客・商品分析**
   - リピート購入率
   - 商品別セッション率
   - カート追加率
   - レビュー評価推移

---

## 🏗️ 推奨アーキテクチャ

### レイヤー構造

```
┌─────────────────────────────────────────────────────────┐
│  Layer 4: BI/可視化層                                    │
│  - Looker Studio (無料)                                 │
│  - Google Sheets (簡易分析)                             │
└─────────────────────────────────────────────────────────┘
                          ↑
┌─────────────────────────────────────────────────────────┐
│  Layer 3: セマンティック層 (新規提案)                    │
│  Dataset: analytics_v2                                  │
│  - view_daily_dashboard (日次ダッシュボード用)          │
│  - view_weekly_summary (週次サマリー)                   │
│  - view_product_performance (商品パフォーマンス)         │
│  - view_keyword_roi (キーワードROI分析)                 │
│  - view_inventory_status (在庫状況)                     │
└─────────────────────────────────────────────────────────┘
                          ↑
┌─────────────────────────────────────────────────────────┐
│  Layer 2: データマート層 (既存活用 + 改善)               │
│  Dataset: analytics                                     │
│  - wide_date_child_asin (商品別日次データ)              │
│  - wide_weekly_parent_asin (週次集計)                   │
│  - wide_monthly_parent_asin (月次集計)                  │
│  - rpt_week_keyword_performances (キーワード分析)       │
│  - mart_flywheel_analysis (フライホイール分析)          │
└─────────────────────────────────────────────────────────┘
                          ↑
┌─────────────────────────────────────────────────────────┐
│  Layer 1: データレイク層 (既存維持)                      │
│  Dataset: native_table                                  │
│  - fact_all-orders-report (注文データ)                  │
│  - fact_sp_product_search_term_placements (広告データ)  │
│  - fact_fba-inventory (在庫データ)                      │
│  - fact_settlement-report-details (決済データ)          │
│  - dim_products (商品マスター)                          │
│  - dim_date_master (日付マスター)                       │
└─────────────────────────────────────────────────────────┘
                          ↑
┌─────────────────────────────────────────────────────────┐
│  Layer 0: 外部テーブル層 (既存維持)                      │
│  Dataset: sp_api_external, amazon_ads_external          │
│  - Amazon SP-API生データ                                │
│  - Amazon Ads API生データ                               │
└─────────────────────────────────────────────────────────┘
```

---

## 📋 具体的な実装提案

### 1. セマンティック層の構築 (最優先)

個人事業主が**即座に理解できる**ビューを作成します。

#### 1.1 日次ダッシュボードビュー
```sql
CREATE OR REPLACE VIEW `analytics_v2.view_daily_dashboard` AS
SELECT
  d.日付,
  d.曜日,
  -- 売上指標
  COALESCE(SUM(d.売上高), 0) AS 総売上,
  COALESCE(SUM(d.広告売上高), 0) AS 広告経由売上,
  COALESCE(SUM(d.売上高) - SUM(d.広告売上高), 0) AS 自然売上,

  -- 注文指標
  COALESCE(SUM(d.全体注文個数), 0) AS 総注文数,
  COALESCE(SUM(d.全体販売個数), 0) AS 総販売個数,

  -- 広告指標
  COALESCE(SUM(d.広告費), 0) AS 広告費,
  SAFE_DIVIDE(SUM(d.広告売上高), SUM(d.広告費)) AS ROAS,
  SAFE_DIVIDE(SUM(d.広告費), SUM(d.広告クリック)) AS CPC平均,

  -- 利益指標 (簡易計算)
  COALESCE(SUM(d.売上高) - SUM(d.広告費), 0) AS 粗利益,
  SAFE_DIVIDE(SUM(d.売上高) - SUM(d.広告費), SUM(d.売上高)) * 100 AS 粗利益率,

  -- トラフィック指標
  COALESCE(SUM(d.セッション数), 0) AS セッション数,
  COALESCE(SUM(d.ページビュー数), 0) AS ページビュー数,
  SAFE_DIVIDE(SUM(d.全体注文個数), SUM(d.セッション数)) * 100 AS CVR,

  -- 在庫状況 (最新日のデータを取得)
  (SELECT SUM(totalQuantity)
   FROM `native_table.fact_fba-inventory` i
   WHERE i.fetchedAt_jst_date = d.日付) AS 総在庫数

FROM `analytics.wide_date_parent_asin` d
GROUP BY d.日付, d.曜日
ORDER BY d.日付 DESC;
```

**活用方法**: 毎朝この1つのビューを見るだけで事業状況を把握

#### 1.2 商品パフォーマンスビュー
```sql
CREATE OR REPLACE VIEW `analytics_v2.view_product_performance` AS
SELECT
  w.親ASIN,
  p.component_name AS 商品名,
  p.brand AS ブランド,

  -- 売上指標
  SUM(w.売上高) AS 総売上,
  SUM(w.全体販売個数) AS 総販売個数,
  AVG(w.平均販売価格) AS 平均単価,

  -- 広告指標
  SUM(w.広告費) AS 広告費合計,
  SAFE_DIVIDE(SUM(w.広告売上高), SUM(w.広告費)) AS ROAS,

  -- トラフィック指標
  SUM(w.セッション数) AS セッション数,
  SAFE_DIVIDE(SUM(w.全体注文個数), SUM(w.セッション数)) * 100 AS CVR,
  AVG(w.カート追加率) AS 平均カート追加率,

  -- 在庫指標
  MAX(w.週終了日) AS 最終更新日,

  -- パフォーマンススコア (総合評価)
  (
    SAFE_DIVIDE(SUM(w.広告売上高), SUM(w.広告費)) * 0.4 +  -- ROAS 40%
    SAFE_DIVIDE(SUM(w.全体注文個数), SUM(w.セッション数)) * 1000 * 0.3 +  -- CVR 30%
    SAFE_DIVIDE(SUM(w.売上高), SUM(w.セッション数)) * 0.3  -- セッション単価 30%
  ) AS パフォーマンススコア

FROM `analytics.wide_weekly_parent_asin` w
LEFT JOIN `native_table.dim_products` p ON w.親ASIN = p.parent_asin
WHERE w.年 = EXTRACT(YEAR FROM CURRENT_DATE())
  AND w.週 >= EXTRACT(WEEK FROM DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK))
GROUP BY w.親ASIN, p.component_name, p.brand
ORDER BY パフォーマンススコア DESC;
```

**活用方法**: どの商品に注力すべきか一目で判断

#### 1.3 キーワードROI分析ビュー
```sql
CREATE OR REPLACE VIEW `analytics_v2.view_keyword_roi` AS
SELECT
  k.キーワード,
  k.運用判断フラグ,

  -- パフォーマンス指標
  SUM(k.広告IMP) AS 合計IMP,
  SUM(k.広告クリック) AS 合計クリック,
  SUM(k.広告購入数) AS 合計購入数,
  SUM(k.広告費) AS 合計広告費,
  SUM(k.広告売上) AS 合計広告売上,

  -- 効率指標
  SAFE_DIVIDE(SUM(k.広告クリック), SUM(k.広告IMP)) * 100 AS CTR,
  SAFE_DIVIDE(SUM(k.広告購入数), SUM(k.広告クリック)) * 100 AS CVR,
  SAFE_DIVIDE(SUM(k.広告費), SUM(k.広告クリック)) AS CPC平均,
  SAFE_DIVIDE(SUM(k.広告売上), SUM(k.広告費)) AS ROAS,

  -- CPC分析
  AVG(k.推奨CPC) AS 推奨CPC平均,
  AVG(k.現在の平均CPC) AS 現在CPC平均,
  AVG(k.推奨CPC) - AVG(k.現在の平均CPC) AS CPC調整余地,

  -- アクション推奨
  CASE
    WHEN SAFE_DIVIDE(SUM(k.広告売上), SUM(k.広告費)) >= 3.0 THEN '✅ 予算増額推奨'
    WHEN SAFE_DIVIDE(SUM(k.広告売上), SUM(k.広告費)) >= 1.5 THEN '👍 現状維持'
    WHEN SAFE_DIVIDE(SUM(k.広告売上), SUM(k.広告費)) >= 1.0 THEN '⚠️ CPC見直し'
    ELSE '❌ 停止検討'
  END AS アクション推奨

FROM `analytics.rpt_week_keyword_performances` k
WHERE k.開始日 >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
GROUP BY k.キーワード, k.運用判断フラグ
HAVING SUM(k.広告費) > 100  -- 100円以上使用したキーワードのみ
ORDER BY ROAS DESC;
```

**活用方法**: 毎週キーワード入札額を最適化

#### 1.4 在庫アラートビュー
```sql
CREATE OR REPLACE VIEW `analytics_v2.view_inventory_alerts` AS
WITH recent_sales AS (
  SELECT
    親ASIN,
    AVG(全体販売個数) AS 日平均販売個数
  FROM `analytics.wide_date_parent_asin`
  WHERE 日付 >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 親ASIN
),
current_inventory AS (
  SELECT
    p.parent_asin,
    SUM(i.totalQuantity) AS 現在庫数
  FROM `native_table.fact_fba-inventory` i
  JOIN `native_table.dim_products` p ON i.asin = p.child_asin
  WHERE i.fetchedAt_jst_date = (SELECT MAX(fetchedAt_jst_date) FROM `native_table.fact_fba-inventory`)
  GROUP BY p.parent_asin
)

SELECT
  ci.parent_asin AS ASIN,
  p.component_name AS 商品名,
  ci.現在庫数,
  rs.日平均販売個数,
  SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) AS 在庫日数,

  -- アラートレベル
  CASE
    WHEN SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) <= 7 THEN '🔴 緊急発注'
    WHEN SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) <= 14 THEN '🟡 発注検討'
    WHEN SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) <= 30 THEN '🟢 正常'
    ELSE '⚪ 過剰在庫'
  END AS 在庫ステータス,

  -- 推奨発注数 (30日分の在庫を維持)
  GREATEST(0, CAST(rs.日平均販売個数 * 30 - ci.現在庫数 AS INT64)) AS 推奨発注数

FROM current_inventory ci
JOIN recent_sales rs ON ci.parent_asin = rs.親ASIN
LEFT JOIN `native_table.dim_products` p ON ci.parent_asin = p.parent_asin
ORDER BY 在庫日数 ASC;
```

**活用方法**: 欠品を防ぎ、キャッシュフローを最適化

---

### 2. 自動化された日次/週次レポート

#### 2.1 Cloud Scheduler + Cloud Functions による自動レポート配信

**実装案**:
```python
# Cloud Function例: 毎朝8時に前日のパフォーマンスをSlack/メール送信
def daily_report(request):
    from google.cloud import bigquery
    import datetime

    client = bigquery.Client()
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    query = f"""
    SELECT * FROM `analytics_v2.view_daily_dashboard`
    WHERE 日付 = '{yesterday}'
    """

    results = client.query(query).to_dataframe()

    # Slack通知 or Gmail送信
    send_notification(results)
```

**スケジュール設定** (Cloud Scheduler):
- **日次レポート**: 毎朝8:00
- **週次レポート**: 毎週月曜9:00
- **月次レポート**: 毎月1日10:00
- **在庫アラート**: 毎日12:00

**コスト**: 月額 約¥0-100円 (無料枠内)

---

### 3. Looker Studio ダッシュボード構築

#### 3.1 推奨ダッシュボード構成

**① 経営ダッシュボード** (毎日確認)
- データソース: `analytics_v2.view_daily_dashboard`
- 表示内容:
  - 本日/昨日/先週同曜日の比較
  - 売上・広告費・利益のトレンドグラフ
  - ROAS推移
  - セッション数とCVR

**② 商品パフォーマンスダッシュボード** (週次確認)
- データソース: `analytics_v2.view_product_performance`
- 表示内容:
  - 商品別売上ランキング
  - パフォーマンススコア分布
  - ROAS vs CVRのバブルチャート

**③ 広告最適化ダッシュボード** (週次確認)
- データソース: `analytics_v2.view_keyword_roi`
- 表示内容:
  - 高ROAS/低ROASキーワード
  - CPC調整推奨リスト
  - 検索語別パフォーマンス

**④ 在庫管理ダッシュボード** (日次確認)
- データソース: `analytics_v2.view_inventory_alerts`
- 表示内容:
  - 在庫日数別商品リスト
  - 発注推奨アラート
  - 在庫回転率

**メリット**:
- ✅ 完全無料
- ✅ Googleアカウントで即利用可能
- ✅ モバイル対応
- ✅ 共有機能 (将来の従業員採用時に活用)

---

### 4. コスト最適化戦略

#### 4.1 BigQuery コスト管理

**現在のデータ量**: 約20MB → **ストレージコスト: ¥0/月** (10GB無料枠内)

**クエリコスト削減策**:
1. **パーティション化** (日付列でパーティション)
   ```sql
   CREATE TABLE `analytics.wide_date_child_asin_partitioned`
   PARTITION BY 日付
   AS SELECT * FROM `analytics.wide_date_child_asin`;
   ```
   → **効果**: クエリコスト 80-90%削減

2. **クラスタリング** (よく使うカラムでクラスタ化)
   ```sql
   CREATE TABLE `analytics.wide_date_child_asin_clustered`
   PARTITION BY 日付
   CLUSTER BY 親ASIN, 子ASIN
   AS SELECT * FROM `analytics.wide_date_child_asin`;
   ```
   → **効果**: クエリ速度 2-10倍向上

3. **マテリアライズドビュー** (集計済みデータをキャッシュ)
   ```sql
   CREATE MATERIALIZED VIEW `analytics_v2.mv_daily_summary`
   AS SELECT * FROM `analytics_v2.view_daily_dashboard`;
   ```
   → **効果**: 頻繁なクエリのコスト削減

**月間コスト見積もり** (個人事業規模):
- BigQuery: ¥0-500円 (無料枠 + 少量のクエリ)
- Cloud Functions: ¥0-100円 (200万リクエスト/月まで無料)
- Cloud Scheduler: ¥0 (3ジョブまで無料)
- **合計**: ¥0-600円/月

#### 4.2 データ保持ポリシー

**提案**:
- **生データ (sp_api_external等)**: 90日間保持
- **処理済みデータ (native_table)**: 2年間保持
- **集計データ (analytics)**: 無期限保持
- **ログデータ**: 30日間保持

**実装** (ライフサイクルポリシー):
```sql
ALTER TABLE `sp_api_external.all-orders-report`
SET OPTIONS (
  partition_expiration_days = 90
);
```

---

### 5. データ品質管理

#### 5.1 データバリデーションルール

**Cloud Functions で自動チェック**:
```python
def data_quality_check(event, context):
    from google.cloud import bigquery

    client = bigquery.Client()

    # チェック1: 最新データの鮮度確認
    query1 = """
    SELECT MAX(日付) as latest_date
    FROM `analytics.wide_date_parent_asin`
    """
    latest = client.query(query1).to_dataframe().iloc[0]['latest_date']

    if latest < datetime.date.today() - datetime.timedelta(days=2):
        send_alert("⚠️ データが2日以上更新されていません")

    # チェック2: 売上異常値検知
    query2 = """
    SELECT 日付, 総売上
    FROM `analytics_v2.view_daily_dashboard`
    WHERE 日付 >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ORDER BY 総売上 DESC
    LIMIT 1
    """
    max_sales = client.query(query2).to_dataframe()

    # 前日の売上が過去30日平均の3倍以上なら異常
    # ... アラート送信ロジック
```

**実行スケジュール**: 毎日 9:00

---

### 6. 段階的な実装ロードマップ

#### フェーズ1: 基盤整備 (1-2週間)
- [ ] `analytics_v2` データセット作成
- [ ] 4つのセマンティックビュー作成
  - `view_daily_dashboard`
  - `view_product_performance`
  - `view_keyword_roi`
  - `view_inventory_alerts`
- [ ] Looker Studio 接続設定

#### フェーズ2: 可視化構築 (1-2週間)
- [ ] 経営ダッシュボード作成
- [ ] 商品パフォーマンスダッシュボード作成
- [ ] 広告最適化ダッシュボード作成
- [ ] 在庫管理ダッシュボード作成

#### フェーズ3: 自動化 (1週間)
- [ ] Cloud Scheduler ジョブ設定
- [ ] Cloud Functions デプロイ (日次レポート)
- [ ] Slack/メール通知設定
- [ ] データ品質チェック実装

#### フェーズ4: 最適化 (継続的)
- [ ] パーティション・クラスタリング適用
- [ ] マテリアライズドビュー作成
- [ ] コストモニタリング設定
- [ ] ユーザーフィードバック収集と改善

---

## 🔧 運用管理のベストプラクティス

### 個人事業主向けの管理指針

#### 1. 日次ルーティン (5-10分)
- [ ] Looker Studio 経営ダッシュボードを確認
- [ ] 在庫アラートをチェック
- [ ] 異常値があれば調査

#### 2. 週次ルーティン (30-60分)
- [ ] 商品パフォーマンスを確認
- [ ] 広告キーワードの入札額調整
- [ ] 低ROAS広告の停止検討
- [ ] 在庫発注判断

#### 3. 月次ルーティン (2-3時間)
- [ ] 月次レポート分析
- [ ] 新商品企画のデータ検証
- [ ] 年間トレンド確認
- [ ] データ品質チェック

### トラブルシューティング

#### データが更新されない場合
1. Cloud Functions ログ確認
2. BigQuery ジョブ履歴確認
3. API接続エラーチェック

#### ダッシュボードが遅い場合
1. パーティション・クラスタリング適用
2. 集計期間を短縮
3. マテリアライズドビュー使用

---

## 💡 将来の拡張提案

### 短期 (3-6ヶ月)
1. **機械学習による需要予測**
   - BigQuery ML で売上予測モデル構築
   - 在庫最適化アルゴリズム実装

2. **競合分析**
   - Amazon商品ページのスクレイピング (合法範囲内)
   - 価格・レビュー推移の追跡

3. **顧客分析**
   - RFM分析 (Recency, Frequency, Monetary)
   - LTV (顧客生涯価値) 算出

### 中長期 (6-12ヶ月)
1. **マルチチャネル対応**
   - 楽天、Yahoo!ショッピングデータ統合
   - 自社ECサイトデータ連携

2. **財務会計連携**
   - freee / マネーフォワード API連携
   - 損益計算書の自動生成

3. **AIアシスタント**
   - ChatGPT API でデータ質問に自動回答
   - 音声での売上報告

---

## 📈 期待される効果

### 定量的効果
- ⏱️ **データ確認時間**: 60分/日 → 10分/日 (83%削減)
- 💰 **広告費削減**: ROAS 1.5 → 2.5 (67%改善目標)
- 📦 **在庫回転率**: 30日 → 20日 (33%改善)
- 💵 **月間利益**: +10-20% (意思決定速度向上による)

### 定性的効果
- ✅ データに基づく意思決定の習慣化
- ✅ 勘に頼らない広告運用
- ✅ 欠品・過剰在庫の防止
- ✅ 将来の事業拡大に耐えうる基盤

---

## 🎓 学習リソース

### BigQuery
- [BigQuery 公式ドキュメント](https://cloud.google.com/bigquery/docs)
- [BigQuery SQL リファレンス](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)

### Looker Studio
- [Looker Studio 公式チュートリアル](https://support.google.com/looker-studio)
- [データビジュアライゼーションのベストプラクティス](https://support.google.com/looker-studio/answer/7450249)

### EC分析
- [Amazon Seller Central ヘルプ](https://sellercentral.amazon.co.jp/help/hub)
- [データドリブンなEC運営ガイド](https://www.commerce-design.com/)

---

## 📞 次のステップ

### 即座に実行可能なアクション
1. ✅ この提案書を読み、疑問点をリストアップ
2. ✅ フェーズ1のビュー作成を依頼
3. ✅ Looker Studio アカウント開設
4. ✅ 週次定例レビュー時間を確保 (毎週月曜 30分)

### Claude Code への依頼事項
次のコマンドで実装を開始できます:

```bash
# セマンティック層のビューを作成
"analytics_v2データセットと4つのビューを作成してください"

# Looker Studio接続情報を出力
"Looker Studio用の接続設定を教えてください"

# 自動化スクリプトを作成
"日次レポートのCloud Functionを作成してください"
```

---

## 結論

本提案は、**個人事業主でも無理なく運用できる**データ分析基盤を目指しています。

重要なのは、**完璧を目指さず、小さく始めて改善を重ねる**ことです。
まずはフェーズ1のビュー作成から始め、日々の意思決定に活用しながら、
段階的に機能を拡張していくことをお勧めします。

データは既に揃っています。あとは「見える化」と「自動化」を進めるだけです。

**今日からデータドリブンなEC経営を始めましょう!**

---

**作成者**: Claude Code (Anthropic)
**バージョン**: 1.0
**最終更新**: 2026-01-17
