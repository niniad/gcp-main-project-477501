-- ============================================================
-- EC事業者向け セマンティック層ビュー作成スクリプト
-- ============================================================
-- プロジェクト: main-project-477501
-- データセット: analytics_v2
-- 作成日: 2026-01-17
-- ============================================================

-- データセット作成
CREATE SCHEMA IF NOT EXISTS `main-project-477501.analytics_v2`
OPTIONS (
  description = 'EC事業者向けセマンティック層 - 日々の意思決定に活用するビュー群',
  location = 'us'
);

-- ============================================================
-- 1. 日次ダッシュボードビュー
-- ============================================================
-- 用途: 毎朝の事業状況確認
-- 更新頻度: リアルタイム
-- ============================================================

CREATE OR REPLACE VIEW `main-project-477501.analytics_v2.view_daily_dashboard` AS
WITH daily_aggregated AS (
  SELECT
    日付,
    曜日,
    -- 売上指標
    SUM(売上高) AS 総売上,
    SUM(広告売上高) AS 広告経由売上,
    SUM(売上高) - SUM(広告売上高) AS 自然売上,

    -- 注文指標
    SUM(全体注文個数) AS 総注文数,
    SUM(全体販売個数) AS 総販売個数,

    -- 広告指標
    SUM(広告費) AS 広告費,
    SUM(広告クリック) AS 広告クリック,
    SUM(広告IMP) AS 広告表示回数,

    -- トラフィック指標
    SUM(セッション数) AS セッション数,
    SUM(ページビュー数) AS ページビュー数
  FROM `main-project-477501.analytics.wide_date_parent_asin`
  GROUP BY 日付, 曜日
),
inventory_summary AS (
  SELECT
    i.fetchedAt_jst_date AS 日付,
    SUM(i.totalQuantity) AS 総在庫数
  FROM `main-project-477501.native_table.fact_fba-inventory` i
  GROUP BY i.fetchedAt_jst_date
)

SELECT
  d.日付,
  d.曜日,

  -- 売上指標
  COALESCE(d.総売上, 0) AS 総売上,
  COALESCE(d.広告経由売上, 0) AS 広告経由売上,
  COALESCE(d.自然売上, 0) AS 自然売上,

  -- 注文指標
  COALESCE(d.総注文数, 0) AS 総注文数,
  COALESCE(d.総販売個数, 0) AS 総販売個数,

  -- 広告指標
  COALESCE(d.広告費, 0) AS 広告費,
  SAFE_DIVIDE(d.広告経由売上, d.広告費) AS ROAS,
  SAFE_DIVIDE(d.広告費, d.広告クリック) AS CPC平均,
  COALESCE(d.広告表示回数, 0) AS 広告表示回数,
  SAFE_DIVIDE(d.広告クリック, d.広告表示回数) * 100 AS CTR,

  -- 利益指標
  COALESCE(d.総売上 - d.広告費, 0) AS 粗利益,
  SAFE_DIVIDE(d.総売上 - d.広告費, d.総売上) * 100 AS 粗利益率,

  -- トラフィック指標
  COALESCE(d.セッション数, 0) AS セッション数,
  COALESCE(d.ページビュー数, 0) AS ページビュー数,
  SAFE_DIVIDE(d.総注文数, d.セッション数) * 100 AS CVR,
  SAFE_DIVIDE(d.総売上, d.セッション数) AS セッション単価,

  -- 在庫状況
  COALESCE(i.総在庫数, 0) AS 総在庫数,

  -- 前日比 (次のクエリで計算可能)
  LAG(d.総売上) OVER (ORDER BY d.日付) AS 前日売上,
  SAFE_DIVIDE(d.総売上 - LAG(d.総売上) OVER (ORDER BY d.日付), LAG(d.総売上) OVER (ORDER BY d.日付)) * 100 AS 前日比率

FROM daily_aggregated d
LEFT JOIN inventory_summary i ON d.日付 = i.日付
ORDER BY d.日付 DESC;


-- ============================================================
-- 2. 商品パフォーマンスビュー
-- ============================================================
-- 用途: 商品ごとの売上・広告効果を分析
-- 更新頻度: 週次
-- ============================================================

CREATE OR REPLACE VIEW `main-project-477501.analytics_v2.view_product_performance` AS
WITH weekly_product_data AS (
  SELECT
    w.親ASIN,
    w.週開始日,
    w.週終了日,
    w.売上高,
    w.全体販売個数,
    w.平均販売価格,
    w.広告費,
    w.広告売上高,
    w.セッション数,
    w.全体注文個数,
    w.カート追加率
  FROM `main-project-477501.analytics.wide_weekly_parent_asin` w
  WHERE w.年 = EXTRACT(YEAR FROM CURRENT_DATE())
    AND w.週 >= EXTRACT(WEEK FROM DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK))
)

SELECT
  wpd.親ASIN,
  p.component_name AS 商品名,
  p.brand AS ブランド,
  p.std_price AS 標準価格,
  p.std_cost AS 標準原価,

  -- 集計期間
  MIN(wpd.週開始日) AS 集計開始日,
  MAX(wpd.週終了日) AS 集計終了日,
  COUNT(DISTINCT wpd.週開始日) AS 集計週数,

  -- 売上指標
  SUM(wpd.売上高) AS 総売上,
  SUM(wpd.全体販売個数) AS 総販売個数,
  AVG(wpd.平均販売価格) AS 平均単価,

  -- 広告指標
  SUM(wpd.広告費) AS 広告費合計,
  SUM(wpd.広告売上高) AS 広告売上合計,
  SAFE_DIVIDE(SUM(wpd.広告売上高), SUM(wpd.広告費)) AS ROAS,

  -- トラフィック指標
  SUM(wpd.セッション数) AS セッション数,
  SUM(wpd.全体注文個数) AS 注文数,
  SAFE_DIVIDE(SUM(wpd.全体注文個数), SUM(wpd.セッション数)) * 100 AS CVR,
  AVG(wpd.カート追加率) AS 平均カート追加率,

  -- 利益指標 (簡易計算)
  SUM(wpd.売上高) - SUM(wpd.広告費) - (SUM(wpd.全体販売個数) * COALESCE(p.std_cost, 0)) AS 推定粗利益,

  -- パフォーマンススコア (100点満点)
  LEAST(100,
    COALESCE(SAFE_DIVIDE(SUM(wpd.広告売上高), SUM(wpd.広告費)) * 15, 0) +  -- ROAS (最大45点)
    COALESCE(SAFE_DIVIDE(SUM(wpd.全体注文個数), SUM(wpd.セッション数)) * 3000, 0) +  -- CVR (最大30点)
    COALESCE(SAFE_DIVIDE(SUM(wpd.売上高), SUM(wpd.セッション数)) / 100, 0)  -- セッション単価 (最大25点)
  ) AS パフォーマンススコア,

  -- 健全性フラグ
  CASE
    WHEN SAFE_DIVIDE(SUM(wpd.広告売上高), SUM(wpd.広告費)) >= 3.0 THEN '🟢 優良'
    WHEN SAFE_DIVIDE(SUM(wpd.広告売上高), SUM(wpd.広告費)) >= 1.5 THEN '🟡 標準'
    ELSE '🔴 要改善'
  END AS 広告健全性

FROM weekly_product_data wpd
LEFT JOIN `main-project-477501.native_table.dim_products` p ON wpd.親ASIN = p.parent_asin
GROUP BY
  wpd.親ASIN,
  p.component_name,
  p.brand,
  p.std_price,
  p.std_cost
HAVING SUM(wpd.売上高) > 0  -- 売上がある商品のみ
ORDER BY パフォーマンススコア DESC;


-- ============================================================
-- 3. キーワードROI分析ビュー
-- ============================================================
-- 用途: 広告キーワードの入札額最適化
-- 更新頻度: 週次
-- ============================================================

CREATE OR REPLACE VIEW `main-project-477501.analytics_v2.view_keyword_roi` AS
WITH recent_keywords AS (
  SELECT
    k.キーワード,
    k.運用判断フラグ,
    k.推奨CPC,
    k.現在の平均CPC,
    k.広告IMP,
    k.広告クリック,
    k.広告購入数,
    k.広告費,
    k.広告売上
  FROM `main-project-477501.analytics.rpt_week_keyword_performances` k
  WHERE k.開始日 >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
)

SELECT
  キーワード,
  運用判断フラグ,

  -- パフォーマンス指標
  SUM(広告IMP) AS 合計IMP,
  SUM(広告クリック) AS 合計クリック,
  SUM(広告購入数) AS 合計購入数,
  SUM(広告費) AS 合計広告費,
  SUM(広告売上) AS 合計広告売上,

  -- 効率指標
  SAFE_DIVIDE(SUM(広告クリック), SUM(広告IMP)) * 100 AS CTR,
  SAFE_DIVIDE(SUM(広告購入数), SUM(広告クリック)) * 100 AS CVR,
  SAFE_DIVIDE(SUM(広告費), SUM(広告クリック)) AS CPC平均,
  SAFE_DIVIDE(SUM(広告売上), SUM(広告費)) AS ROAS,

  -- CPC分析
  AVG(推奨CPC) AS 推奨CPC平均,
  AVG(現在の平均CPC) AS 現在CPC平均,
  AVG(推奨CPC) - AVG(現在の平均CPC) AS CPC調整余地,

  -- コスト効率
  SAFE_DIVIDE(SUM(広告費), SUM(広告購入数)) AS CPA,

  -- アクション推奨
  CASE
    WHEN SAFE_DIVIDE(SUM(広告売上), SUM(広告費)) >= 4.0 THEN '✅ 予算大幅増額推奨'
    WHEN SAFE_DIVIDE(SUM(広告売上), SUM(広告費)) >= 2.5 THEN '✅ 予算増額推奨'
    WHEN SAFE_DIVIDE(SUM(広告売上), SUM(広告費)) >= 1.5 THEN '👍 現状維持'
    WHEN SAFE_DIVIDE(SUM(広告売上), SUM(広告費)) >= 1.0 THEN '⚠️ CPC見直し'
    WHEN SUM(広告費) >= 1000 THEN '❌ 停止推奨'
    ELSE '💤 様子見'
  END AS アクション推奨,

  -- 優先度スコア (ROASと広告費の積)
  SAFE_DIVIDE(SUM(広告売上), SUM(広告費)) * SUM(広告費) AS 優先度スコア

FROM recent_keywords
GROUP BY キーワード, 運用判断フラグ
HAVING SUM(広告費) > 10  -- 10円以上使用したキーワードのみ
ORDER BY 優先度スコア DESC;


-- ============================================================
-- 4. 在庫アラートビュー
-- ============================================================
-- 用途: 発注タイミングの判断
-- 更新頻度: 日次
-- ============================================================

CREATE OR REPLACE VIEW `main-project-477501.analytics_v2.view_inventory_alerts` AS
WITH recent_sales AS (
  SELECT
    親ASIN,
    AVG(全体販売個数) AS 日平均販売個数,
    STDDEV(全体販売個数) AS 販売個数標準偏差
  FROM `main-project-477501.analytics.wide_date_parent_asin`
  WHERE 日付 >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND 全体販売個数 > 0
  GROUP BY 親ASIN
),
current_inventory AS (
  SELECT
    p.parent_asin,
    SUM(i.totalQuantity) AS 現在庫数,
    MAX(i.fetchedAt_jst_date) AS 最終更新日
  FROM `main-project-477501.native_table.fact_fba-inventory` i
  JOIN `main-project-477501.native_table.dim_products` p
    ON i.asin = p.child_asin OR i.asin = p.parent_asin
  WHERE i.fetchedAt_jst_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY p.parent_asin
)

SELECT
  ci.parent_asin AS ASIN,
  p.component_name AS 商品名,
  p.brand AS ブランド,

  -- 在庫状況
  ci.現在庫数,
  ci.最終更新日,

  -- 販売動向
  ROUND(rs.日平均販売個数, 1) AS 日平均販売個数,
  ROUND(rs.販売個数標準偏差, 1) AS 販売ばらつき,

  -- 在庫日数
  SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) AS 在庫日数,

  -- セーフティストック (平均 + 1標準偏差 × 7日分)
  ROUND((rs.日平均販売個数 + rs.販売個数標準偏差) * 7) AS 安全在庫数,

  -- 推奨発注数 (30日分の在庫を目標)
  GREATEST(0,
    CAST(ROUND(rs.日平均販売個数 * 30 - ci.現在庫数) AS INT64)
  ) AS 推奨発注数,

  -- アラートレベル
  CASE
    WHEN ci.現在庫数 = 0 THEN '🔴🔴 欠品中'
    WHEN SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) <= 3 THEN '🔴 緊急発注 (3日以内)'
    WHEN SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) <= 7 THEN '🟠 至急発注 (1週間以内)'
    WHEN SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) <= 14 THEN '🟡 発注検討 (2週間以内)'
    WHEN SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) <= 30 THEN '🟢 正常'
    WHEN SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数) <= 60 THEN '⚪ やや過剰'
    ELSE '🔵 過剰在庫'
  END AS 在庫ステータス,

  -- 在庫回転率 (年間)
  SAFE_DIVIDE(365, SAFE_DIVIDE(ci.現在庫数, rs.日平均販売個数)) AS 在庫回転率

FROM current_inventory ci
JOIN recent_sales rs ON ci.parent_asin = rs.親ASIN
LEFT JOIN `main-project-477501.native_table.dim_products` p ON ci.parent_asin = p.parent_asin
WHERE rs.日平均販売個数 > 0  -- 販売実績がある商品のみ
ORDER BY 在庫日数 ASC;


-- ============================================================
-- 5. 週次サマリービュー (ボーナス)
-- ============================================================
-- 用途: 週次レビュー会議での振り返り
-- 更新頻度: 週次
-- ============================================================

CREATE OR REPLACE VIEW `main-project-477501.analytics_v2.view_weekly_summary` AS
WITH weekly_data AS (
  SELECT
    週開始日,
    週終了日,
    年,
    週,
    SUM(売上高) AS 週間売上,
    SUM(広告費) AS 週間広告費,
    SUM(全体販売個数) AS 週間販売個数,
    SUM(セッション数) AS 週間セッション数
  FROM `main-project-477501.analytics.wide_weekly_parent_asin`
  WHERE 年 = EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY 週開始日, 週終了日, 年, 週
)

SELECT
  週開始日,
  週終了日,
  年,
  週,

  -- 売上指標
  週間売上,
  週間広告費,
  週間売上 - 週間広告費 AS 週間粗利益,

  -- 効率指標
  SAFE_DIVIDE(週間売上, 週間広告費) AS ROAS,
  SAFE_DIVIDE(週間販売個数, 週間セッション数) * 100 AS CVR,

  -- 前週比
  LAG(週間売上) OVER (ORDER BY 週開始日) AS 前週売上,
  SAFE_DIVIDE(週間売上 - LAG(週間売上) OVER (ORDER BY 週開始日),
              LAG(週間売上) OVER (ORDER BY 週開始日)) * 100 AS 前週比率,

  -- 移動平均 (4週間)
  AVG(週間売上) OVER (
    ORDER BY 週開始日
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ) AS 売上4週移動平均

FROM weekly_data
ORDER BY 週開始日 DESC;


-- ============================================================
-- 実行確認
-- ============================================================
-- 以下のクエリでビューが正しく作成されたか確認

SELECT
  table_name,
  table_type,
  TIMESTAMP_MILLIS(creation_time) AS created_at
FROM `main-project-477501.analytics_v2.__TABLES__`
ORDER BY table_name;
