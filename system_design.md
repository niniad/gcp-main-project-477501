# EC事業 システム設計書
*最終更新: 2026-02-23*

---

## 1. システム全体データフロー

```
【自動取得】
  Amazon SP-API ──→ Cloud Run (spapi-to-gcs-daily) ──→ GCS ──→ BQ (sp_api_external)
  Amazon Ads API ─→ Cloud Run (amazon-ads-to-gcs-daily) ─→ GCS ─→ BQ (amazon_ads_external)

【手動入力】
  代行会社帳票 ──→ NocoDB EC base（振替・発注ロット・発注明細）
  銀行・カード ──→ NocoDB EC base（各明細テーブル、CSV手動インポート）
  商品マスタ  ──→ NocoDB EC base（製品・購入商品・標準原価履歴）
  輸入記録    ──→ NocoDB EC base（輸入ロット・輸入明細 ※数量参照のみ）

【NocoDB → BigQuery エクスポート（定期）】
  振替テーブル         ──→ BQ (nocodb.振替)
  標準原価履歴         ──→ BQ (analytics.stg_unit_costs)
  製品マスタ           ──→ BQ (nocodb.dim_products)
  銀行・カード明細      ──→ BQ (nocodb.銀行明細等)
  セールモンスター売上  ──→ BQ (nocodb.sale_monster)

【BigQuery → freee インポート】
  BQ (freee.settlement_journal_payload_view) → freee（Amazon売上・経費）
  BQ (freee.proxy_journal_view)              → freee（代行会社仕入・預け金）※要作成
  BQ (freee.bank_journal_view)               → freee（銀行入出金・経費）※要作成
  BQ (freee.inventory_journal_view)          → freee（月次棚卸）※要作成
```

---

## 2. データ管理の役割分担（2026-02-23 確定）

| ツール | 役割 |
|--------|------|
| **NocoDB EC base** | 手動入力データのすべて（代行会社・銀行・商品マスタ等） |
| **GCS → BQ** | SP-API / Ads API 自動取得データ |
| **BigQuery** | 全データの統合・仕訳計算・分析（freeeへの投入もここで計算） |
| **freee** | BQで計算した仕訳をインポートするだけ。直接入力はしない |
| **Google Sheets** | **廃止**（agency_ledger, po_details等 → NocoDB移管済み） |

---

## 3. NocoDB EC base テーブル一覧

### 代行会社・発注系

| テーブル | 役割 | BQエクスポート |
|---------|------|-------------|
| 代行会社 | ESPRIME/YP/THE直行便マスタ | 不要 |
| 振替 | 代行会社への預け金入出金の全記録（全費用の正） | **必要** |
| 発注ロットマスタ | PO単位のヘッダ | 不要 |
| 発注明細 | POの品目・数量・外貨単価の内訳（振替「商品代金」行の内訳） | **不要** ※後述 |

### 輸入系（数量参照のみ）

| テーブル | 役割 | BQエクスポート |
|---------|------|-------------|
| 輸入ロットマスタ | 船便単位のヘッダ | 不要 |
| 輸入明細 | 製品ごとの輸入数量の参照記録（金額列は不要） | 不要 |

> **発注 ≠ 輸入**：1POが複数輸入ロットに跨がること・1輸入ロットに複数POが混載されることがある。
> 輸入明細は数量の参照用として保持するが、コスト計算には使用しない。

### 商品マスタ系

| テーブル | 役割 | BQエクスポート |
|---------|------|-------------|
| 製品マスタ | SKU/ASIN等の完成品マスタ | **必要** |
| 購入商品マスタ | 中国から仕入れる原材料・部品（PRD/MAT） | 不要 |
| セット構成マスタ | 購入商品 → 製品の構成比（参照用） | 不要 |
| 標準原価履歴 | SKU別標準原価（年次手動更新） | **必要** |
| GTIN | JANコード管理 | 不要 |

### 財務・銀行系

| テーブル | 役割 | BQエクスポート |
|---------|------|-------------|
| 楽天銀行ビジネス口座入出金明細 | CSV手動インポート | **必要** |
| PayPay銀行入出金明細 | CSV手動インポート（2025〜） | **必要** |
| NTTファイナンスBizカード明細 | CSV手動インポート | **必要** |
| セールモンスター売上レポート | CSV手動インポート | **必要** |
| freee勘定科目 | freee科目マスタ参照 | 不要 |
| Amazonレビュー | レビュー管理 | 不要 |

---

## 4. BigQuery データセット構成

| データセット | 内容 | 状態 |
|------------|------|------|
| `sp_api_external` | SP-API raw data（settlement, sales/traffic, FBA inventory等） | 稼働中 |
| `amazon_ads_external` | 広告API raw data | 稼働中 |
| `analytics` | 分析ビュー（rpt_pnl_5stage, rpt_pnl_real, stg_unit_costs等） | 稼働中 |
| `freee` | freee仕訳ビュー群 + マスタ（account_map等） | 構築中 |
| `nocodb` | NocoDB各テーブルのエクスポート先 | **未作成・要整備** |
| `google_sheets` | 旧GS外部テーブル（廃止予定） | 廃止予定 |
| `gss_connected` | 旧GS直接連携（廃止予定） | 廃止予定 |
| `assets` | Obsidian画像カタログ | 稼働中 |

---

## 5. 原価計算・会計方針（2026-02-23 確定）

### 5.1 基本方針

| 用途 | 原価の種類 | 元データ |
|------|-----------|---------|
| freee 月次棚卸評価 | **標準原価**（年1回手動更新） | NocoDB標準原価履歴 → BQ |
| freee 仕入高計上 | **実際支払額**（直近入金レート換算） | NocoDB振替テーブル → BQ |
| BQ 月次損益分析 | 標準原価 × 販売数量 | analytics.stg_unit_costs |
| 標準原価の年次精度確認 | 発注明細の加重平均単価 + 間接費率 | NocoDB内で手計算 → 標準原価履歴更新 |

- **為替レート**: 税関公示レートではなく、振替テーブルに記録した**直近入金レート**を使用
- **実勢原価のSKU別按分は行わない**（代行会社でのセット組みが介在するため精度が出ない）
- **発注明細 → BQ エクスポートは不要**（標準原価は年1回手動更新のみ）

### 5.2 月次会計フロー

```
① 代行会社帳票（NocoDB振替）発生時
     仕入高 Dr / 預け金 Cr  ← BQが振替テーブルから計算してfreeeへ

② 月末棚卸
     FBA在庫レポート（BQ sp_api_external.fba-inventory）で数量確認
     × 標準原価（BQ analytics.stg_unit_costs）= 期末棚卸高
     商品 Dr / 仕入高 Cr  ← BQが計算してfreeeへ

③ 結果: 売上原価 = 当月仕入高(実額) - 棚卸増減(標準原価評価)
```

### 5.3 標準原価の年次更新手順

```
1. 振替テーブルで年間の間接費率を算出
   間接費率 = (振替合計 - 商品代金合計) / 商品代金合計

2. 発注明細でSKU別の加重平均仕入単価を算出（NocoDB内で手計算）
   ※ 購入商品(PRD/MAT) → セット構成マスタ → 製品(SKU) の変換を考慮

3. 標準原価_翌年 = 加重平均仕入単価 × (1 + 間接費率)

4. NocoDB「標準原価履歴」テーブルに登録 → BQへエクスポート
```

### 5.4 過年度データ方針

| 年度 | 方針 |
|------|------|
| 2023年 | MF確定申告値をターゲット（売上456,547・仕入621,932） |
| 2024年 | MF確定申告値をターゲット（売上1,184,778・仕入1,563,278） |
| 2025年〜 | このシステムで正確な数値を把握 |

差異は年末調整仕訳で吸収（修正申告は行わない）。

---

## 6. freee連携フロー（構築中）

| freee仕訳の種類 | BQビュー（計算元） | 元データ |
|----------------|-----------------|---------|
| Amazon売上・手数料 | `freee.settlement_journal_payload_view` | sp_api_external（稼働中） |
| 代行会社仕入・預け金 | `freee.proxy_journal_view` ※要作成 | nocodb.振替 |
| 銀行入出金・経費 | `freee.bank_journal_view` ※要作成 | nocodb.銀行明細等 |
| 月次棚卸 | `freee.inventory_journal_view` ※要作成 | sp_api_external + stg_unit_costs |
| セールモンスター売上 | `freee.sale_monster_view` ※要作成 | nocodb.セールモンスター売上 |

**freeeには直接入力しない。すべてBQで計算した結果をインポートする。**

### freee再構築ステータス

- [x] データソース解析完了（MF CSV / xlsx / BQ）
- [x] 確定申告ターゲット値確認
- [x] freee MCP サーバー設計
- [ ] NocoDB → BQ エクスポートパイプライン整備
- [ ] freee MCP サーバー実装
- [ ] 新BQビュー群作成（proxy/bank/inventory/sale_monster）
- [ ] 既存freeeデータバックアップ・削除
- [ ] 2023〜2025年度データインポート

---

## 7. Cloud Run ジョブ

| プロジェクト | 内容 | 頻度 |
|------------|------|------|
| spapi-to-gcs-daily | SP-API各種レポート → GCS | 日次 |
| amazon-ads-to-gcs-daily | 広告APIデータ → GCS | 日次 |
| fetch-customs-exchange-rate | 税関公示レート → GCS（参考値として保持） | 週次 |

---

## 8. 次のアクション（優先順）

1. **NocoDB → BQ エクスポートパイプライン作成**（最優先）
   - 対象: 振替・標準原価履歴・製品マスタ・銀行明細・セールモンスター売上
   - 方式: 定期実行スクリプト or Cloud Run Job
   - 出力先: BQ `nocodb` データセット（新規作成）

2. **freee BQビュー群作成**
   - `freee.proxy_journal_view`（代行会社仕入）
   - `freee.bank_journal_view`（銀行入出金）
   - `freee.inventory_journal_view`（月次棚卸）

3. **freee MCP サーバー実装** → freee全データ再構築

4. **輸入明細の金額列整理**（不要列の削除）
   - 残す列: 輸入ロット_ID・製品_ID・輸入数量・入荷日
   - 削除対象: 仕入単価_円・OCS運賃_単価・代行費按分_円・関税按分_円・実勢原価_円

---

## 9. 設計文書・参考リンク

| ドキュメント | 場所 |
|-----------|------|
| ゴール定義 | [goal.md](goal.md) |
| freee再構築計画 | [freee/freee_rebuild_plan.md](../freee/freee_rebuild_plan.md) |
| 過去システム構成図 | obsidian/EC/system-architecture.md |
| 意思決定記録 | obsidian/EC/decisions.md |
