# 月次・年次締め作業フロー（AI主導ガイド）
*最終更新: 2026-03-06*

このドキュメントはAIが主導して月次・年次締め作業を進めるための手順書です。
AIはこのフローに従って**ユーザーに指示を出し**、確認・実行を依頼します。

---

## 📋 月次締め作業（毎月末）

### STEP 1: 銀行明細のインポート

**AIからの指示：**
> 「今月分の銀行明細をダウンロードしてNocoDB に入力してください。」
>
> - **楽天銀行**: ビジネス口座 → 明細CSV → NocoDB「楽天銀行ビジネス口座入出金明細」に追記
> - **PayPay銀行**: 明細CSV → NocoDB「PayPay銀行入出金明細」に追記
>
> 追記後、各行の `freee勘定科目` を確認・設定してください：
> - Amazonからの振込 → 科目=Amazon出品アカウント(9)、振替テーブルにリンク
> - セールモンスターからの振込 → 科目=セールモンスター(166)
> - ESPRIME向け送金 → 科目=ESPRIME(5)、振替テーブルにリンク
> - PayPayデビット AMAZON → 科目=Amazon出品アカウント(9)、振替テーブルにリンク
> - その他経費 → 適切な費用科目

**AIが確認するクエリ:**
```sql
-- 最新入力日の確認
SELECT MAX(取引日) FROM `main-project-477501.nocodb.rakuten_bank_statements`
SELECT MAX(取引日) FROM `main-project-477501.nocodb.paypay_bank_statements`
```

---

### STEP 2: NTTカード明細のインポート

**AIからの指示：**
> 「今月分のNTTファイナンスBizカード明細をNocoDB に入力してください。」
>
> - NTT利用明細CSV → NocoDB「NTTファイナンスBizカード明細」に追記
> - 各行の勘定科目（`freee科目_id`）が merchant_account_rules に基づいて自動設定されます
> - BQ同期後、AIが勘定科目の自動判定を確認します

---

### STEP 3: Amazon決済データの確認

**AIからの指示：**
> 「Amazonの精算データはSP-APIで自動取得されています。確認のみお願いします。」

**AIが確認するクエリ:**
```sql
-- 当月の精算データが取り込まれているか
SELECT settlement_id, DATE(booking_date), net_amount_check
FROM `main-project-477501.accounting.settlement_journal_view`
WHERE DATE(booking_date) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
ORDER BY booking_date
```

> もし精算データが欠落している場合 → Cloud Runジョブのログを確認してください。

---

### STEP 4: 新規振替リンクの設定

**AIからの指示：**
> 「銀行明細のうち、振替テーブルへのリンクが未設定の行があります。確認・設定してください。」

**AIが確認するクエリ:**
```sql
-- 振替_idが未設定のAmazon関連PayPay入金
SELECT id, 取引日, お預かり金額, 摘要
FROM `main-project-477501.nocodb.paypay_bank_statements`
WHERE freee勘定科目_id = 9  -- Amazon
  AND 振替_id IS NULL
ORDER BY 取引日

-- 対応するAmazon Settlement Net（振替リンク未設定）
SELECT id, 取引日, 金額, settlement_id
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE 振替_id IS NULL
  AND 金額 < 0  -- DEPOSIT行
ORDER BY 取引日
```

> 対応するペアを確認して、NocoDB の `振替` テーブルに新規レコードを追加し、
> 両テーブルの `振替_id` を設定してください。

---

### STEP 5: 代行会社への入金確認（ESPRIME向けがあれば）

**AIからの指示：**
> 「今月ESPRIME/YP/THE直行便への送金はありましたか？」

もし「はい」の場合：
> 「代行会社テーブルに対応する入金受取エントリを追加し、振替テーブルでリンクしてください。」
> - NocoDB「代行会社」→ 新規行追加（取引日・金額・freee科目=5など）
> - NocoDB「振替」→ 新規行追加 → PayPay銀行側と代行会社側の両エントリを振替_idでリンク

---

### STEP 6: セールモンスター売上のインポート（月次）

**AIからの指示：**
> 「今月分のセールモンスター売上レポートはありますか？」

もし「はい」の場合：
> 「セールモンスター売上レポートCSVをNocoDB にインポートしてください。」

---

### STEP 7: NocoDB → BQ 同期

**AIの実行指示：**
> 「全データ入力が完了したら、BQ同期を実行します。」

```bash
cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py
```

---

### STEP 8: 月次P/L確認

**AIが実行して結果を報告するクエリ:**
```sql
-- 当月のP/L概要
SELECT
  EXTRACT(MONTH FROM journal_date) AS month,
  SUM(CASE WHEN pl_contribution > 0 THEN pl_contribution ELSE 0 END) AS revenue,
  SUM(CASE WHEN pl_contribution < 0 THEN -pl_contribution ELSE 0 END) AS expenses,
  SUM(pl_contribution) AS net
FROM `main-project-477501.accounting.pl_journal_entries`
WHERE fiscal_year = EXTRACT(YEAR FROM CURRENT_DATE())
  AND EXTRACT(MONTH FROM journal_date) = EXTRACT(MONTH FROM CURRENT_DATE()) - 1
GROUP BY 1
```

> AIがP/Lを報告し、異常値（急激な費用増・売上減）があれば原因を確認します。

---

## 📋 年次締め作業（12月末〜翌年3月）

### PHASE 1: 期末データ収集（12月末）

#### 1-1. 月次締め（12月分）を完了させる
→ 上記の月次締め STEP 1〜8 を実施

#### 1-2. 棚卸在庫の確認

**AIの確認:**
```sql
-- 12月末FBA在庫 × 標準原価
SELECT *
FROM `main-project-477501.accounting.inventory_journal_view`
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE())
ORDER BY month DESC LIMIT 3
```

> 「12月末のFBA在庫数量がSP-APIから正しく取得されていることを確認してください。」
> 「標準原価（nocodb.standard_cost_history）の翌年分を登録してください。」

#### 1-3. ESPRIME残高の為替差損益調整

**AIの確認:**
> 「ESPRIMEの残高（CNY）を確認して、年末レートで円換算した差額を手動仕訳で処理します。」

```bash
# ESPRIME残高確認
SELECT *
FROM `main-project-477501.nocodb.agency_account_balances`
WHERE account = 'ESPRIME'
ORDER BY 取引日 DESC LIMIT 1
```

> 「年末の中国人民銀行レートまたは三菱UFJレートを確認して、差額を教えてください。」
> AIが手動仕訳（Dr.為替差損益 / Cr.ESPRIME または逆）を NocoDB に追加します。

#### 1-4. 未払金・未収金の確認

**AIの確認クエリ:**
```sql
SELECT account_name,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
WHERE account_name IN ('未払金', 'Amazon出品アカウント', 'セールモンスター')
GROUP BY 1
```

> Amazonの年末残高（12月31日精算済みで翌年入金のもの）が残高として計上されていることを確認。

---

### PHASE 2: BQ同期・P/L最終確認

```bash
# BQ同期
cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py
```

**AIが確認するクエリ:**
```sql
-- FY全体のP/L
SELECT fiscal_year, SUM(pl_contribution) AS net_income
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY 1 ORDER BY 1
-- 期待値: FY2023=-1,340,610 / FY2024=-1,088,882 / FY202X=合理的な値
```

> 過去年度（FY2023/2024）の値が変わっていないことを確認します。
> FY2025以降の純損益が合理的な範囲か確認します。

---

### PHASE 3: freee 同期

**AIの実行指示:**
```bash
cd C:/Users/ninni/projects/gcp-main-project-477501
C:/Users/ninni/projects/nocodb-to-bq/.venv/Scripts/python.exe tmp/freee_sync_fy202X.py
```

> 「同期が完了したら、freeeの試算表を確認してください：」
> - 損益計算書のP/Lが BQ と一致しているか
> - 貸借対照表の主要勘定残高が期待値と一致しているか
> - 資産合計 = 負債合計 + 純資産合計（貸借一致）

**freee試算表で確認すべき科目:**

| 科目 | 確認内容 |
|---|---|
| 楽天銀行 | 実際の残高と一致 |
| PayPay銀行 | 実際の残高と一致 |
| Amazon出品アカウント | 12/31精算の未入金分 |
| ESPRIME | CNY残高×年末レート ≈ 円建残高 |
| 商品 | SP-API在庫数×標準原価 |
| 未払金 | ¥0（NTTカード当月分は翌月に反映） |

---

### PHASE 4: 確定申告

> 「freeeの損益計算書・貸借対照表を確認後、確定申告の準備ができています。」
>
> **確定申告で使用する数値:**
> - 事業所得（青色）= freee 差引損益計算 の値（△は赤字）
> - 期末商品棚卸高 = 商品残高
> - 開業費残高 = 繰延資産として申告書に記載

---

## 📋 特別処理の手順

### Amazon不足金支払いの処理

Amazonの精算でネット金額がマイナスになった場合（不足金）の処理手順:

1. **PayPay明細確認**: 「Vデビット AMAZON.CO.JP」のPayPay出金を確認
2. **振替テーブルに追加**: 振替_id を新規採番
3. **PayPay側の設定**: `freee勘定科目_id=9（Amazon出品アカウント）`、`振替_id=新ID`
4. **Amazon側の設定**: 対応するSettlement Net行に `振替_id=新ID` を設定

### NTT経由の不足金支払い処理

NTTカードでAmazonの不足金を支払った場合:
- NTTカード明細の当該行: `freee科目=Amazon出品アカウント(9)`
- Amazon Settlement Net の対応行: 振替_idは設定しない（NTTの振替_idは月次バッチリンク用）
- journal_entries の `is_transfer=FALSE` フィルタを確認

### ESPRIME/代行会社への新規送金の処理

1. PayPay明細に送金を追加（freee科目=ESPRIME(5), 振替_id=新ID）
2. 代行会社テーブルに受取エントリを追加（振替_id=新ID）
3. 振替テーブルに新規レコード追加

---

## 📋 緊急確認用クエリ集

```sql
-- 全年度P/L確認
SELECT fiscal_year, SUM(pl_contribution) AS net_income
FROM `main-project-477501.accounting.pl_journal_entries`
GROUP BY 1 ORDER BY 1;

-- BS主要科目残高確認（全年度累計）
SELECT account_name,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS balance
FROM `main-project-477501.accounting.journal_entries`
GROUP BY account_name
HAVING balance != 0
ORDER BY ABS(balance) DESC;

-- 仕訳バランス確認（借方=貸方になっているか）
SELECT fiscal_year,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE 0 END) AS total_debit,
  SUM(CASE WHEN entry_side='credit' THEN amount_jpy ELSE 0 END) AS total_credit,
  SUM(CASE WHEN entry_side='debit' THEN amount_jpy ELSE -amount_jpy END) AS imbalance
FROM `main-project-477501.accounting.journal_entries`
GROUP BY 1 ORDER BY 1;
-- imbalance が 0 であることを確認

-- 振替リンク未設定のAmazon DEPOSIT確認
SELECT id, 取引日, 金額, settlement_id
FROM `main-project-477501.nocodb.amazon_account_statements`
WHERE 振替_id IS NULL AND 金額 < 0
ORDER BY 取引日;
```

---

## 📋 システム稼働確認チェックリスト

| チェック項目 | 確認方法 |
|---|---|
| NocoDB が起動しているか | `http://localhost:8080` にアクセス |
| BQ同期スクリプトが動くか | `cd nocodb-to-bq && uv run python main.py` |
| Cloud Run ジョブが正常か | GCP コンソール → Cloud Run → Jobs |
| freee APIトークンが有効か | freee同期スクリプトを実行して確認 |
| NocoDB バックアップが存在するか | `G:/マイドライブ/backup/nocodb/` を確認 |
