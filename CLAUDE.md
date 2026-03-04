# CLAUDE.md — gcp-main-project-477501

## プロジェクト概要

個人EC事業（Amazon.co.jp）の会計・分析基盤。NocoDB → BigQuery → freee の3層構成。
2023/2024年度はMF（マネーフォワード）確定申告値と照合。2025年以降はこのシステムが唯一の会計記録。

## 重要ファイル

| ファイル | 内容 |
|---------|------|
| `system_design.md` | システム全体設計（データフロー、テーブル定義、VIEW定義） |
| `accounting_policies.md` | 会計方針（日付基準、仕訳ルール等） |
| `mf_bq_reconciliation.md` | MF⇔BQ照合結果と差異分析 |
| `scheduled_queries/` | BQ Scheduled Query スクリプト |
| `tmp/` | 一時スクリプト（.gitignore対象） |

## 外部リソース

| リソース | パス / ID |
|---------|-----------|
| **NocoDB SQLite** | `C:/Users/ninni/nocodb/noco.db` |
| **nocodb-to-bq sync** | `cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py` |
| **BQ Project** | `main-project-477501` |
| **freee スキル** | `C:/Users/ninni/.claude/skills/freee/` |
| **freee Company ID** | `11078943` |
| **MF FY2023 総勘定元帳** | `C:/Users/ninni/projects/rawdata/マネーフォワード/MF_総勘定元帳_2023年度.csv` |
| **MF FY2024 総勘定元帳** | `C:/Users/ninni/projects/rawdata/マネーフォワード/MF_総勘定元帳_2024年度.csv` |
| **MF FY2023 決算書** | `C:/Users/ninni/projects/rawdata/マネーフォワード/MF2023年度決算書.pdf` |

## 会計ルール（絶対遵守）

### 資金移動
- **THE直行便→YPの直接資金移動は存在しない**。必ず「楽天銀行に引き落とし → 楽天銀行からYP送金」の流れ。
- イーウーパスポート（三井住友）への振込 = YPへの預け金送金。THE直行便ではない。

### 事業主借
- MFの「楽天カード」= 個人用カード = **事業主借**。カード科目として認識する必要はない。
- NocoDB に開業費テーブルはない。開業費は `事業主借`（owner_contribution）テーブル内のエントリ。

### freee 同期
- freee は FY2023 のみ会計期間が存在（2023/1/1-2023/12/31）
- BQ→freee は振替伝票（manual_journals）で一括同期。同期スクリプト: `tmp/freee_sync_fy2023.py`
- freee の口座間振替（transfers）は使わない（manual journals と二重計上になる）
- freee には walletable-linked account と手動作成 account が重複するので注意

### Amazon 会計
- Amazon出品アカウント = 売掛金の口座。NocoDB に `Amazon出品アカウント明細` テーブルとして管理（694件）。
- BQ settlement_journal_view から口座視点に変換して NocoDB に格納。入金=+、手数料=-、銀行送金(DEPOSIT)=-。
- 楽天銀行への送金（DEPOSIT行）は振替テーブル経由でリンク済み（45件）。
- 振替リンク済みの行は journal_entries VIEW から除外（二重計上防止）。
- MF は月次集約のため構造的差異あり（Amazon出品+売掛金の合算で比較）。

### 振替（資金移動）
- 口座間の資金移動は `振替` テーブル（transfer_records）でリンク。
- journal_entries VIEW では `振替_id IS NULL` で振替行を除外（P/Lに影響しない資金移動を排除）。
- NTTファイナンスの振替_id は月次支払バッチへのリンク用（振替フラグではない）。

## NocoDB テーブル → BQ マッピング

主要テーブル（会計系、21テーブル中）:
- `nc_opau___楽天銀行ビジネス口座入出金明細` → `nocodb.rakuten_bank_statements`
- `nc_opau___PayPay銀行入出金明細` → `nocodb.paypay_bank_statements`
- `nc_opau___Amazon出品アカウント明細` → `nocodb.amazon_account_statements`
- `nc_opau___事業主借` → `nocodb.owner_contribution_entries`
- `nc_opau___手動仕訳` → `nocodb.manual_journal_entries`
- `nc_opau___NTTファイナンスBizカード明細` → `nocodb.ntt_finance_statements`
- `nc_opau___freee勘定科目` → `nocodb.account_items`
- `nc_opau___代行会社` → `nocodb.agency_transactions`
- `nc_opau___振替` → `nocodb.transfer_records`

## NocoDB 勘定科目ID（頻出）

| nocodb_id | account_name |
|-----------|-------------|
| 3 | THE直行便 |
| 5 | ESPRIME |
| 6 | 楽天銀行 |
| 7 | YP |
| 8 | PayPay銀行 |
| 9 | Amazon出品アカウント |
| 12 | 売掛金 |
| 15 | 開業費 |
| 70 | 未払金 |
| 85 | 事業主借 |
| 99 | 売上高 |
| 100 | 売上値引高 |
| 101 | 売上戻り高 |
| 104 | 雑収入 |
| 105 | 為替差損益 |
| 109 | 仕入高 |
| 119 | 荷造運賃 |
| 124 | 通信費 |
| 125 | 広告宣伝費 |
| 126 | 販売手数料 |
| 146 | 地代家賃 |
| 148 | 支払手数料 |
| 156 | 諸会費 |
| 162 | 雑費 |

## 作業手順

### NocoDB データ修正 → BQ 反映
1. SQLite 直接 UPDATE: `C:/Users/ninni/nocodb/noco.db`
2. BQ sync: `cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py`
3. 検証: BQ クエリで確認

### freee 同期
1. BQ sync 完了後
2. `cd C:/Users/ninni/projects/gcp-main-project-477501 && uv run --with requests --with google-cloud-secret-manager --with google-auth --with google-cloud-bigquery python tmp/freee_sync_fy2023.py`
3. freee API で trial_bs/trial_pl 検証

## Python 実行環境
- `uv run python` を使用
- freee API 依存: `--with requests --with google-cloud-secret-manager --with google-auth`
- BQ 依存: `--with google-cloud-bigquery`
- 日本語出力: `sys.stdout.reconfigure(encoding='utf-8')` 必須
