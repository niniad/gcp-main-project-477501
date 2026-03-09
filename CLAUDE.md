# CLAUDE.md — accounting

## コンパクション後の復帰手順

コンパクション直後は必ず: (1) この CLAUDE.md を再読 (2) 現在のタスクを確認してから再開

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
| `reference/accounting-rules.md` | **会計ルール（絶対遵守）← 仕訳判断時は必読** |
| `reference/nocodb-tables.md` | NocoDB テーブル → BQ マッピング表 |
| `reference/account-ids.md` | 勘定科目ID一覧 |
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

## 作業手順

### NocoDB データ修正 → BQ 反映
1. SQLite 直接 UPDATE: `C:/Users/ninni/nocodb/noco.db`
2. BQ sync: `cd C:/Users/ninni/projects/nocodb-to-bq && uv run python main.py`
3. 検証: BQ クエリで確認

### freee 同期
1. BQ sync 完了後
2. `cd C:/Users/ninni/projects/accounting && uv run --with requests --with google-cloud-secret-manager --with google-auth --with google-cloud-bigquery python tmp/freee_sync_fy2023.py`
3. freee API で trial_bs/trial_pl 検証

## Python 実行環境
- `uv run python` を使用
- freee API 依存: `--with requests --with google-cloud-secret-manager --with google-auth`
- BQ 依存: `--with google-cloud-bigquery`
- 日本語出力: `sys.stdout.reconfigure(encoding='utf-8')` 必須
