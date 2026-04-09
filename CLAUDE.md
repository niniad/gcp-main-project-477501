# CLAUDE.md — accounting

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
| `scripts/journal_entries_view.py` | **journal_entries VIEW 定義（最重要）← 変更時はここを編集してBQに再デプロイ** |
| `scripts/monthly_closing_audit.py` | 月次締め完了チェック（未分類取引の洗い出し） |
| `scripts/full_audit.py` | 全テーブル完全監査 |
| `scripts/freee_sync_fy2025.py` | freee への仕訳同期（FY2025。翌年は複製して年度変更） |
| `scripts/create_scheduled_queries.py` | BQ Scheduled Query の登録スクリプト |
| `reference/accounting-rules.md` | **会計ルール（絶対遵守）← 仕訳判断時は必読** |
| `reference/nocodb-tables.md` | NocoDB テーブル → BQ マッピング表 |
| `reference/account-ids.md` | 勘定科目ID一覧 |
| `tmp/` | 一時スクリプト（.gitignore対象、いつ削除しても可） |

## 外部リソース

| リソース | パス / ID |
|---------|-----------|
| **NocoDB SQLite** | `C:/Users/ninni/nocodb/noco.db` |
| **nocodb-to-bq sync** | `cd C:/Users/ninni/infra/nocodb-to-bq && uv run python main.py` |
| **BQ Project** | `main-project-477501` |
| **freee スキル** | `C:/Users/ninni/.claude/skills/freee/` |
| **freee Company ID** | `11078943` |
| **MF FY2023 総勘定元帳** | `C:/Users/ninni/projects/rawdata/マネーフォワード/MF_総勘定元帳_2023年度.csv` |
| **MF FY2024 総勘定元帳** | `C:/Users/ninni/projects/rawdata/マネーフォワード/MF_総勘定元帳_2024年度.csv` |
| **MF FY2023 決算書** | `C:/Users/ninni/projects/rawdata/マネーフォワード/MF2023年度決算書.pdf` |

## 作業手順

- **NocoDB → BQ反映**: NocoDB修正後は `nocodb-to-bq` で同期し、BQクエリで整合性を検証すること
- **freee同期**: BQ sync完了後に `scripts/freee_sync_fy2025.py` を実行し、trial_bs/trial_pl で検証
- **sync/freeeスクリプトのパス**: 外部リソースの nocodb-to-bq と freee スキルを参照
