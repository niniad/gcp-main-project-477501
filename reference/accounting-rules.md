# 会計ルール（絶対遵守）

## 資金移動

- **THE直行便→YPの直接資金移動は存在しない**。必ず「楽天銀行に引き落とし → 楽天銀行からYP送金」の流れ。
- イーウーパスポート（三井住友）への振込 = YPへの預け金送金。THE直行便ではない。

## 事業主借

- MFの「楽天カード」= 個人用カード = **事業主借**。カード科目として認識する必要はない。
- NocoDB に開業費テーブルはない。開業費は `手動仕訳`（manual_journal_entries）テーブル内のエントリ。
- `事業主借` テーブルは 2026-03-06 に手動仕訳へ統合済みで削除済み。

## freee 同期

- freee は FY2023 のみ会計期間が存在（2023/1/1-2023/12/31）
- BQ→freee は振替伝票（manual_journals）で一括同期。同期スクリプト: `tmp/freee_sync_fy2023.py`
- freee の口座間振替（transfers）は使わない（manual journals と二重計上になる）
- freee には walletable-linked account と手動作成 account が重複するので注意

## Amazon 会計

- Amazon 売上は BQ `settlement_journal_view` から直接 `journal_entries` VIEW に取り込み（NocoDB 中間テーブルなし）。
- Amazon→楽天/PayPay への振替送金（DEPOSIT）は `振替` テーブル（43件）でリンクし、journal_entries VIEW から除外（二重計上防止）。
- MF は月次集約のため構造的差異あり（Amazon出品+売掛金の合算で比較）。

## 振替（資金移動）

- 口座間の資金移動は `振替` テーブル（transfer_records）でリンク。
- journal_entries VIEW では `振替_id IS NULL` で振替行を除外（P/Lに影響しない資金移動を排除）。
- NTTファイナンスの振替_id は月次支払バッチへのリンク用（振替フラグではない）。
