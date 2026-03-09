# NocoDB テーブル → BQ マッピング

主要テーブル（会計系、opau ベース）:

| NocoDB テーブル名 | BQ テーブル名 | 備考 |
|-----------------|-------------|------|
| `nc_opau___楽天銀行ビジネス口座入出金明細` | `nocodb.rakuten_bank_statements` | |
| `nc_opau___PayPay銀行入出金明細` | `nocodb.paypay_bank_statements` | |
| `nc_opau___手動仕訳` | `nocodb.manual_journal_entries` | 開業費・事業主借の仕訳も含む |
| `nc_opau___NTTファイナンスBizカード明細` | `nocodb.ntt_finance_statements` | |
| `nc_opau___freee勘定科目` | `nocodb.account_items` | |
| `nc_opau___代行会社` | `nocodb.agency_transactions` | |
| `nc_opau___振替` | `nocodb.transfer_records` | 口座間資金移動リンク（43件） |

## 廃止テーブル（参照不要）

| テーブル名 | 廃止日 | 理由 |
|-----------|-------|------|
| `nc_opau___Amazon出品アカウント明細` | 2026-03-06 | settlement_journal_view 直接参照に変更 |
| `nc_opau___事業主借` | 2026-03-06 | 手動仕訳テーブルに統合 |
| `nc_opau___開業費` | 以前 | 事業主借→手動仕訳に移行済み |
