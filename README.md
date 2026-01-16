# GCP Main Project

Google Cloud Platform 開発環境

## プロジェクト情報

- **プロジェクトID**: `main-project-477501`
- **サービスアカウント**: `claude-code-dev@main-project-477501.iam.gserviceaccount.com`

## 利用可能なGCPサービス

| サービス | CLI | Python SDK |
|---------|-----|------------|
| Cloud Storage (GCS) | gsutil | google-cloud-storage |
| BigQuery | bq | google-cloud-bigquery |
| Secret Manager | gcloud secrets | google-cloud-secret-manager |
| Cloud Functions | gcloud functions | google-cloud-functions |
| Cloud Scheduler | gcloud scheduler | google-cloud-scheduler |
| Cloud Logging | gcloud logging | google-cloud-logging |

## セットアップ

新しいセッションで環境を初期化する場合:

```bash
./scripts/setup-gcp-env.sh
```

## サービスアカウントキー

キーファイルは `/root/.config/gcloud/service-account-key.json` に配置します。

**注意**: キーファイルはGitにコミットしないでください。
