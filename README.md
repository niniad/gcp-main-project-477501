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

### Linux / macOS / Claude Code on the Web

新しいセッションで環境を初期化する場合:

```bash
./scripts/setup-gcp-env.sh
```

### Windows

Windows環境でのセットアップについては、以下のドキュメントを参照してください:

- **🚀 [クイックスタート（5分）](docs/windows-quickstart.md)** - 最速でセットアップ
- **📚 [詳細ガイド](docs/windows-setup-guide.md)** - 詳しい手順とトラブルシューティング

#### Windows クイックセットアップ

```powershell
# 1. 必要なツールをインストール
winget install Git.Git
winget install OpenJS.NodeJS.LTS
winget install Python.Python.3.12

# 2. Claude Code CLIをインストール
npm install -g @anthropic-ai/claude-code

# 3. リポジトリをクローン
git clone https://github.com/niniad/gcp-main-project-477501.git
cd gcp-main-project-477501

# 4. 自動セットアップを実行
.\scripts\windows\setup-dev-env.ps1

# 5. 接続テスト
.\scripts\windows\test-gcp-connection.ps1
```

## サービスアカウントキー

### Linux / macOS / Claude Code on the Web

キーファイルは `/root/.config/gcloud/service-account-key.json` に配置します。

### Windows

キーファイルは `C:\Users\<YourUsername>\.gcp\service-account-key.json` に配置します。

**注意**: キーファイルはGitにコミットしないでください。`.gitignore` に追加されています。
