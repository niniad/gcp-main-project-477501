# Windows クイックスタートガイド

最速でWindows環境にClaude Code + GCP開発環境を構築する手順です。

## 🚀 5分でセットアップ

### ステップ 1: 前提条件の確認

PowerShellを開いて以下を実行：

```powershell
# PowerShellのバージョン確認（5.1以上が必要）
$PSVersionTable.PSVersion

# wingetが使えるか確認（Windows 10/11）
winget --version
```

### ステップ 2: 必要なツールのインストール

```powershell
# 管理者権限でPowerShellを開く（右クリック → 管理者として実行）

# Git
winget install Git.Git

# Node.js
winget install OpenJS.NodeJS.LTS

# Python
winget install Python.Python.3.12

# Google Cloud CLI
# インストーラーをダウンロードして実行
Start-Process "https://cloud.google.com/sdk/docs/install"
```

インストール後、**PowerShellを再起動**してください。

### ステップ 3: Claude Codeのインストール

```powershell
# npmでインストール
npm install -g @anthropic-ai/claude-code

# インストール確認
claude --version
```

### ステップ 4: GCPサービスアカウントキーの配置

1. GCPコンソールからサービスアカウントキー（JSON）をダウンロード
2. 以下のフォルダに保存：

```powershell
# フォルダを作成
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.gcp"

# ダウンロードしたキーをコピー（例）
Copy-Item "C:\Users\YourName\Downloads\service-account-key.json" "$env:USERPROFILE\.gcp\service-account-key.json"
```

### ステップ 5: 自動セットアップスクリプトの実行

```powershell
# プロジェクトをクローン
git clone https://github.com/niniad/gcp-main-project-477501.git
cd gcp-main-project-477501

# 実行ポリシーを設定（初回のみ）
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# セットアップスクリプトを実行
.\scripts\windows\setup-dev-env.ps1
```

スクリプトは自動的に：
- ✅ インストールされているツールを確認
- ✅ GCP環境変数を設定
- ✅ gcloud認証を実行
- ✅ Python仮想環境を作成
- ✅ GCPライブラリをインストール
- ✅ PowerShellプロファイルを設定

### ステップ 6: 接続テスト

```powershell
# 新しいPowerShellウィンドウを開いて実行
.\scripts\windows\test-gcp-connection.ps1
```

すべて ✓ が表示されれば成功です！

## 🎯 使い方

### Claude Codeの起動

```powershell
# プロジェクトディレクトリで
claude

# または特定のプロンプトで
claude "このプロジェクトの構造を説明して"
```

### GCP環境の初期化

```powershell
# 新しいPowerShellセッションで実行
gcp-init
```

### Python仮想環境の使用

```powershell
# 有効化
.\venv\Scripts\Activate.ps1

# 無効化
deactivate
```

### よく使うコマンド

```powershell
# gcloudコマンド
gcloud projects list
gcloud config list

# Cloud Storageの操作
gsutil ls
gsutil cp file.txt gs://your-bucket/

# BigQueryの操作
bq ls
bq query "SELECT * FROM dataset.table LIMIT 10"
```

## ⚠️ トラブルシューティング

### コマンドが見つからない

```powershell
# PATHを確認
$env:PATH

# PowerShellを再起動して再試行
```

### スクリプトが実行できない

```powershell
# 実行ポリシーを確認
Get-ExecutionPolicy

# 変更が必要な場合
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 認証エラー

```powershell
# サービスアカウントキーを確認
Test-Path $env:GOOGLE_APPLICATION_CREDENTIALS

# 再認証
gcloud auth activate-service-account --key-file=$env:GOOGLE_APPLICATION_CREDENTIALS
gcloud config set project your-project-id
```

### Python仮想環境のエラー

```powershell
# 仮想環境を削除して再作成
Remove-Item -Recurse -Force venv
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install --upgrade pip
```

## 📚 詳細ドキュメント

より詳しい情報は以下を参照してください：

- **詳細セットアップ**: [docs/windows-setup-guide.md](windows-setup-guide.md)
- **Claude Code公式**: https://docs.anthropic.com/claude/docs/claude-code
- **GCP CLI**: https://cloud.google.com/sdk/docs

## 🔧 カスタマイズ

### 特定のPythonライブラリのみインストール

```powershell
# requirements.txtを作成
@"
google-cloud-storage==2.10.0
google-cloud-bigquery==3.11.0
"@ | Out-File -FilePath requirements.txt -Encoding UTF8

# セットアップスクリプトを実行（requirements.txtがあれば自動的に使用されます）
.\scripts\windows\setup-dev-env.ps1
```

### 複数のGCPプロジェクトを使い分ける

```powershell
# プロジェクトを切り替え
gcloud config set project project-1
gcloud config set project project-2

# 現在のプロジェクトを確認
gcloud config get-value project
```

## 💡 ヒント

1. **VS Codeとの統合**
   - VS Codeのターミナルから直接 `claude` コマンドが使えます
   - プロジェクトを開いて `` Ctrl + ` `` でターミナルを起動

2. **エイリアスの活用**
   - PowerShellプロファイルに自分用のエイリアスを追加できます
   - `notepad $PROFILE` で編集

3. **バックグラウンドで実行**
   - 時間のかかるコマンドは `Start-Job` で実行
   ```powershell
   Start-Job -ScriptBlock { gcloud compute instances list }
   ```

4. **ログの確認**
   ```powershell
   # gcloudのログ
   gcloud info --log-http
   ```

## 🆘 サポート

問題が発生した場合：

1. [トラブルシューティング](#トラブルシューティング)セクションを確認
2. `.\scripts\windows\test-gcp-connection.ps1` を実行して診断
3. GitHubのIssuesで質問
4. [詳細ガイド](windows-setup-guide.md)を参照

---

**次のステップ**: [詳細セットアップガイド](windows-setup-guide.md) を読んでより高度な設定を学ぶ
