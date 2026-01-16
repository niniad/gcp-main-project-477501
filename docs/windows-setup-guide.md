# Windows開発環境構築ガイド - Claude Code

Windows環境でClaude Codeを使用するための開発環境構築ガイドです。

## 目次

1. [前提条件](#前提条件)
2. [Claude Code CLIのインストール](#claude-code-cliのインストール)
3. [必要なツールのインストール](#必要なツールのインストール)
4. [GCP環境の設定](#gcp環境の設定)
5. [自動セットアップスクリプト](#自動セットアップスクリプト)
6. [トラブルシューティング](#トラブルシューティング)

---

## 前提条件

- Windows 10/11 (64-bit)
- 管理者権限
- インターネット接続

## Claude Code CLIのインストール

### 方法1: npm経由（推奨）

```powershell
# Node.jsがインストールされている場合
npm install -g @anthropic-ai/claude-code
```

### 方法2: 公式インストーラー

1. [Claude Code 公式サイト](https://docs.anthropic.com/claude/docs/claude-code)からWindows用インストーラーをダウンロード
2. ダウンロードした `.exe` ファイルを実行
3. インストールウィザードに従ってインストール

### インストール確認

```powershell
claude --version
```

## 必要なツールのインストール

### 1. Git for Windows

```powershell
# wingetを使用（Windows 10/11）
winget install Git.Git

# または公式サイトからダウンロード
# https://git-scm.com/download/win
```

**設定:**
```powershell
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

### 2. Node.js

```powershell
# wingetを使用
winget install OpenJS.NodeJS.LTS

# バージョン確認
node --version
npm --version
```

### 3. Python

```powershell
# wingetを使用
winget install Python.Python.3.12

# バージョン確認
python --version
pip --version
```

### 4. Google Cloud CLI

```powershell
# PowerShellで実行
(New-Object Net.WebClient).DownloadFile("https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe", "$env:Temp\GoogleCloudSDKInstaller.exe")
& $env:Temp\GoogleCloudSDKInstaller.exe
```

インストール後、新しいPowerShellウィンドウで：

```powershell
gcloud --version
```

### 5. Visual Studio Code（オプション）

```powershell
winget install Microsoft.VisualStudioCode
```

## GCP環境の設定

### 1. サービスアカウントキーの準備

GCPコンソールからサービスアカウントキー（JSON）をダウンロードし、安全な場所に保存します。

推奨パス: `C:\Users\<YourUsername>\.gcp\service-account-key.json`

### 2. 環境変数の設定

#### 方法A: PowerShellプロファイル（推奨）

PowerShellプロファイルを編集：

```powershell
# プロファイルファイルを開く
notepad $PROFILE

# 存在しない場合は作成
if (!(Test-Path -Path $PROFILE)) {
    New-Item -ItemType File -Path $PROFILE -Force
}
```

以下を追加：

```powershell
# GCP環境変数
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\Users\<YourUsername>\.gcp\service-account-key.json"
$env:GCP_PROJECT_ID = "your-project-id"

# PATH設定
$env:PATH = "$env:PATH;C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin"

# gcloud認証（初回のみ）
function Initialize-GCP {
    gcloud auth activate-service-account --key-file=$env:GOOGLE_APPLICATION_CREDENTIALS
    gcloud config set project $env:GCP_PROJECT_ID
    Write-Host "GCP環境を初期化しました" -ForegroundColor Green
}

# エイリアス
Set-Alias -Name gcp-init -Value Initialize-GCP
```

プロファイルを再読み込み：

```powershell
. $PROFILE
```

#### 方法B: システム環境変数

1. `Win + X` → `システム` → `システムの詳細設定`
2. `環境変数` をクリック
3. 以下を追加：
   - `GOOGLE_APPLICATION_CREDENTIALS`: `C:\Users\<YourUsername>\.gcp\service-account-key.json`
   - `GCP_PROJECT_ID`: `your-project-id`

### 3. gcloud認証

```powershell
gcloud auth activate-service-account --key-file=$env:GOOGLE_APPLICATION_CREDENTIALS
gcloud config set project $env:GCP_PROJECT_ID

# 認証確認
gcloud auth list
gcloud config list
```

## 自動セットアップスクリプト

### PowerShellスクリプト: `setup-dev-env.ps1`

このリポジトリの `scripts/windows/setup-dev-env.ps1` を実行：

```powershell
# スクリプトを実行可能にする（初回のみ）
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# スクリプトを実行
.\scripts\windows\setup-dev-env.ps1
```

スクリプトは以下を自動的に実行します：
- 必要なツールのインストール確認
- GCP環境の設定
- Python仮想環境の作成
- 必要なPythonパッケージのインストール

## Claude Codeの使用方法

### 基本的な使い方

```powershell
# リポジトリのクローン
git clone https://github.com/your-org/your-repo.git
cd your-repo

# Claude Codeを起動
claude

# または特定のプロンプトで起動
claude "プロジェクトの構造を説明してください"
```

### VS Codeとの統合

VS Codeから直接Claude Codeを使用する：

1. VS Codeでプロジェクトを開く
2. ターミナルを開く（`` Ctrl + ` ``）
3. `claude` コマンドを実行

## トラブルシューティング

### Claude Codeが見つからない

```powershell
# PATHを確認
$env:PATH

# npm global binのパスを追加
$env:PATH += ";$env:APPDATA\npm"
```

### gcloudコマンドが見つからない

```powershell
# gcloud SDKのパスを確認
$env:PATH += ";C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin"
```

### PowerShellスクリプトが実行できない

```powershell
# 実行ポリシーを変更
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 認証エラー

```powershell
# サービスアカウントキーのパスを確認
Test-Path $env:GOOGLE_APPLICATION_CREDENTIALS

# 認証をやり直す
gcloud auth activate-service-account --key-file=$env:GOOGLE_APPLICATION_CREDENTIALS
```

### Python仮想環境の問題

```powershell
# 仮想環境を作り直す
python -m venv venv --clear
.\venv\Scripts\Activate.ps1
pip install --upgrade pip
```

## 推奨される開発ワークフロー

1. **プロジェクト開始時**
   ```powershell
   gcp-init  # GCP環境を初期化
   ```

2. **作業開始**
   ```powershell
   cd your-project
   .\venv\Scripts\Activate.ps1  # 仮想環境を有効化（Pythonプロジェクトの場合）
   claude  # Claude Codeを起動
   ```

3. **作業終了時**
   ```powershell
   deactivate  # 仮想環境を無効化
   ```

## 参考リンク

- [Claude Code 公式ドキュメント](https://docs.anthropic.com/claude/docs/claude-code)
- [Google Cloud CLI ドキュメント](https://cloud.google.com/sdk/docs)
- [Git for Windows](https://git-scm.com/download/win)
- [Windows Package Manager (winget)](https://learn.microsoft.com/ja-jp/windows/package-manager/winget/)

## サポート

問題が発生した場合は、以下を確認してください：
1. このドキュメントのトラブルシューティングセクション
2. プロジェクトの Issues ページ
3. Claude Code の公式サポート
