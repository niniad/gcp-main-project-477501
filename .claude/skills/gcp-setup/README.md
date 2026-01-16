# GCP Environment Setup Skill

Claude Code on the Web で GCP 開発環境を自動的にセットアップするskillです。

## 機能

- gcloud CLI の自動インストール
- Python GCP ライブラリの自動インストール
- サービスアカウント認証の設定
- プロジェクト設定

## 使い方

### 1. Claude Code on the Web の環境設定

環境変数を設定します（Cloud environment設定画面）：

```env
GOOGLE_APPLICATION_CREDENTIALS_JSON='{"type":"service_account","project_id":"your-project-id",...}'
GCP_PROJECT_ID=your-project-id
```

### 2. スクリプトをプロジェクトにコピー

```bash
# .claude/skills ディレクトリを作成
mkdir -p .claude/skills

# このskillをコピー
cp -r /path/to/gcp-setup .claude/skills/
```

### 3. SessionStart hookとして設定（推奨）

`.claude/settings.json` を作成：

```json
{
  "hooks": {
    "SessionStart": [
      {
        "name": "GCP Environment Setup",
        "command": "bash .claude/skills/gcp-setup/setup.sh"
      }
    ]
  }
}
```

### 4. 手動実行

```bash
bash .claude/skills/gcp-setup/setup.sh
```

## 環境変数

### 必須

- `GOOGLE_APPLICATION_CREDENTIALS_JSON`: サービスアカウントキー（JSON形式）

### オプション

- `GCP_PROJECT_ID`: GCPプロジェクトID（デフォルト: 未設定）
- `GCP_KEY_PATH`: サービスアカウントキーの保存先（デフォルト: `/root/.config/gcloud/service-account-key.json`）
- `GCP_PYTHON_LIBS`: インストールするPythonライブラリ（デフォルト: 標準的なGCPライブラリ）

## カスタマイズ例

### 特定のライブラリのみインストール

```env
GCP_PYTHON_LIBS="google-cloud-storage google-cloud-bigquery"
```

### カスタムキーパス

```env
GCP_KEY_PATH=/custom/path/to/key.json
```

## 他のプロジェクトでの使用

このskillは汎用的に設計されているため、任意のGCPプロジェクトで使用できます：

1. `.claude/skills/gcp-setup/` ディレクトリを新しいプロジェクトにコピー
2. 環境変数 `GOOGLE_APPLICATION_CREDENTIALS_JSON` と `GCP_PROJECT_ID` を設定
3. スクリプトを実行またはhookとして設定

## トラブルシューティング

### 認証エラー

- 環境変数 `GOOGLE_APPLICATION_CREDENTIALS_JSON` が正しく設定されているか確認
- JSONの形式が正しいか確認（シングルクォートで囲む）

### プロジェクトが設定されない

- 環境変数 `GCP_PROJECT_ID` を設定してください

### PATH エラー

スクリプトは自動的にPATHを設定しますが、セッション再起動が必要な場合があります。
