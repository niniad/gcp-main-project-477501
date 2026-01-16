#!/bin/bash
# GCP開発環境セットアップスクリプト（汎用版）
# 他のプロジェクトでも再利用可能

set -e

# PATHを確実に設定
export PATH="/usr/bin:/bin:/usr/local/bin:/opt/google-cloud-sdk/bin:$PATH"

echo "=== GCP開発環境セットアップ ==="

# gcloud CLIがインストールされているか確認
if ! command -v gcloud &> /dev/null; then
    echo "gcloud CLIをインストール中..."
    # 既存のディレクトリがある場合は削除
    if [ -d "/opt/google-cloud-sdk" ]; then
        rm -rf /opt/google-cloud-sdk
    fi
    curl -sSL https://sdk.cloud.google.com > /tmp/install_gcloud.sh
    bash /tmp/install_gcloud.sh --disable-prompts --install-dir=/opt
    export PATH="/opt/google-cloud-sdk/bin:$PATH"
else
    echo "gcloud CLIは既にインストールされています"
fi

# PATHを.bashrcに追加
if ! grep -q 'google-cloud-sdk/bin' ~/.bashrc 2>/dev/null; then
    echo 'export PATH="/opt/google-cloud-sdk/bin:$PATH"' >> ~/.bashrc
fi

# Python ライブラリをインストール（GCP_PYTHON_LIBSが指定されていればそれを使用）
if [ -n "$GCP_PYTHON_LIBS" ]; then
    echo "Python GCPライブラリをインストール中..."
    pip3 install --quiet --ignore-installed $GCP_PYTHON_LIBS 2>/dev/null || true
else
    echo "Python GCPライブラリをインストール中（デフォルト）..."
    pip3 install --quiet --ignore-installed \
        google-cloud-storage \
        google-cloud-bigquery \
        google-cloud-secret-manager \
        google-cloud-functions \
        google-cloud-scheduler \
        google-cloud-logging \
        cffi \
        cryptography 2>/dev/null || true
fi

# サービスアカウントキーの設定
KEY_PATH="${GCP_KEY_PATH:-/root/.config/gcloud/service-account-key.json}"
mkdir -p "$(dirname "$KEY_PATH")"

# 環境変数からJSONを読み取る
if [ -n "$GOOGLE_APPLICATION_CREDENTIALS_JSON" ]; then
    echo "環境変数からサービスアカウントキーを設定中..."
    # シングルクォートを削除してJSONを書き込む
    echo "$GOOGLE_APPLICATION_CREDENTIALS_JSON" | sed "s/^'//; s/'$//" > "$KEY_PATH"
    chmod 600 "$KEY_PATH"
    echo "✓ 環境変数からキーを設定しました"
elif [ -f "$KEY_PATH" ]; then
    echo "既存のサービスアカウントキーを使用します: $KEY_PATH"
else
    echo "エラー: サービスアカウントキーが見つかりません"
    echo ""
    echo "環境変数 GOOGLE_APPLICATION_CREDENTIALS_JSON にサービスアカウントキー（JSON）を設定してください"
    exit 1
fi

# 環境変数を設定
export GOOGLE_APPLICATION_CREDENTIALS="$KEY_PATH"
if ! grep -q 'GOOGLE_APPLICATION_CREDENTIALS' ~/.bashrc 2>/dev/null; then
    echo "export GOOGLE_APPLICATION_CREDENTIALS=\"$KEY_PATH\"" >> ~/.bashrc
fi

# gcloud認証
echo "gcloud 認証を実行中..."
if gcloud auth activate-service-account --key-file="$KEY_PATH" 2>&1 | grep -q "Activated service account"; then
    echo "✓ 認証成功"
else
    echo "警告: 認証に問題が発生した可能性があります"
fi

# プロジェクトを設定（GCP_PROJECT_IDが指定されていればそれを使用）
if [ -n "$GCP_PROJECT_ID" ]; then
    PROJECT_ID="$GCP_PROJECT_ID"
    gcloud config set project "$PROJECT_ID" 2>/dev/null
    echo "✓ プロジェクトを設定しました: $PROJECT_ID"
else
    echo "注意: GCP_PROJECT_ID が設定されていません。プロジェクトは設定されていません。"
fi

# 認証情報を確認
ACCOUNT=$(gcloud config get-value account 2>/dev/null)
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)

echo ""
echo "=== セットアップ完了 ==="
echo "プロジェクト: ${CURRENT_PROJECT:-未設定}"
echo "サービスアカウント: $ACCOUNT"
echo ""
echo "利用可能なサービス:"
echo "  - gcloud CLI"
echo "  - gsutil (Cloud Storage)"
echo "  - bq (BigQuery)"
echo "  - Python GCP SDKs"
echo ""
