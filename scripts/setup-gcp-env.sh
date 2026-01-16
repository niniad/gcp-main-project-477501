#!/bin/bash
# GCP開発環境セットアップスクリプト
# Web版Claude Codeのセッション開始時に実行

set -e

echo "=== GCP開発環境セットアップ ==="

# gcloud CLIがインストールされているか確認
if ! command -v gcloud &> /dev/null; then
    echo "gcloud CLIをインストール中..."
    curl -sSL https://sdk.cloud.google.com > /tmp/install_gcloud.sh
    bash /tmp/install_gcloud.sh --disable-prompts --install-dir=/opt
fi

# PATHを設定
export PATH="/opt/google-cloud-sdk/bin:$PATH"
echo 'export PATH="/opt/google-cloud-sdk/bin:$PATH"' >> ~/.bashrc

# Python ライブラリをインストール
echo "Python GCPライブラリをインストール中..."
pip3 install --quiet --ignore-installed \
    google-cloud-storage \
    google-cloud-bigquery \
    google-cloud-secret-manager \
    google-cloud-functions \
    google-cloud-scheduler \
    google-cloud-logging \
    cffi \
    cryptography 2>/dev/null || true

# サービスアカウントキーの確認
KEY_PATH="/root/.config/gcloud/service-account-key.json"
if [ ! -f "$KEY_PATH" ]; then
    echo "警告: サービスアカウントキーが見つかりません: $KEY_PATH"
    echo "キーを設定してください"
    exit 1
fi

# 環境変数を設定
export GOOGLE_APPLICATION_CREDENTIALS="$KEY_PATH"
echo "export GOOGLE_APPLICATION_CREDENTIALS=\"$KEY_PATH\"" >> ~/.bashrc

# gcloud認証
gcloud auth activate-service-account --key-file="$KEY_PATH" 2>/dev/null
gcloud config set project main-project-477501 2>/dev/null

echo ""
echo "=== セットアップ完了 ==="
echo "プロジェクト: main-project-477501"
echo "サービスアカウント: claude-code-dev@main-project-477501.iam.gserviceaccount.com"
echo ""
echo "利用可能なサービス:"
echo "  - GCS (gsutil / Python SDK)"
echo "  - BigQuery (bq / Python SDK)"
echo "  - Secret Manager (gcloud secrets)"
echo "  - Cloud Functions (gcloud functions)"
echo "  - Cloud Scheduler (gcloud scheduler)"
