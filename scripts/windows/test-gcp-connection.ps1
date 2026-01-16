# GCP接続テストスクリプト

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  GCP接続テスト" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 環境変数チェック
Write-Host "[1/4] 環境変数の確認..." -ForegroundColor Green
$allGood = $true

if ($env:GOOGLE_APPLICATION_CREDENTIALS) {
    Write-Host "  ✓ GOOGLE_APPLICATION_CREDENTIALS: $env:GOOGLE_APPLICATION_CREDENTIALS" -ForegroundColor Green

    if (Test-Path $env:GOOGLE_APPLICATION_CREDENTIALS) {
        Write-Host "  ✓ サービスアカウントキーファイルが存在します" -ForegroundColor Green
    } else {
        Write-Host "  ✗ サービスアカウントキーファイルが見つかりません" -ForegroundColor Red
        $allGood = $false
    }
} else {
    Write-Host "  ✗ GOOGLE_APPLICATION_CREDENTIALS が設定されていません" -ForegroundColor Red
    $allGood = $false
}

if ($env:GCP_PROJECT_ID) {
    Write-Host "  ✓ GCP_PROJECT_ID: $env:GCP_PROJECT_ID" -ForegroundColor Green
} else {
    Write-Host "  ⚠ GCP_PROJECT_ID が設定されていません（オプション）" -ForegroundColor Yellow
}
Write-Host ""

# gcloudコマンドの確認
Write-Host "[2/4] gcloud CLIの確認..." -ForegroundColor Green
try {
    $gcloudVersion = gcloud --version 2>&1 | Select-Object -First 1
    Write-Host "  ✓ gcloud CLI: $gcloudVersion" -ForegroundColor Green
} catch {
    Write-Host "  ✗ gcloud CLI が見つかりません" -ForegroundColor Red
    $allGood = $false
}
Write-Host ""

# 認証状態の確認
Write-Host "[3/4] 認証状態の確認..." -ForegroundColor Green
try {
    $accounts = gcloud auth list --format="value(account)" 2>&1
    if ($accounts) {
        Write-Host "  ✓ 認証済みアカウント:" -ForegroundColor Green
        $accounts | ForEach-Object { Write-Host "    - $_" -ForegroundColor White }
    } else {
        Write-Host "  ✗ 認証済みアカウントがありません" -ForegroundColor Red
        $allGood = $false
    }
} catch {
    Write-Host "  ✗ 認証状態の確認に失敗しました" -ForegroundColor Red
    $allGood = $false
}

try {
    $currentProject = gcloud config get-value project 2>&1
    if ($currentProject -and $currentProject -ne "") {
        Write-Host "  ✓ 現在のプロジェクト: $currentProject" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ プロジェクトが設定されていません" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ⚠ プロジェクトの確認に失敗しました" -ForegroundColor Yellow
}
Write-Host ""

# GCP APIへの接続テスト
Write-Host "[4/4] GCP APIへの接続テスト..." -ForegroundColor Green
try {
    if ($env:GCP_PROJECT_ID) {
        $projectInfo = gcloud projects describe $env:GCP_PROJECT_ID --format="value(projectId,name)" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  ✓ プロジェクト情報の取得に成功しました" -ForegroundColor Green
            Write-Host "    $projectInfo" -ForegroundColor White
        } else {
            Write-Host "  ✗ プロジェクト情報の取得に失敗しました" -ForegroundColor Red
            $allGood = $false
        }
    } else {
        Write-Host "  ⊘ GCP_PROJECT_ID が設定されていないためスキップ" -ForegroundColor Gray
    }
} catch {
    Write-Host "  ✗ GCP APIへの接続に失敗しました" -ForegroundColor Red
    Write-Host "    $_" -ForegroundColor Red
    $allGood = $false
}
Write-Host ""

# 結果表示
Write-Host "========================================" -ForegroundColor Cyan
if ($allGood) {
    Write-Host "  テスト結果: 成功 ✓" -ForegroundColor Green
    Write-Host "  GCP環境は正しく設定されています" -ForegroundColor Green
} else {
    Write-Host "  テスト結果: 失敗 ✗" -ForegroundColor Red
    Write-Host "  上記のエラーを確認してください" -ForegroundColor Red
    Write-Host ""
    Write-Host "  トラブルシューティング:" -ForegroundColor Yellow
    Write-Host "    1. .\scripts\windows\setup-dev-env.ps1 を実行して環境を再設定" -ForegroundColor White
    Write-Host "    2. gcp-init コマンドでGCP環境を初期化" -ForegroundColor White
    Write-Host "    3. docs/windows-setup-guide.md を参照" -ForegroundColor White
}
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

exit $(if ($allGood) { 0 } else { 1 })
