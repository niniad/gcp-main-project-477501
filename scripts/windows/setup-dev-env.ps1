# Windows開発環境自動セットアップスクリプト
# PowerShell 5.1以上が必要

param(
    [switch]$SkipToolsCheck,
    [switch]$GCPOnly,
    [string]$ServiceAccountKeyPath
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Windows開発環境セットアップ" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 管理者権限チェック
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "警告: 管理者権限で実行されていません。一部の機能が制限される可能性があります。" -ForegroundColor Yellow
    Write-Host ""
}

# ツールのインストール確認
function Test-CommandExists {
    param([string]$Command)
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $false
    }
}

# 必要なツールの確認とインストールガイド
if (-not $SkipToolsCheck -and -not $GCPOnly) {
    Write-Host "[1/5] 必要なツールの確認..." -ForegroundColor Green

    $tools = @(
        @{Name="git"; DisplayName="Git"; InstallCmd="winget install Git.Git"},
        @{Name="node"; DisplayName="Node.js"; InstallCmd="winget install OpenJS.NodeJS.LTS"},
        @{Name="python"; DisplayName="Python"; InstallCmd="winget install Python.Python.3.12"},
        @{Name="gcloud"; DisplayName="Google Cloud CLI"; InstallCmd="インストーラーをダウンロード: https://cloud.google.com/sdk/docs/install"}
    )

    $missingTools = @()

    foreach ($tool in $tools) {
        if (Test-CommandExists $tool.Name) {
            Write-Host "  ✓ $($tool.DisplayName) がインストールされています" -ForegroundColor Green
        } else {
            Write-Host "  ✗ $($tool.DisplayName) が見つかりません" -ForegroundColor Red
            $missingTools += $tool
        }
    }

    if ($missingTools.Count -gt 0) {
        Write-Host ""
        Write-Host "以下のツールがインストールされていません:" -ForegroundColor Yellow
        foreach ($tool in $missingTools) {
            Write-Host "  - $($tool.DisplayName): $($tool.InstallCmd)" -ForegroundColor Yellow
        }
        Write-Host ""
        $response = Read-Host "続行しますか？ (Y/N)"
        if ($response -ne "Y" -and $response -ne "y") {
            Write-Host "セットアップを中止しました。" -ForegroundColor Red
            exit 1
        }
    }
    Write-Host ""
}

# GCP環境のセットアップ
Write-Host "[2/5] GCP環境のセットアップ..." -ForegroundColor Green

# サービスアカウントキーのパスを決定
if (-not $ServiceAccountKeyPath) {
    $defaultKeyPath = "$env:USERPROFILE\.gcp\service-account-key.json"

    if (Test-Path $defaultKeyPath) {
        $ServiceAccountKeyPath = $defaultKeyPath
        Write-Host "  既存のサービスアカウントキーを使用: $ServiceAccountKeyPath" -ForegroundColor Green
    } else {
        Write-Host "  サービスアカウントキーが見つかりません。" -ForegroundColor Yellow
        Write-Host "  GCPコンソールからサービスアカウントキー（JSON）をダウンロードしてください。" -ForegroundColor Yellow
        Write-Host ""
        $keyPath = Read-Host "サービスアカウントキーのパスを入力してください（スキップする場合はEnter）"

        if ($keyPath) {
            $ServiceAccountKeyPath = $keyPath
        } else {
            Write-Host "  GCP環境のセットアップをスキップします。" -ForegroundColor Yellow
            $ServiceAccountKeyPath = $null
        }
    }
}

if ($ServiceAccountKeyPath) {
    if (Test-Path $ServiceAccountKeyPath) {
        # 環境変数を設定
        [System.Environment]::SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", $ServiceAccountKeyPath, "User")
        $env:GOOGLE_APPLICATION_CREDENTIALS = $ServiceAccountKeyPath

        Write-Host "  ✓ GOOGLE_APPLICATION_CREDENTIALS を設定しました" -ForegroundColor Green

        # プロジェクトIDを取得
        if (Test-CommandExists "gcloud") {
            try {
                $json = Get-Content $ServiceAccountKeyPath | ConvertFrom-Json
                $projectId = $json.project_id

                if ($projectId) {
                    [System.Environment]::SetEnvironmentVariable("GCP_PROJECT_ID", $projectId, "User")
                    $env:GCP_PROJECT_ID = $projectId
                    Write-Host "  ✓ GCP_PROJECT_ID を設定しました: $projectId" -ForegroundColor Green

                    # gcloud認証
                    Write-Host "  gcloud認証を実行中..." -ForegroundColor Cyan
                    gcloud auth activate-service-account --key-file=$ServiceAccountKeyPath 2>$null
                    gcloud config set project $projectId 2>$null
                    Write-Host "  ✓ gcloud認証が完了しました" -ForegroundColor Green
                }
            } catch {
                Write-Host "  警告: サービスアカウントキーの読み込みに失敗しました" -ForegroundColor Yellow
            }
        }
    } else {
        Write-Host "  エラー: サービスアカウントキーが見つかりません: $ServiceAccountKeyPath" -ForegroundColor Red
    }
}
Write-Host ""

# Python環境のセットアップ
if (-not $GCPOnly -and (Test-CommandExists "python")) {
    Write-Host "[3/5] Python環境のセットアップ..." -ForegroundColor Green

    # 仮想環境の作成
    if (-not (Test-Path "venv")) {
        Write-Host "  Python仮想環境を作成中..." -ForegroundColor Cyan
        python -m venv venv
        Write-Host "  ✓ 仮想環境を作成しました" -ForegroundColor Green
    } else {
        Write-Host "  ✓ 既存の仮想環境を使用します" -ForegroundColor Green
    }

    # 仮想環境を有効化
    & ".\venv\Scripts\Activate.ps1"

    # pipのアップグレード
    Write-Host "  pipをアップグレード中..." -ForegroundColor Cyan
    python -m pip install --upgrade pip --quiet

    # GCP Pythonライブラリのインストール
    if (Test-Path "requirements.txt") {
        Write-Host "  requirements.txtからパッケージをインストール中..." -ForegroundColor Cyan
        pip install -r requirements.txt --quiet
    } else {
        Write-Host "  基本的なGCPライブラリをインストール中..." -ForegroundColor Cyan
        pip install --quiet `
            google-cloud-storage `
            google-cloud-bigquery `
            google-cloud-secret-manager `
            google-cloud-functions `
            google-cloud-scheduler `
            google-cloud-logging
    }
    Write-Host "  ✓ Pythonパッケージのインストールが完了しました" -ForegroundColor Green
    Write-Host ""
} elseif (-not $GCPOnly) {
    Write-Host "[3/5] Python環境のセットアップ... スキップ（Pythonが見つかりません）" -ForegroundColor Yellow
    Write-Host ""
}

# PowerShellプロファイルの設定
Write-Host "[4/5] PowerShellプロファイルの設定..." -ForegroundColor Green

if (-not (Test-Path $PROFILE)) {
    Write-Host "  PowerShellプロファイルを作成中..." -ForegroundColor Cyan
    New-Item -ItemType File -Path $PROFILE -Force | Out-Null
}

$profileContent = Get-Content $PROFILE -Raw -ErrorAction SilentlyContinue

# GCP初期化関数を追加
$gcpInitFunction = @'

# GCP環境初期化関数
function Initialize-GCP {
    if ($env:GOOGLE_APPLICATION_CREDENTIALS -and (Test-Path $env:GOOGLE_APPLICATION_CREDENTIALS)) {
        gcloud auth activate-service-account --key-file=$env:GOOGLE_APPLICATION_CREDENTIALS 2>$null
        if ($env:GCP_PROJECT_ID) {
            gcloud config set project $env:GCP_PROJECT_ID 2>$null
        }
        Write-Host "GCP環境を初期化しました" -ForegroundColor Green
    } else {
        Write-Host "警告: GOOGLE_APPLICATION_CREDENTIALS が設定されていません" -ForegroundColor Yellow
    }
}

# エイリアス
Set-Alias -Name gcp-init -Value Initialize-GCP
'@

if (-not $profileContent.Contains("Initialize-GCP")) {
    Add-Content -Path $PROFILE -Value $gcpInitFunction
    Write-Host "  ✓ PowerShellプロファイルにGCP関数を追加しました" -ForegroundColor Green
} else {
    Write-Host "  ✓ PowerShellプロファイルは既に設定されています" -ForegroundColor Green
}
Write-Host ""

# セットアップ完了
Write-Host "[5/5] セットアップ完了！" -ForegroundColor Green
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  セットアップが完了しました" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 環境情報の表示
Write-Host "環境情報:" -ForegroundColor Cyan
if ($env:GCP_PROJECT_ID) {
    Write-Host "  プロジェクトID: $env:GCP_PROJECT_ID" -ForegroundColor White
}
if ($env:GOOGLE_APPLICATION_CREDENTIALS) {
    Write-Host "  サービスアカウントキー: $env:GOOGLE_APPLICATION_CREDENTIALS" -ForegroundColor White
}
Write-Host ""

Write-Host "次のステップ:" -ForegroundColor Cyan
Write-Host "  1. 新しいPowerShellウィンドウを開いて環境変数を反映してください" -ForegroundColor White
Write-Host "  2. 'gcp-init' コマンドでGCP環境を初期化できます" -ForegroundColor White
if ((Test-Path "venv")) {
    Write-Host "  3. '.\venv\Scripts\Activate.ps1' でPython仮想環境を有効化できます" -ForegroundColor White
}
Write-Host ""

Write-Host "詳細なドキュメント: docs/windows-setup-guide.md" -ForegroundColor Gray
Write-Host ""
