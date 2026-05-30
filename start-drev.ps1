# Requires PowerShell 5.1 or newer
# Usage:
#   .\start-drev.ps1              # local SQLite mode (default)
#   .\start-drev.ps1 -Mode saas   # PostgreSQL + Supabase mode

param([string]$Mode = "local")

# --- 1. Cleanup old processes ---
Write-Host "Cleaning up port 3000 (Next.js)..." -ForegroundColor Yellow
$pid3000 = (netstat -ano | findstr :3000 | ForEach-Object { $_.Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries)[-1] } | Select-Object -Unique)
if ($pid3000) {
    Write-Host "Killing process $pid3000 on port 3000..." -ForegroundColor Yellow
    Stop-Process -Id $pid3000 -Force -ErrorAction SilentlyContinue
}

$processes = @("drevd", "drev-router", "ngrok")
foreach ($p in $processes) {
    $existing = Get-Process $p -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "Stopping existing $p..." -ForegroundColor Yellow
        Stop-Process -Name $p -Force
        Start-Sleep -Seconds 1
    }
}

# --- 2. Build ---
$env:DREV_LOCAL_REPO = $PSScriptRoot
Write-Host "Compiling Drev CI Ecosystem (mode: $Mode)..." -ForegroundColor Cyan

go build -o bin/drevd.exe ./cmd/drevd
if ($LASTEXITCODE -ne 0) { Write-Host "CRITICAL: drevd build failed!" -ForegroundColor Red; exit 1 }

go build -o bin/drev.exe ./cmd/drev
if ($LASTEXITCODE -ne 0) { Write-Host "CRITICAL: drev build failed!" -ForegroundColor Red; exit 1 }

go build -o bin/drev-router.exe ./cmd/drev-router
if ($LASTEXITCODE -ne 0) { Write-Host "CRITICAL: drev-router build failed!" -ForegroundColor Red; exit 1 }

# --- 3. Launch everything ---
Write-Host "Launching Drev CI Ecosystem..." -ForegroundColor Green

if ($Mode -eq "saas") {
    # ── SaaS / Postgres mode ──────────────────────────────────────────
    Write-Host "  > Mode: SaaS (PostgreSQL / Supabase)" -ForegroundColor Magenta

    # Load .env file and export variables
    if (Test-Path ".env") {
        Write-Host "  > Loading .env..." -ForegroundColor Gray
        Get-Content ".env" | Where-Object { $_ -match "^\s*[^#]" -and $_ -match "=" } | ForEach-Object {
            $parts = $_ -split "=", 2
            $key   = $parts[0].Trim()
            $value = $parts[1].Trim()
            [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
            Set-Item -Path "env:$key" -Value $value
        }
    } else {
        Write-Host "WARNING: .env file not found. DREV_DB_URL must be set manually." -ForegroundColor Red
    }

    if (-not $env:DREV_DB_URL) {
        Write-Host "CRITICAL: DREV_DB_URL is not set. Add it to .env or set it manually." -ForegroundColor Red
        exit 1
    }

    Write-Host "  > Starting Backend (PostgreSQL)..." -ForegroundColor Gray
    Start-Process -NoNewWindow -FilePath ".\bin\drevd.exe" -ArgumentList `
        "--db-type", "postgres", `
        "--db-url", $env:DREV_DB_URL, `
        "--port", "9090"

    Write-Host ""
    Write-Host "  Database: PostgreSQL (Supabase)" -ForegroundColor Magenta
    Write-Host "  Project:  okgoarstmlrxcwtfotbp.supabase.co" -ForegroundColor Magenta

} else {
    # ── Local / SQLite mode ───────────────────────────────────────────
    Write-Host "  > Mode: Local (SQLite)" -ForegroundColor Cyan

    Write-Host "  > Starting Backend (SQLite)..." -ForegroundColor Gray
    Start-Process -NoNewWindow -FilePath ".\bin\drevd.exe"
}

# Start Router (8888) — same for both modes
Write-Host "  > Starting Router..." -ForegroundColor Gray
Start-Process -NoNewWindow -FilePath ".\bin\drev-router.exe"

# Start ngrok (with your permanent domain)
Write-Host "  > Starting ngrok Tunnel..." -ForegroundColor Gray
Start-Process -NoNewWindow -FilePath "ngrok" -ArgumentList "http --domain=picked-indirectly-cheetah.ngrok-free.app 8888"

# Start Dashboard (3000) — same for both modes
Write-Host "  > Starting Dashboard..." -ForegroundColor Gray
Start-Process -NoNewWindow -FilePath "npm.cmd" -ArgumentList "run dev" -WorkingDirectory ".\dashboard"

Write-Host ""
Write-Host "All systems are GO! (mode: $Mode)" -ForegroundColor Green
Write-Host "--------------------------------------------------"
Write-Host "Dashboard:  http://localhost:3000"        -ForegroundColor Cyan
Write-Host "Backend:    http://localhost:9090"        -ForegroundColor Cyan
Write-Host "Public URL: https://picked-indirectly-cheetah.ngrok-free.app" -ForegroundColor Cyan
Write-Host "--------------------------------------------------"
Write-Host "To stop: Stop-Process -Name drevd, drev-router, ngrok" -ForegroundColor Yellow
