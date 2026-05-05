# Requires PowerShell 5.1 or newer

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

# --- 2. Compile everything ---
Write-Host "Compiling Drev CI Ecosystem..." -ForegroundColor Cyan
go build -o bin/drevd.exe ./cmd/drevd
if ($LASTEXITCODE -ne 0) { Write-Host "CRITICAL: Backend build failed! Fixing code now..." -ForegroundColor Red; exit 1 }
go build -o bin/drev.exe ./cmd/drev
go build -o bin/drev-router.exe ./cmd/drev-router

# --- 3. Launch everything ---
Write-Host "Launching Drev CI Ecosystem..." -ForegroundColor Green

# Start Backend (9090)
Write-Host "  > Starting Backend..." -ForegroundColor Gray
Start-Process -NoNewWindow -FilePath ".\bin\drevd.exe"

# Start Router (8888)
Write-Host "  > Starting Router..." -ForegroundColor Gray
Start-Process -NoNewWindow -FilePath ".\bin\drev-router.exe"

# Start ngrok (with your permanent domain)
Write-Host "  > Starting ngrok Tunnel..." -ForegroundColor Gray
Start-Process -NoNewWindow -FilePath "ngrok" -ArgumentList "http --domain=picked-indirectly-cheetah.ngrok-free.app 8888"

# Start Dashboard (3000)
Write-Host "  > Starting Dashboard..." -ForegroundColor Gray
Start-Process -NoNewWindow -FilePath "npm.cmd" -ArgumentList "run dev" -WorkingDirectory ".\dashboard"

Write-Host "`nAll systems are GO!" -ForegroundColor Green
Write-Host "--------------------------------------------------"
Write-Host "Dashboard: http://localhost:3000" -ForegroundColor Cyan
Write-Host "Public URL: https://picked-indirectly-cheetah.ngrok-free.app" -ForegroundColor Cyan
Write-Host "--------------------------------------------------"
Write-Host "To stop everything, simply close this terminal or run: Stop-Process -Name drevd, drev-router, ngrok" -ForegroundColor Yellow
