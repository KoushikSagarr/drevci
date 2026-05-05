# Requires PowerShell 5.1 or newer

Write-Host "Compiling Drev CI..." -ForegroundColor Cyan
go build -o bin/drevd.exe ./cmd/drevd
go build -o bin/drev.exe ./cmd/drev

Write-Host "Starting Drev CI Daemon..." -ForegroundColor Green
Start-Process -NoNewWindow -FilePath ".\bin\drevd.exe"

Write-Host ""
Write-Host "Drev CI Server is running!" -ForegroundColor Yellow
Write-Host "To view processes: Get-Process drevd"
Write-Host "To stop the server: Stop-Process -Name drevd"
Write-Host ""
Write-Host "You can now run commands natively, for example:" -ForegroundColor Cyan
Write-Host ".\bin\drev.exe run configs\example.drev.yml"
