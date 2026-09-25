<#
.SYNOPSIS
    Equivalente en PowerShell del Makefile (para Windows sin `make`).

.EXAMPLE
    .\scripts\tasks.ps1 up
    .\scripts\tasks.ps1 test-integration
    .\scripts\tasks.ps1 demo
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("help", "up", "down", "logs", "ps", "migrate", "seed", "test-unit",
                 "test-unit-docker", "test-integration", "lint", "demo", "clean")]
    [string]$Task = "help"
)

$ErrorActionPreference = "Stop"
# Raíz del repositorio (carpeta padre de scripts/).
Set-Location (Split-Path -Parent $PSScriptRoot)

$Projects = @("shared", "orders-service", "processor-service", "notifier-service", "cleanup-job")
$Services = @("orders-service", "processor-service", "notifier-service", "cleanup-job")
$ApiKey = if ($env:API_KEY) { $env:API_KEY } else { "cafe-cloud-dev-key" }

# Ejecuta un comando nativo y aborta si devuelve un código de error.
function Invoke-Checked([scriptblock]$Command) {
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "Command failed with exit code $LASTEXITCODE" }
}

switch ($Task) {
    "help" { Get-Help $PSCommandPath -Detailed }
    "up" { Invoke-Checked { docker compose up --build -d } }
    "down" { Invoke-Checked { docker compose down } }
    "logs" { docker compose logs -f @Services }
    "ps" { docker compose ps }
    "migrate" { Invoke-Checked { docker compose run --rm migrate alembic upgrade head } }
    "seed" { Invoke-Checked { docker compose run --rm migrate python -m app.seed } }
    "test-unit" {
        foreach ($p in $Projects) {
            Write-Host "== $p"
            Push-Location $p
            try { Invoke-Checked { poetry run pytest -q } } finally { Pop-Location }
        }
    }
    "test-unit-docker" {
        foreach ($s in $Services) {
            Write-Host "== $s"
            Invoke-Checked { docker build -q -f "$s/Dockerfile" --target test -t "cafe-cloud/${s}:test" . }
            Invoke-Checked { docker run --rm "cafe-cloud/${s}:test" }
        }
    }
    "test-integration" { Invoke-Checked { docker compose --profile test run --rm --build integration-tests } }
    "lint" {
        foreach ($p in $Projects) {
            Push-Location $p
            try { Invoke-Checked { poetry run ruff check . } } finally { Pop-Location }
        }
    }
    "demo" {
        $headers = @{ "X-API-Key" = $ApiKey; "Idempotency-Key" = [guid]::NewGuid().ToString() }
        $body = '{"customer_id":"abc123","items":[{"name":"latte","qty":1},{"name":"muffin","qty":2}]}'
        Write-Host "1) POST /orders"
        Invoke-RestMethod -Method Post -Uri "http://localhost:8001/orders" -Headers $headers `
            -ContentType "application/json" -Body $body | ConvertTo-Json
        Write-Host "2) Esperando la preparacion (2-5 s)..."; Start-Sleep -Seconds 7
        Write-Host "3) GET /notifications/abc123"
        Invoke-RestMethod -Uri "http://localhost:8003/notifications/abc123" `
            -Headers @{ "X-API-Key" = $ApiKey } | ConvertTo-Json -Depth 5
    }
    "clean" { Invoke-Checked { docker compose --profile test down -v --rmi local } }
}
