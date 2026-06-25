# Pokretanje demo aplikacije (Windows)
# Usage: .\run.ps1 [config_path]
# Ako se ne navede putanja, koristi se ./demo/config.yaml

param(
    [string]$ConfigPath = ".\demo\config.yaml"
)

if (-not (Test-Path $ConfigPath)) {
    Write-Error "Config file not found: $ConfigPath"
    exit 1
}

Write-Host "Using config: $ConfigPath"
$env:DEMO_CONFIG = $ConfigPath
go run ./cmd/demo/
