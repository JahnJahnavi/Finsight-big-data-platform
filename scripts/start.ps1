<#
.SYNOPSIS
  FinSight - start the local infrastructure stack (Windows / PowerShell).

.EXAMPLE
  .\scripts\start.ps1              # start everything in COMPOSE_PROFILES (.env)
  .\scripts\start.ps1 -Min         # core only: kafka, HDFS, mongodb, neo4j
  .\scripts\start.ps1 -Build       # (re)build custom images first
  .\scripts\start.ps1 -Profiles hive,spark
#>
[CmdletBinding()]
param(
  [switch]$Min,
  [switch]$Build,
  [string[]]$Profiles
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not (Test-Path ".env")) {
  Write-Host "[finsight] .env not found - creating it from .env.example."
  Write-Host "[finsight] >>> Edit .env and replace every CHANGE_ME value before continuing. <<<"
  Copy-Item ".env.example" ".env"
  exit 1
}

$buildArg = @()
if ($Build) { $buildArg = @("--build") }

if ($Min) {
  Write-Host "[finsight] starting CORE services only (no profiles)."
  $env:COMPOSE_PROFILES = ""
  docker compose up -d --remove-orphans @buildArg kafka namenode datanode mongodb neo4j
}
elseif ($Profiles) {
  $profileArgs = @()
  foreach ($p in $Profiles) { $profileArgs += @("--profile", $p) }
  Write-Host "[finsight] starting core + profiles: $($Profiles -join ', ')"
  $env:COMPOSE_PROFILES = ""
  docker compose @profileArgs up -d --remove-orphans @buildArg
}
else {
  Write-Host "[finsight] starting all services in COMPOSE_PROFILES from .env"
  docker compose up -d --remove-orphans @buildArg
}

Write-Host ""
docker compose ps
Write-Host ""
Write-Host "[finsight] services need 1-3 minutes to become healthy on first run."
Write-Host "[finsight] verify with:  python scripts\healthcheck.py"
