<#
.SYNOPSIS
  FinSight - stop the local infrastructure stack (Windows / PowerShell).

.EXAMPLE
  .\scripts\stop.ps1            # stop + remove containers (data volumes kept)
  .\scripts\stop.ps1 -Wipe     # ALSO delete all named volumes (destroys data)
#>
[CmdletBinding()]
param([switch]$Wipe)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$allProfiles = @("--profile","tools","--profile","connect","--profile","hive","--profile","spark")

if ($Wipe) {
  Write-Host "[finsight] stopping stack and DELETING ALL VOLUMES (HDFS, Mongo, Neo4j, Kafka, Hive metastore)..."
  $confirm = Read-Host "Type 'wipe' to confirm"
  if ($confirm -ne "wipe") { Write-Host "aborted."; exit 1 }
  docker compose @allProfiles down -v --remove-orphans
}
else {
  Write-Host "[finsight] stopping stack (data volumes preserved)..."
  docker compose @allProfiles down --remove-orphans
}

Write-Host "[finsight] done."
