# Copyright 2026 Andrea Provenzali and AProDB contributors
# SPDX-License-Identifier: AGPL-3.0-only

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $DataDir,
    [string] $SshTarget = 'ex44',
    [string] $Container = 'emeroteca',
    [string] $SourceDatabase = 'emeroteca',
    [string] $Schema = '*',
    [string] $Table = '*',
    [ValidateRange(0, [long]::MaxValue)]
    [long] $RowLimit = 0,
    [string] $Tenant = 'commit',
    [string] $Namespace = 'emeroteca',
    [ValidateSet('durable', 'relaxed')]
    [string] $Durability = 'durable',
    [ValidateRange(1, 1024)]
    [int] $BatchOperations = 1024,
    [ValidateRange(1, [long]::MaxValue)]
    [long] $ProgressEvery = 100000,
    [ValidateRange(1, [long]::MaxValue)]
    [long] $MaxDataBytes = 68719476736,
    [ValidateRange(1, [long]::MaxValue)]
    [long] $MinFreeDiskBytes = 8589934592,
    [ValidateRange(1, [long]::MaxValue)]
    [long] $MaxCompactionTemporaryBytes = 17179869184
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-SafeToken([string] $Name, [string] $Value, [string] $Pattern) {
    if ($Value -notmatch $Pattern) {
        throw "$Name contains unsupported characters"
    }
}

Assert-SafeToken 'SSH target' $SshTarget '^[A-Za-z0-9_.@-]+$'
Assert-SafeToken 'container' $Container '^[A-Za-z0-9_.-]+$'
Assert-SafeToken 'database' $SourceDatabase '^[A-Za-z_][A-Za-z0-9_$-]*$'
Assert-SafeToken 'schema' $Schema '^(?:\*|[A-Za-z_][A-Za-z0-9_$]*)$'
Assert-SafeToken 'table' $Table '^(?:\*|[A-Za-z_][A-Za-z0-9_$]*)$'

$repository = Split-Path -Parent $PSScriptRoot
$exportScript = Join-Path $PSScriptRoot 'postgres_export_jsonl.sql'
$releaseImporter = Join-Path $repository 'target\release\aprodb-pg-import.exe'
$debugImporter = Join-Path $repository 'target\debug\aprodb-pg-import.exe'
$importer = if (Test-Path -LiteralPath $releaseImporter -PathType Leaf) {
    $releaseImporter
} else {
    $debugImporter
}
if (-not (Test-Path -LiteralPath $exportScript -PathType Leaf)) {
    throw "export script not found: $exportScript"
}
if (-not (Test-Path -LiteralPath $importer -PathType Leaf)) {
    throw "importer not built: $importer"
}
if (Test-Path -LiteralPath $DataDir) {
    throw "destination already exists: $DataDir"
}

$remote = "docker exec -i $Container sh -lc 'export PGPASSWORD=`"`$POSTGRES_PASSWORD`"; exec psql -h 127.0.0.1 -X -qAt -v ON_ERROR_STOP=1 -v schema_name=$Schema -v table_name=$Table -v row_limit=$RowLimit -U `"`$POSTGRES_USER`" -d $SourceDatabase'"
$importArguments = @(
    '--data-dir', $DataDir,
    '--tenant', $Tenant,
    '--namespace', $Namespace,
    '--durability', $Durability,
    '--batch-operations', $BatchOperations,
    '--progress-every', $ProgressEvery,
    '--max-data-bytes', $MaxDataBytes,
    '--min-free-disk-bytes', $MinFreeDiskBytes,
    '--max-compaction-temporary-bytes', $MaxCompactionTemporaryBytes
)

Get-Content -Raw -LiteralPath $exportScript |
    & ssh.exe -o BatchMode=yes -o StrictHostKeyChecking=yes $SshTarget $remote |
    & $importer @importArguments
if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL import failed with exit code $LASTEXITCODE"
}
