[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [uri] $BaseUri,

    [string] $ApiKeyEnvironmentVariable = 'CHATAPP_OPS_API_KEY',

    [switch] $AllowUnauthenticated,

    [switch] $AllowInsecureHttp,

    [string] $OutputPath,

    [ValidateRange(1, 100)]
    [int] $PageSize = 50,

    [ValidateRange(2, 10)]
    [int] $RequiredCleanPasses = 2,

    [ValidateRange(2, 10)]
    [int] $MinimumRebuilderStablePasses = 2,

    [ValidateRange(1, 10000)]
    [int] $MaxPagesPerPass = 10000,

    [ValidateRange(1, 300)]
    [int] $TimeoutSeconds = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Import-Module (Join-Path $PSScriptRoot 'RelationshipProjectionReconcile.psm1') -Force

$apiKey = [Environment]::GetEnvironmentVariable($ApiKeyEnvironmentVariable)
if ([string]::IsNullOrWhiteSpace($apiKey) -and -not $AllowUnauthenticated) {
    throw "Ops API key environment variable '$ApiKeyEnvironmentVariable' is empty."
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $stamp = [DateTimeOffset]::UtcNow.ToString('yyyyMMddTHHmmssZ')
    $OutputPath = Join-Path `
        (Split-Path $PSScriptRoot -Parent) `
        ".artifacts/relationship-projection-reconcile/$stamp/report.json"
}

$report = Invoke-RelationshipProjectionReconcileGate `
    -BaseUri $BaseUri `
    -ApiKey $apiKey `
    -OutputPath $OutputPath `
    -PageSize $PageSize `
    -RequiredCleanPasses $RequiredCleanPasses `
    -MinimumRebuilderStablePasses $MinimumRebuilderStablePasses `
    -MaxPagesPerPass $MaxPagesPerPass `
    -TimeoutSeconds $TimeoutSeconds `
    -AllowInsecureHttp:$AllowInsecureHttp

$resolvedOutput = [System.IO.Path]::GetFullPath($OutputPath)
if (-not $report.GatePassed) {
    Write-Error "Relationship projection reconcile gate failed: $($report.FailureReason). Report: $resolvedOutput"
    exit 1
}

Write-Output "Relationship projection reconcile gate passed. Report: $resolvedOutput"
