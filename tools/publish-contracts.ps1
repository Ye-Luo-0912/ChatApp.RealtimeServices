# P1-1：打包并发布 Realtime 契约包到本地 NuGet feed。
# 用法：pwsh tools/publish-contracts.ps1 -FeedPath <feed目录>
param(
    [Parameter(Mandatory = $true)]
    [string]$FeedPath
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$packs = @(
    (Join-Path $root 'ChatApp.Realtime.Abstractions\ChatApp.Realtime.Abstractions.csproj'),
    (Join-Path $root 'ChatApp.Realtime.Integration\ChatApp.Realtime.Integration.csproj')
)
New-Item -ItemType Directory -Force -Path $FeedPath | Out-Null
foreach ($proj in $packs) {
    dotnet pack $proj -c Release -o $FeedPath
    if ($LASTEXITCODE -ne 0) { throw "pack 失败: $proj" }
}
Write-Host "已发布到: $FeedPath"