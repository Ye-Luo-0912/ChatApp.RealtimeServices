# 打包并发布 Realtime 契约包到本地 NuGet feed。
# 同一 PackageId/Version 已存在但内容不同会立即失败，禁止不可变包被覆盖。
# 用法：先执行 locked restore，再运行：
# pwsh tools/publish-contracts.ps1 -FeedPath <feed目录>
param(
    [Parameter(Mandatory = $true)]
    [string]$FeedPath,

    [ValidateNotNullOrEmpty()]
    [string]$Configuration = 'Release'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$packs = @(
    (Join-Path $root 'ChatApp.Realtime.Abstractions\ChatApp.Realtime.Abstractions.csproj'),
    (Join-Path $root 'ChatApp.Realtime.Integration\ChatApp.Realtime.Integration.csproj'),
    (Join-Path $root 'ChatApp.Realtime.Outbox.EntityFrameworkCore\ChatApp.Realtime.Outbox.EntityFrameworkCore.csproj')
)

$resolvedFeedPath = [System.IO.Path]::GetFullPath($FeedPath)
$stagingRoot = Join-Path $root ".artifacts/package-publish/$([Guid]::NewGuid().ToString('N'))"

function Get-PackageContentFingerprint([string]$Path) {
    $archive = [System.IO.Compression.ZipFile]::OpenRead($Path)
    $canonicalStream = [System.IO.MemoryStream]::new()
    $writer = [System.IO.BinaryWriter]::new(
        $canonicalStream,
        [System.Text.UTF8Encoding]::new($false),
        $true)

    try {
        $entries = @(
            $archive.Entries |
                Where-Object {
                    # NuGet writes a random OPC core-properties part name and matching relationship.
                    # The nuspec and payload entries below remain the stable semantic package content.
                    $_.FullName -ne '.signature.p7s' -and
                    $_.FullName -ne '_rels/.rels' -and
                    -not $_.FullName.StartsWith(
                        'package/services/metadata/core-properties/',
                        [StringComparison]::Ordinal)
                } |
                Sort-Object -Property FullName -CaseSensitive
        )
        foreach ($entry in $entries) {
            $writer.Write($entry.FullName)
            $writer.Write([long]$entry.Length)
            $entryStream = $entry.Open()
            try {
                $entryStream.CopyTo($canonicalStream)
            }
            finally {
                $entryStream.Dispose()
            }
        }

        $writer.Flush()
        return [Convert]::ToHexString(
            [System.Security.Cryptography.SHA256]::HashData($canonicalStream.ToArray()))
    }
    finally {
        $writer.Dispose()
        $canonicalStream.Dispose()
        $archive.Dispose()
    }
}

try {
    New-Item -ItemType Directory -Force -Path $stagingRoot | Out-Null
    foreach ($proj in $packs) {
        # Repository-local packages can be produced before the final Git commit exists.
        # Omitting SCM-derived metadata keeps the same version reproducible across that commit boundary.
        dotnet pack $proj -c $Configuration --no-restore -o $stagingRoot `
            -p:EnableSourceControlManagerQueries=false
        if ($LASTEXITCODE -ne 0) { throw "pack 失败: $proj" }
    }

    $builtPackages = @(Get-ChildItem -LiteralPath $stagingRoot -Filter '*.nupkg' -File)
    if ($builtPackages.Count -ne $packs.Count) {
        throw "预期生成 $($packs.Count) 个包，实际生成 $($builtPackages.Count) 个。"
    }

    New-Item -ItemType Directory -Force -Path $resolvedFeedPath | Out-Null
    foreach ($package in $builtPackages) {
        $destination = Join-Path $resolvedFeedPath $package.Name
        if (Test-Path -LiteralPath $destination) {
            $builtFingerprint = Get-PackageContentFingerprint $package.FullName
            $publishedFingerprint = Get-PackageContentFingerprint $destination
            if ($builtFingerprint -ne $publishedFingerprint) {
                throw "包不可变性冲突：$($package.Name) 已存在但规范化内容 SHA256 不同。请先提升包版本，禁止覆盖旧制品。"
            }

            Write-Host "已验证不可变包: $($package.Name) (content $builtFingerprint)"
            continue
        }

        Copy-Item -LiteralPath $package.FullName -Destination $destination
        $archiveHash = (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash
        $contentFingerprint = Get-PackageContentFingerprint $destination
        Write-Host "已发布: $($package.Name) (archive $archiveHash; content $contentFingerprint)"
    }
}
finally {
    if (Test-Path -LiteralPath $stagingRoot) {
        Remove-Item -LiteralPath $stagingRoot -Recurse -Force
    }
}

Write-Host "已发布到: $resolvedFeedPath"
