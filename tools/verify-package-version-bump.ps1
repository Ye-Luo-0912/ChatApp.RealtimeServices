# 验证可打包项目的源码/构建输入一旦变化，就必须相对 CI base 提升包版本。
# 这条门禁防止同一 PackageId/Version 在不同提交中产生不同制品。
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$BaseRef
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$projects = @(
    'ChatApp.Realtime.Abstractions/ChatApp.Realtime.Abstractions.csproj',
    'ChatApp.Realtime.Integration/ChatApp.Realtime.Integration.csproj',
    'ChatApp.Realtime.Outbox.EntityFrameworkCore/ChatApp.Realtime.Outbox.EntityFrameworkCore.csproj'
)
$compiledInputExtensions = @('.cs', '.csproj', '.props', '.targets', '.resx')

Push-Location $root
try {
    & git rev-parse --verify $BaseRef *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "无法解析比较基准 '$BaseRef'。CI checkout 必须使用 fetch-depth: 0。"
    }

    foreach ($relativeProject in $projects) {
        $projectPath = Join-Path $root $relativeProject
        [xml]$currentProject = Get-Content -Raw -LiteralPath $projectPath
        $currentVersionNode = $currentProject.SelectSingleNode('/Project/PropertyGroup/Version')
        if ($null -eq $currentVersionNode -or [string]::IsNullOrWhiteSpace($currentVersionNode.InnerText)) {
            throw "可打包项目必须显式声明 Version: $relativeProject"
        }

        $baseProjectLines = @(& git show "${BaseRef}:$relativeProject" 2> $null)
        if ($LASTEXITCODE -ne 0) {
            Write-Host "新增包项目，跳过历史版本比较: $relativeProject ($($currentVersionNode.InnerText))"
            continue
        }

        $baseProjectText = ($baseProjectLines -join [Environment]::NewLine).TrimStart([char]0xFEFF)
        [xml]$baseProject = $baseProjectText
        $baseVersionNode = $baseProject.SelectSingleNode('/Project/PropertyGroup/Version')
        if ($null -eq $baseVersionNode -or [string]::IsNullOrWhiteSpace($baseVersionNode.InnerText)) {
            throw "基准提交中的可打包项目未显式声明 Version: $relativeProject"
        }

        $projectDirectory = Split-Path -Parent $relativeProject
        $projectReferences = @(
            $currentProject.SelectNodes('/Project/ItemGroup/ProjectReference') |
                ForEach-Object {
                    $referenceInclude = $_.Include.Replace('\', '/')
                    $projectDirectoryPath = Join-Path $root $projectDirectory
                    $referencedPath = [System.IO.Path]::GetFullPath(
                        (Join-Path $projectDirectoryPath $referenceInclude))
                    [System.IO.Path]::GetRelativePath($root, $referencedPath).Replace('\', '/')
                }
        )
        $diffTargets = @($projectDirectory, 'Directory.Build.props', 'Directory.Build.targets') + $projectReferences
        $gitArguments = @('diff', '--name-only', '--diff-filter=ACMRD', $BaseRef, '--') + $diffTargets
        $changedPaths = @(& git @gitArguments)
        if ($LASTEXITCODE -ne 0) {
            throw "无法读取 $relativeProject 相对 $BaseRef 的变更。"
        }

        $packageInputsChanged = @(
            $changedPaths | Where-Object {
                $normalizedPath = $_.Replace('\', '/')
                $extension = [System.IO.Path]::GetExtension($normalizedPath)
                $normalizedPath -in @('Directory.Build.props', 'Directory.Build.targets') -or
                    $normalizedPath -in $projectReferences -or
                    ($normalizedPath.StartsWith("$projectDirectory/", [StringComparison]::Ordinal) -and
                        $extension -in $compiledInputExtensions)
            }
        )

        if ($packageInputsChanged.Count -eq 0) {
            Write-Host "包输入未变化: $relativeProject"
            continue
        }

        $baseVersion = $baseVersionNode.InnerText.Trim()
        $currentVersion = $currentVersionNode.InnerText.Trim()
        try {
            $baseSemanticVersion = [Version]$baseVersion
            $currentSemanticVersion = [Version]$currentVersion
        }
        catch {
            throw "Version 必须是可比较的数字版本：$relativeProject ($baseVersion -> $currentVersion)"
        }

        if ($currentSemanticVersion -le $baseSemanticVersion) {
            $changedList = $packageInputsChanged -join ', '
            throw "包版本未提升：$relativeProject 的输入已变化 ($changedList)，但 Version 为 $currentVersion（基准 $baseVersion）。"
        }

        Write-Host (
            "包版本门禁通过: $relativeProject $baseVersion -> $currentVersion; 输入: " +
            ($packageInputsChanged -join ', '))
    }
}
finally {
    Pop-Location
}
