<#
.SYNOPSIS
六-3：BenchmarkDotNet JSON 结果 baseline 对比脚本。
对比当前运行结果与 baseline，若关键指标回退超过阈值则返回非零退出码。
支持 Mean 耗时、Allocated 内存、SQL 命令数（Return 值）三项指标对比。

.PARAMETER Baseline
baseline JSON 文件路径（BenchmarkDotNet --exporters json 产出）。

.PARAMETER Current
当前运行 JSON 文件路径。

.PARAMETER MeanRegressionPct
平均耗时回退阈值百分比，默认 10。

.PARAMETER AllocRegressionPct
内存分配回退阈值百分比，默认 10。

.PARAMETER SqlCommandCountRegressionPct
SQL 命令数回退阈值百分比，默认 0（任何增加都算回退，因为 SQL 往返次数应单调不增）。

.EXAMPLE
pwsh compare-benchmarks.ps1 -Baseline baseline.json -Current current.json
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Baseline,
    [Parameter(Mandatory)] [string] $Current,
    [double] $MeanRegressionPct = 10,
    [double] $AllocRegressionPct = 10,
    [double] $SqlCommandCountRegressionPct = 0
)

$ErrorActionPreference = 'Stop'

function Read-BenchmarkResults([string] $path) {
    $json = Get-Content $path -Raw | ConvertFrom-Json
    $map = @{}
    foreach ($r in $json.Benchmarks) {
        $key = $r.DisplayInfo
        if (-not $key) { $key = "$($r.Method)/$($r.Parameters)" }
        # 六-1：SQL 命令数通过 benchmark 方法返回值报告，BenchmarkDotNet 将其存入 Properties.Return。
        # 不同版本字段名可能是 "Return" 或在 Properties 里，做兼容处理。
        $sqlCmdCount = $null
        if ($r.PSObject.Properties.Name -contains 'Return') {
            $sqlCmdCount = $r.Return
        } elseif ($r.PSObject.Properties.Name -contains 'Properties') {
            $props = $r.Properties
            if ($props.PSObject.Properties.Name -contains 'Return') {
                $sqlCmdCount = $props.Return
            }
        }
        $map[$key] = @{
            Mean = $r.Statistics.Mean
            Median = $r.Statistics.Median
            StdDev = $r.Statistics.StandardDeviation
            AllocatedBytes = $r.Memory?.AllocatedBytes
            Operations = $r.Statistics.Operations
            SqlCommandCount = $sqlCmdCount
        }
    }
    return $map
}

$baselineResults = Read-BenchmarkResults $Baseline
$currentResults = Read-BenchmarkResults $Current

$failures = @()
$improvements = @()
$ok = @()

foreach ($key in $currentResults.Keys) {
    if (-not $baselineResults.ContainsKey($key)) {
        Write-Warning "基准中找不到: $key（新基准或改名）"
        continue
    }

    $base = $baselineResults[$key]
    $curr = $currentResults[$key]

    # Mean regression check
    if ($base.Mean -gt 0) {
        $meanPct = (($curr.Mean - $base.Mean) / $base.Mean) * 100
        if ($meanPct -gt $MeanRegressionPct) {
            $failures += "[FAIL] $key : Mean 回退 $([math]::Round($meanPct, 1))% (baseline=$([math]::Round($base.Mean, 1))ns, current=$([math]::Round($curr.Mean, 1))ns)"
        } elseif ($meanPct -lt -$MeanRegressionPct) {
            $improvements += "[IMPROVE] $key : Mean 改善 $([math]::Round(-$meanPct, 1))%"
        } else {
            $ok += "[OK] $key : Mean 差异 $([math]::Round($meanPct, 1))%"
        }
    }

    # Allocation regression check
    if ($base.AllocatedBytes -and $curr.AllocatedBytes) {
        $allocPct = (($curr.AllocatedBytes - $base.AllocatedBytes) / $base.AllocatedBytes) * 100
        if ($allocPct -gt $AllocRegressionPct) {
            $failures += "[FAIL] $key : Alloc 回退 $([math]::Round($allocPct, 1))% (baseline=$($base.AllocatedBytes)B, current=$($curr.AllocatedBytes)B)"
        }
    }

    # 六-1：SQL 命令数回退检查（单调不减门禁）。
    # SQL 往返次数应单调不增——若增加说明代码路径被拆分成多次往返。
    if ($base.SqlCommandCount -ne $null -and $curr.SqlCommandCount -ne $null) {
        $baseCmd = [double]$base.SqlCommandCount
        $currCmd = [double]$curr.SqlCommandCount
        if ($baseCmd -gt 0) {
            $cmdPct = (($currCmd - $baseCmd) / $baseCmd) * 100
            if ($cmdPct -gt $SqlCommandCountRegressionPct) {
                $failures += "[FAIL] $key : SQL 命令数回退 $([math]::Round($cmdPct, 1))% (baseline=$baseCmd, current=$currCmd)"
            } elseif ($cmdPct -lt 0) {
                $improvements += "[IMPROVE] $key : SQL 命令数减少 $([math]::Round(-$cmdPct, 1))% (baseline=$baseCmd, current=$currCmd)"
            } else {
                $ok += "[OK] $key : SQL 命令数 $currCmd (baseline=$baseCmd)"
            }
        }
    }
}

Write-Output ""
Write-Output "=== Benchmark Baseline Comparison ==="
Write-Output "Baseline: $Baseline"
Write-Output "Current:  $Current"
Write-Output "Threshold: Mean > ${MeanRegressionPct}%, Alloc > ${AllocRegressionPct}%, SQL Cmd > ${SqlCommandCountRegressionPct}%"
Write-Output ""

if ($improvements.Count -gt 0) {
    Write-Output "--- Improvements ---"
    $improvements | ForEach-Object { Write-Output $_ }
    Write-Output ""
}

if ($ok.Count -gt 0) {
    Write-Output "--- Within Threshold ---"
    $ok | ForEach-Object { Write-Output $_ }
    Write-Output ""
}

if ($failures.Count -gt 0) {
    Write-Output "--- Regressions (FAIL) ---"
    $failures | ForEach-Object { Write-Output $_ }
    Write-Output ""
    Write-Error "检测到 $($failures.Count) 项性能回退，超过阈值。"
    exit 1
}

Write-Output "所有基准在阈值范围内，门禁通过。"
exit 0