param(
    [switch]$SkipConfig,
    [switch]$SkipRestart,
    [string]$ProjectPath = "$PSScriptRoot\..\ChatApp.RealtimeServices\ChatApp.RealtimeServices.csproj"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$scriptDir = $PSScriptRoot
$repoRoot = Resolve-Path "$scriptDir\.."
$userConfigPath = "$env:USERPROFILE\.chatapp\realtime.user.json"
$appsettingsPath = "$repoRoot\ChatApp.RealtimeServices\appsettings.json"

$natsSubject = "chat.incoming-messages"
$consumerGroup = "chatapp-realtime-services"
$jsStream = "INCOMING_MESSAGES"

$ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$normalMsgId = "test-normal-$ts"
$dupMsgId = "test-dup-$ts"
$poisonMsgId = "test-poison-$ts"

$testSenderUserId = 9999
$testReceiverUserId = 8888

Write-Host "===== JetStream 入站消息测试 =====" -ForegroundColor Cyan
Write-Host ""

# ─── Prerequisite checks ──────────────────────────────────────────
Write-Host "[1/7] 检查前置条件..." -ForegroundColor Yellow

$natsAvailable = $null -ne (Get-Command nats -ErrorAction SilentlyContinue)
if (-not $natsAvailable) {
    Write-Error "nats CLI 未安装。请先安装: https://github.com/nats-io/natscli"
    exit 1
}
Write-Host "  nats CLI: 已安装"

$natsConnected = $false
try {
    $conn = nats server check connection --server nats://localhost:4222 2>&1
    $natsConnected = $LASTEXITCODE -eq 0
} catch { }
if (-not $natsConnected) {
    Write-Error "无法连接 NATS (nats://localhost:4222)。请先启动 NATS。"
    exit 1
}
Write-Host "  NATS 连接: OK (nats://localhost:4222)"

$dbOk = $false
try {
    $dbCheck = docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -c "SELECT 1;" 2>&1
    $dbOk = $LASTEXITCODE -eq 0
} catch { }
if (-not $dbOk) {
    Write-Error "无法连接 PostgreSQL (chatapp_postgres)。请先启动 Docker。"
    exit 1
}
Write-Host "  PostgreSQL: OK"
Write-Host ""

# ─── Switch to JetStream mode ─────────────────────────────────────
if (-not $SkipConfig) {
    Write-Host "[2/7] 切换到 JetStream 模式..." -ForegroundColor Yellow

    $userConfigDir = Split-Path $userConfigPath -Parent
    if (-not (Test-Path $userConfigDir)) { New-Item -ItemType Directory -Path $userConfigDir -Force | Out-Null }

    if (Test-Path $userConfigPath) {
        $config = Get-Content $userConfigPath -Raw | ConvertFrom-Json
    } else {
        $config = [PSCustomObject]@{ ConnectionStrings = [PSCustomObject]@{} }
    }
    $config | Add-Member -MemberType NoteProperty -Name Nats -Value ([PSCustomObject]@{ Mode = "JetStream" }) -Force
    $config | ConvertTo-Json -Depth 5 | Set-Content $userConfigPath -NoNewline
    Write-Host "  已设置 ~/.chatapp/realtime.user.json Nats:Mode = JetStream"

    $appsettings = Get-Content $appsettingsPath -Raw | ConvertFrom-Json
    if ($appsettings.Nats.Mode -ne "JetStream") {
        Write-Host "  注意: appsettings.json Nats:Mode 仍为 '$($appsettings.Nats.Mode)'，用户配置会覆盖它"
    }
} else {
    Write-Host "[2/7] 跳过配置切换 (-SkipConfig)" -ForegroundColor Yellow
}
Write-Host ""

# ─── Restart service ──────────────────────────────────────────────
if (-not $SkipRestart) {
    Write-Host "[3/7] 启动实时服务 (JetStream)..." -ForegroundColor Yellow
    Write-Host "  等待 JetStream 流和消费者自动创建..."

    $svcJob = Start-Job -ScriptBlock {
        param($proj)
        Set-Location $using:repoRoot
        dotnet run --project $proj --no-launch-profile 2>&1
    } -ArgumentList $ProjectPath

    Start-Sleep -Seconds 10

    $svcRunning = $svcJob.State -eq "Running"
    if (-not $svcRunning) {
        $svcOutput = $svcJob | Receive-Job
        Write-Error "服务启动失败: $svcOutput"
        $svcJob | Remove-Job -Force
        exit 1
    }
    Write-Host "  服务已启动 (PID: $($svcJob.Id))"
} else {
    Write-Host "[3/7] 跳过服务重启 (-SkipRestart)" -ForegroundColor Yellow
}
Write-Host ""

try {
    # ─── Verify JetStream stream/consumer ──────────────────────────
    Start-Sleep -Seconds 5
    Write-Host "[4/7] 验证 JetStream 流和消费者..." -ForegroundColor Yellow

    $streamInfo = nats str info $jsStream 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "  $jsStream 流信息获取失败: $streamInfo"
    } else {
        Write-Host "  流 $jsStream: OK"
    }

    $conInfo = nats con info $jsStream $consumerGroup 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "  消费者信息获取失败 (可能是队列组名编码问题): $conInfo"
    } else {
        Write-Host "  消费者 $consumerGroup: OK"
        Write-Host "  $conInfo" | Select-String "Pending|AckPending|NumDelivered"
    }
    Write-Host ""

    # ─── Test 1: 正常消息 ─────────────────────────────────────────
    Write-Host "[5/7] 发布正常消息并验证入库..." -ForegroundColor Yellow

    $normalMsg = @{
        CommandId       = $normalMsgId
        ClientMessageId = $normalMsgId
        SenderUserId    = $testSenderUserId
        SenderSessionId = "test-session-normal-$ts"
        ReceiverUserId  = $testReceiverUserId
        Content         = "JetStream 正常测试消息"
        ReceivedAtMs    = $ts
    } | ConvertTo-Json -Compress

    nats pub $natsSubject $normalMsg 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "  正常消息发布失败"
        exit 1
    }
    Write-Host "  已发布: $normalMsgId"
    Start-Sleep -Seconds 3

    $dbCount = docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -t -c "SELECT count(*) FROM realtime.messages WHERE client_message_id = '$normalMsgId';" 2>&1
    $dbCount = $dbCount.Trim()
    if ($dbCount -eq "1") {
        Write-Host "  DB 验证: 1 条记录 (正常入库)" -ForegroundColor Green
    } else {
        Write-Warning "  DB 验证: $dbCount 条记录 (预期 1 条)"
    }
    Write-Host ""

    # ─── Test 2: 重复消息 ─────────────────────────────────────────
    Write-Host "[6/7] 发布重复消息 (相同 ClientMessageId)..." -ForegroundColor Yellow

    $dupMsg = @{
        CommandId       = $dupMsgId
        ClientMessageId = $dupMsgId
        SenderUserId    = $testSenderUserId
        SenderSessionId = "test-session-dup-$ts"
        ReceiverUserId  = $testReceiverUserId
        Content         = "JetStream 重复测试消息"
        ReceivedAtMs    = $ts
    } | ConvertTo-Json -Compress

    # 第一次发布
    nats pub $natsSubject $dupMsg 2>&1 | Out-Null
    Start-Sleep -Seconds 2
    $count1 = (docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -t -c "SELECT count(*) FROM realtime.messages WHERE client_message_id = '$dupMsgId';" 2>&1).Trim()

    # 第二次发布 (重复)
    nats pub $natsSubject $dupMsg 2>&1 | Out-Null
    Start-Sleep -Seconds 2
    $count2 = (docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -t -c "SELECT count(*) FROM realtime.messages WHERE client_message_id = '$dupMsgId';" 2>&1).Trim()

    if ($count1 -eq "1" -and $count2 -eq "1") {
        Write-Host "  去重验证: 两次发布只有 1 条入库 (重复被跳过)" -ForegroundColor Green
    } else {
        Write-Warning "  去重验证: 第一次=$count1, 第二次=$count2 (预期均为 1)"
    }
    Write-Host ""

    # ─── Test 3: 毒丸消息 ─────────────────────────────────────────
    Write-Host "[7/7] 发布毒丸消息 (空内容引发处理失败)..." -ForegroundColor Yellow

    $poisonMsg = @{
        CommandId       = $poisonMsgId
        ClientMessageId = $poisonMsgId
        SenderUserId    = $testSenderUserId
        SenderSessionId = "test-session-poison-$ts"
        ReceiverUserId  = $testReceiverUserId
        Content         = ""
        ReceivedAtMs    = $ts
    } | ConvertTo-Json -Compress

    nats pub $natsSubject $poisonMsg 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  已发布毒丸: $poisonMsgId (Content 为空)"
    }

    Start-Sleep -Seconds 5

    # 检查消费者状态
    $conInfo2 = nats con info $jsStream $consumerGroup 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  消费者状态:"
        Write-Host "  $conInfo2" | Select-String "Pending|AckPending|NumDelivered|NumAckPending"
    }

    Write-Host ""
    Write-Host "  毒丸策略说明:" -ForegroundColor DarkGray
    Write-Host "    - Content 为空 → ProcessAsync 返回 Failed → Worker 调 NakAsync"
    Write-Host "    - JetStream 会按 AckWait(30s) 重新投递，MaxDeliver=10"
    Write-Host "    - 投递计数 >= 8 时 Worker 输出 '检测到毒丸消息，直接丢弃' 并 Ack"
    Write-Host "    - 前 7 次投递在服务日志中可见 Nak 和 '入站消息处理失败' 日志"
    Write-Host ""

} finally {
    # ─── Cleanup ───────────────────────────────────────────────────
    if (-not $SkipRestart) {
        Write-Host "停止测试服务..." -ForegroundColor Yellow
        $svcJob | Stop-Job -PassThru | Remove-Job -Force
        Write-Host "  服务已停止"
    }
}

# ─── Report ────────────────────────────────────────────────────────
Write-Host "===== 测试完成 =====" -ForegroundColor Cyan
Write-Host ""
Write-Host "正常消息 ID: $normalMsgId"
Write-Host "重复消息 ID: $dupMsgId"
Write-Host "毒丸消息 ID: $poisonMsgId"
Write-Host ""
Write-Host "手动验证命令:" -ForegroundColor DarkGray
Write-Host "  # 检查正常消息入库"
Write-Host "  docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -c ""SELECT * FROM realtime.messages WHERE client_message_id = '$normalMsgId';"""
Write-Host ""
Write-Host "  # 检查重复消息数量 (应只有 1 条)"
Write-Host "  docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -c ""SELECT count(*) FROM realtime.messages WHERE client_message_id = '$dupMsgId';"""
Write-Host ""
Write-Host "  # 检查毒丸消费者状态"
Write-Host "  nats con info $jsStream $consumerGroup"
