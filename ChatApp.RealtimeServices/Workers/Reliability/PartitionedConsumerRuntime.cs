using System.Threading.Channels;
using ChatApp.Realtime.Infrastructure.Core.Health;
using Microsoft.Extensions.Logging;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// 提取自 IncomingMessageWorker / MessageReceiptWorker 的分区消费生命周期骨架。
/// 负责：心跳协调、分区 Channel 创建、订阅重连退避、shutdown 协调。
/// 业务 Worker 通过委托提供：如何取得分区键、如何消费流、如何处理单个分区。
/// </summary>
internal sealed class PartitionedConsumerRuntime<TEnvelope>
{
    private readonly string _workerName;
    private readonly int _partitionCount;
    private readonly int _queueCapacity;
    private readonly int _workerIntervalMs;
    private readonly RealtimeReadinessState _readinessState;
    private readonly ILogger _logger;
    private readonly Func<TEnvelope, long>? _byteSizer;
    private readonly long _maxQueueBytes;
    private readonly long _maxSinglePayloadBytes;

    public PartitionedConsumerRuntime(
        string workerName,
        int partitionCount,
        int queueCapacity,
        int workerIntervalMs,
        RealtimeReadinessState readinessState,
        ILogger logger,
        Func<TEnvelope, long>? byteSizer = null,
        long maxQueueBytes = 0,
        long maxSinglePayloadBytes = 0)
    {
        _workerName = workerName;
        _partitionCount = partitionCount;
        _queueCapacity = queueCapacity;
        _workerIntervalMs = workerIntervalMs;
        _readinessState = readinessState;
        _logger = logger;
        _byteSizer = byteSizer;
        _maxQueueBytes = maxQueueBytes;
        _maxSinglePayloadBytes = maxSinglePayloadBytes;
    }

    /// <summary>
    /// 运行分区消费生命周期：创建 channels、启动 heartbeat、启动分区 processors、
    /// 运行 produce 循环（含订阅重连退避）、协调 shutdown。
    /// 业务 Worker 提供 consume 委托和 processPartition 委托。
    /// </summary>
    /// <remarks>
    /// P0-6：channel 元素类型为 <see cref="LeasedEnvelope{TEnvelope}"/>，
    /// 携带 <see cref="ByteBudgetLease"/>。processor 应在 finally 块 Dispose lease，
    /// 使预算覆盖 queued + processing，而非在 dequeue 时释放。
    /// </remarks>
    public async Task RunAsync(
        Func<CancellationToken, IAsyncEnumerable<TEnvelope>> consume,
        Func<TEnvelope, int> getPartition,
        Func<int, ChannelReader<LeasedEnvelope<TEnvelope>>, CancellationToken, Task> processPartition,
        CancellationToken stoppingToken)
    {
        _readinessState.MarkStarted(_workerName);
        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        // Reliability-1：processor 故障时取消 produce 循环；produce 故障时取消 processors。
        // 不再用 stoppingToken 直接驱动两者，否则 processor 死后 produce 会卡在满 channel 上，
        // heartbeat 仍继续刷新，readiness 保持“正常”。
        using var faultCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var faultToken = faultCts.Token;
        var heartbeatTask = WorkerHeartbeat.RunAsync(_readinessState, _workerName, heartbeatCts.Token);

        var channels = PartitionChannelPool<LeasedEnvelope<TEnvelope>>.Create(_partitionCount, _queueCapacity);
        var byteBudget = _maxQueueBytes > 0 && _byteSizer is not null
            ? new ByteBudget(_maxQueueBytes, _maxSinglePayloadBytes)
            : null;

        // P0-6：不再使用 ByteReleasingChannelReader。lease 在 processor finally 块释放，
        // 预算覆盖 queued + processing，避免正在执行的大消息不计入预算。
        // Reliability-3：使用一个 runtime 级 reporter 定期刷新所有分区深度，
        // 避免每分区各建一个 Timer 并重复 O(P) 扫描（总计 O(P²)）。
        var processors = channels
            .Select((channel, index) =>
                processPartition(index, channel.Reader, faultToken))
            .ToArray();
        var processorsAggregate = Task.WhenAll(processors);
        var queueDepthTask = ReportQueueDepthUntilCompletedAsync(
            processorsAggregate,
            channels,
            faultToken);

        try
        {
            // Reliability-1：并发观察 produce 循环与 processor 聚合任务。
            // 任一异常都取消整个 Worker 并标记 Faulted，避免 processor 死后 produce 卡在满 channel。
            var produceTask = ProduceAsync(consume, getPartition, channels, byteBudget, faultToken);
            var completed = await Task.WhenAny(produceTask, processorsAggregate).ConfigureAwait(false);

            if (completed == processorsAggregate)
            {
                // Processor 先结束（异常或提前退出）。取消 produce 循环以避免卡在满 channel 上。
                faultCts.Cancel();
                try
                {
                    await produceTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (faultToken.IsCancellationRequested) { }
                // 重新 await 以抛出 processor 的原始异常（如果有）；无异常则继续到 finally。
                await processorsAggregate.ConfigureAwait(false);
            }
            else
            {
                // Produce 先结束（正常停止或异常）。取消 faultCts 以通知 processors 加速退出。
                faultCts.Cancel();
                await produceTask.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("{Worker} 正在停止。", _workerName);
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(_workerName, ex);
            _logger.LogError(ex, "{Worker} 异常退出。", _workerName);
            throw;
        }
        finally
        {
            foreach (var channel in channels)
                channel.Writer.TryComplete();
            try
            {
                await Task.WhenAll(processors).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested
                                                     || faultToken.IsCancellationRequested) { }
            finally
            {
                // P0-6：释放 ByteBudget 持有的 SemaphoreSlim 资源
                byteBudget?.Dispose();
                faultCts.Cancel();
                await WorkerHeartbeat
                    .SuppressCancellationAsync(queueDepthTask)
                    .ConfigureAwait(false);
                heartbeatCts.Cancel();
                await WorkerHeartbeat.SuppressCancellationAsync(heartbeatTask).ConfigureAwait(false);
                _readinessState.MarkStopped(_workerName);
                _logger.LogInformation("{Worker} 已停止。", _workerName);
            }
        }
    }

    private async Task ProduceAsync(
        Func<CancellationToken, IAsyncEnumerable<TEnvelope>> consume,
        Func<TEnvelope, int> getPartition,
        IReadOnlyList<Channel<LeasedEnvelope<TEnvelope>>> channels,
        ByteBudget? byteBudget,
        CancellationToken ct)
    {
        var retryAttempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // IAsyncEnumerable 消费接口没有独立的“订阅已建立”回调。
                // 进入 ConsumeAsync 枚举即表示消费循环已激活；真正的连接故障会进入
                // catch 并置 false，同时依赖健康检查会独立验证 NATS。
                // 不能在等待首条消息时保持 false，否则空队列会永远无法 Ready。
                _readinessState.MarkSubscriptionConnected(
                    _workerName,
                    connected: true);
                await foreach (var envelope in consume(ct).ConfigureAwait(false))
                {
                    retryAttempt = 0;
                    // Reliability-2：用 RecordMessageConsumed 替代 MarkHeartbeat，使 readiness 反映真实消费进展。
                    _readinessState.RecordMessageConsumed(_workerName);
                    var partition = getPartition(envelope);

                    // P0-6：入队前按 payload 字节长度计费，返回 lease。
                    // lease 在 processor finally 块 Dispose，预算覆盖 queued + processing。
                    // 单条消息超过 MaxSinglePayloadBytes 时永久拒绝（不获取 lease，直接入队让 processor 死信）。
                    ByteBudgetLease? lease = null;
                    if (byteBudget is not null && _byteSizer is not null)
                    {
                        var bytes = _byteSizer(envelope);
                        try
                        {
                            lease = await byteBudget.AcquireAsync(bytes, ct).ConfigureAwait(false);
                        }
                        catch (ByteBudgetOversizedException)
                        {
                            // 单条消息超过硬上限，不获取预算直接入队。
                            // 处理器会在 ProcessOneAsync 中检测 MaxSinglePayloadBytes 并转入死信流。
                            lease = null;
                        }
                    }

                    var leased = new LeasedEnvelope<TEnvelope>(envelope, lease);
                    await channels[partition].Writer.WriteAsync(leased, ct).ConfigureAwait(false);

                    // Reliability-2：报告队列深度，满队列持续过久会被 GetSnapshot 判定为不就绪。
                    ReportQueueDepth(channels);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                retryAttempt++;
                var delay = ConsumerRetryPolicy.CalculateSubscriptionRetryDelay(retryAttempt);
                _logger.LogWarning(ex, "{Worker} 订阅中断，将重新订阅。延迟={Delay}", _workerName, delay);
                _readinessState.MarkSubscriptionConnected(_workerName, connected: false);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }

            _readinessState.MarkHeartbeat(_workerName);
            await Task.Delay(_workerIntervalMs, ct).ConfigureAwait(false);
        }
    }

    private void ReportQueueDepth(IReadOnlyList<Channel<LeasedEnvelope<TEnvelope>>> channels)
    {
        if (channels.Count == 0)
            return;
        var totalDepth = 0;
        foreach (var channel in channels)
        {
            try
            {
                totalDepth += channel.Reader.Count;
            }
            catch (NotSupportedException)
            {
                // 某些 Channel 实现不支持 Count，跳过队列深度报告。
                return;
            }
        }
        _readinessState.RecordQueueDepth(_workerName, totalDepth, _queueCapacity);
    }

    /// <summary>
    /// Reliability-3：单个 runtime 级 reporter 定期刷新所有分区的 QueueDepth。
    /// 队列排空后即使没有新入队，也能清除 stale QueueFullSince。
    /// </summary>
    private async Task ReportQueueDepthUntilCompletedAsync(
        Task processorsAggregate,
        IReadOnlyList<Channel<LeasedEnvelope<TEnvelope>>> channels,
        CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        try
        {
            while (!processorsAggregate.IsCompleted &&
                   await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                ReportQueueDepth(channels);
            }
        }
        catch (OperationCanceledException)
            when (ct.IsCancellationRequested)
        {
            // Worker 正常停止或任一 produce/processor 失败。
        }
        finally
        {
            ReportQueueDepth(channels);
        }
    }
}
