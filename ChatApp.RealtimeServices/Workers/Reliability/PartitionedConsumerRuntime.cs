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

    public PartitionedConsumerRuntime(
        string workerName,
        int partitionCount,
        int queueCapacity,
        int workerIntervalMs,
        RealtimeReadinessState readinessState,
        ILogger logger,
        Func<TEnvelope, long>? byteSizer = null,
        long maxQueueBytes = 0)
    {
        _workerName = workerName;
        _partitionCount = partitionCount;
        _queueCapacity = queueCapacity;
        _workerIntervalMs = workerIntervalMs;
        _readinessState = readinessState;
        _logger = logger;
        _byteSizer = byteSizer;
        _maxQueueBytes = maxQueueBytes;
    }

    /// <summary>
    /// 运行分区消费生命周期：创建 channels、启动 heartbeat、启动分区 processors、
    /// 运行 produce 循环（含订阅重连退避）、协调 shutdown。
    /// 业务 Worker 提供 consume 委托和 processPartition 委托。
    /// </summary>
    public async Task RunAsync(
        Func<CancellationToken, IAsyncEnumerable<TEnvelope>> consume,
        Func<TEnvelope, int> getPartition,
        Func<int, ChannelReader<TEnvelope>, CancellationToken, Task> processPartition,
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

        var channels = PartitionChannelPool<TEnvelope>.Create(_partitionCount, _queueCapacity);
        var byteBudget = _maxQueueBytes > 0 && _byteSizer is not null
            ? new ByteBudget(_maxQueueBytes)
            : null;

        // Perf-6：如果配置了字节预算，用 ByteReleasingChannelReader 包装每个分区的 reader，
        // 使处理完成后自动释放字节配额。
        var processors = channels
            .Select((channel, index) =>
            {
                var reader = byteBudget is not null && _byteSizer is not null
                    ? new ByteReleasingChannelReader<TEnvelope>(channel.Reader, byteBudget, _byteSizer)
                    : channel.Reader;
                return processPartition(index, reader, faultToken);
            })
            .ToArray();
        var processorsAggregate = Task.WhenAll(processors);

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
        IReadOnlyList<Channel<TEnvelope>> channels,
        ByteBudget? byteBudget,
        CancellationToken ct)
    {
        var retryAttempt = 0;
        while (!ct.IsCancellationRequested)
        {
            // Reliability-2：订阅尝试开始时标记连接状态。
            _readinessState.MarkSubscriptionConnected(_workerName, connected: false);
            try
            {
                await foreach (var envelope in consume(ct).ConfigureAwait(false))
                {
                    retryAttempt = 0;
                    // Reliability-2：用 RecordMessageConsumed 替代 MarkHeartbeat，使 readiness 反映真实消费进展。
                    _readinessState.RecordMessageConsumed(_workerName);
                    _readinessState.MarkSubscriptionConnected(_workerName, connected: true);
                    var partition = getPartition(envelope);

                    // Perf-6：入队前按 payload 字节长度计费。超预算时施加背压：
                    // 等待已入队项被处理后再重试，而非立即入队。
                    if (byteBudget is not null && _byteSizer is not null)
                    {
                        var bytes = _byteSizer(envelope);
                        while (!byteBudget.TryAcquire(bytes) && !ct.IsCancellationRequested)
                        {
                            await Task.Delay(Math.Max(10, _workerIntervalMs / 4), ct).ConfigureAwait(false);
                        }
                        if (ct.IsCancellationRequested)
                            return;
                    }

                    await channels[partition].Writer.WriteAsync(envelope, ct).ConfigureAwait(false);

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

    private void ReportQueueDepth(IReadOnlyList<Channel<TEnvelope>> channels)
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
}
