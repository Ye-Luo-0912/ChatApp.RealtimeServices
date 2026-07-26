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

    public PartitionedConsumerRuntime(
        string workerName,
        int partitionCount,
        int queueCapacity,
        int workerIntervalMs,
        RealtimeReadinessState readinessState,
        ILogger logger)
    {
        _workerName = workerName;
        _partitionCount = partitionCount;
        _queueCapacity = queueCapacity;
        _workerIntervalMs = workerIntervalMs;
        _readinessState = readinessState;
        _logger = logger;
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
        var heartbeatTask = WorkerHeartbeat.RunAsync(_readinessState, _workerName, heartbeatCts.Token);

        var channels = PartitionChannelPool<TEnvelope>.Create(_partitionCount, _queueCapacity);
        var processors = channels
            .Select((channel, index) => processPartition(index, channel.Reader, stoppingToken))
            .ToArray();

        try
        {
            await ProduceAsync(consume, getPartition, channels, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("{Worker} 正在停止。", _workerName);
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(_workerName, ex);
            _logger.LogError(ex, "{Worker} 生产循环异常退出。", _workerName);
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
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
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
        CancellationToken ct)
    {
        var retryAttempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var envelope in consume(ct).ConfigureAwait(false))
                {
                    retryAttempt = 0;
                    _readinessState.MarkHeartbeat(_workerName);
                    var partition = getPartition(envelope);
                    await channels[partition].Writer.WriteAsync(envelope, ct).ConfigureAwait(false);
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
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }

            _readinessState.MarkHeartbeat(_workerName);
            await Task.Delay(_workerIntervalMs, ct).ConfigureAwait(false);
        }
    }
}
