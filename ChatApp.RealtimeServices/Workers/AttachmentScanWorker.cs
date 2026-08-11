using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.RealtimeServices.Concurrency;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 附件扫描消费者后台服务。从 <see cref="IAttachmentScanConsumer"/> 读取
/// <see cref="AttachmentScanCommand"/>，调用 <see cref="IAttachmentScanProcessor.ProcessAsync"/>
/// 驱动 Uploaded → Scanning → Available | Rejected 状态转换（全程带 state_version 条件更新）。
/// <para>
/// 扫描结果为 fire-and-forget（无回执）：命令可被重放，任何旧结果/重复命令都不会覆盖新状态。
/// 单个命令异常或过载不阻断消费循环，下一条继续；消费循环异常按退避重试。
/// </para>
/// </summary>
public sealed class AttachmentScanWorker : BackgroundService
{
    private const string WorkerName = nameof(AttachmentScanWorker);
    private const QueryPoolKind PoolKind = QueryPoolKind.Mutation;
    private readonly IAttachmentScanConsumer _consumer;
    private readonly IAttachmentScanProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeOptions _options;
    private readonly RealtimeQueryConcurrencyGate _queryGate;
    private readonly ILogger<AttachmentScanWorker> _logger;

    public AttachmentScanWorker(
        IAttachmentScanConsumer consumer,
        IAttachmentScanProcessor processor,
        RealtimeReadinessState readinessState,
        IOptions<RealtimeOptions> options,
        RealtimeQueryConcurrencyGate queryGate,
        ILogger<AttachmentScanWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _readinessState = readinessState;
        _options = options.Value;
        _queryGate = queryGate;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("附件扫描工作器已启动。");
        _readinessState.MarkStarted(WorkerName);

        using var heartbeatCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeatTask = RunHeartbeatAsync(heartbeatCts.Token);

        try
        {
            await ConsumeAndProcessAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("附件扫描工作器正在停止。");
        }
        finally
        {
            heartbeatCts.Cancel();
            await SuppressCancellationAsync(heartbeatTask).ConfigureAwait(false);
            _readinessState.MarkStopped(WorkerName);
        }
    }

    private async Task ConsumeAndProcessAsync(CancellationToken ct)
    {
        var retryAttempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var command in _consumer
                                   .ConsumeAsync(ct)
                                   .ConfigureAwait(false))
                {
                    retryAttempt = 0;
                    await ProcessAsync(command, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                retryAttempt++;
                var delay = TimeSpan.FromMilliseconds(
                    Math.Min(30_000, 500 * Math.Pow(2, Math.Min(retryAttempt, 6)))
                    + Random.Shared.Next(0, 500));
                _logger.LogWarning(
                    ex,
                    "附件扫描消费循环异常，将在 {Delay} 后重试。",
                    delay);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }

            _readinessState.MarkHeartbeat(WorkerName);
            await Task.Delay(_options.WorkerIntervalMs, ct)
                .ConfigureAwait(false);
        }
    }

    private async Task ProcessAsync(AttachmentScanCommand command, CancellationToken ct)
    {
        var gateAcquired = await _queryGate
            .WaitAsync(PoolKind, _options.OverloadGateTimeoutMs, ct)
            .ConfigureAwait(false);
        if (!gateAcquired)
        {
            // 过载时跳过本命令（无回执通道）。扫描结果可由扫描服务重放，
            // 落库的 state_version 条件更新保证旧结果不会覆盖新状态。
            _logger.LogWarning(
                "附件扫描处理过载，跳过命令。附件={AttachmentId}",
                command.AttachmentId);
            return;
        }

        try
        {
            var result = await _processor
                .ProcessAsync(command, ct)
                .ConfigureAwait(false);
            if (!result.Succeeded)
            {
                _logger.LogWarning(
                    "附件扫描未成功。附件={AttachmentId}；错误={ErrorCode}",
                    command.AttachmentId,
                    result.ErrorCode);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "附件扫描处理异常。附件={AttachmentId}",
                command.AttachmentId);
        }
        finally
        {
            _queryGate.Release(PoolKind);
        }
    }

    private async Task RunHeartbeatAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            _readinessState.MarkHeartbeat(WorkerName);
    }

    private static async Task SuppressCancellationAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }
}