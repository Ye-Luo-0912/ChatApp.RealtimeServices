using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 通话信令命令工作线程：消费 Core NATS 零持久化 subject 上的通话命令，
/// 交由 <see cref="ICallControlProcessor"/> 驱动状态机，并把结果回给调用方。
/// <para>
/// 通话命令不进入 JetStream/PostgreSQL/Outbox——消费即处理，失败即 NACK（一次性）。
/// </para>
/// </summary>
public sealed class CallControlWorker : BackgroundService
{
    private const string WorkerName = nameof(CallControlWorker);
    private readonly ICallControlConsumer _consumer;
    private readonly ICallControlProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly ILogger<CallControlWorker> _logger;

    public CallControlWorker(
        ICallControlConsumer consumer,
        ICallControlProcessor processor,
        RealtimeReadinessState readinessState,
        RealtimeNatsTrustSettings trust,
        ILogger<CallControlWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _readinessState = readinessState;
        _trust = trust;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("通话信令命令工作线程启动");
        _readinessState.MarkStarted(WorkerName);

        var channel = Channel.CreateUnbounded<CallCommandEnvelope>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = true,
                AllowSynchronousContinuations = false
            });
        var processor = ProcessAsync(channel.Reader, stoppingToken);

        try
        {
            await ProduceAsync(channel.Writer, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("通话信令命令消费循环已取消");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "通话信令命令消费循环异常");
            throw;
        }
        finally
        {
            channel.Writer.TryComplete();
            try
            {
                await processor.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }

            _readinessState.MarkStopped(WorkerName);
        }
    }

    private async Task ProduceAsync(
        ChannelWriter<CallCommandEnvelope> writer,
        CancellationToken ct)
    {
        var retryAttempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var envelope in _consumer.ConsumeAsync(ct).ConfigureAwait(false))
                {
                    retryAttempt = 0;
                    _readinessState.MarkHeartbeat(WorkerName);
                    await writer.WriteAsync(envelope, ct).ConfigureAwait(false);
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
                _logger.LogWarning(ex, "通话信令命令消费异常，重试延迟={Delay}", delay);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }

            // Core NATS 消费循环在订阅被取消（连接断开）时结束，需重连重订阅。
            await Task.Delay(TimeSpan.FromMilliseconds(100), ct).ConfigureAwait(false);
        }
    }

    private async Task ProcessAsync(
        ChannelReader<CallCommandEnvelope> reader,
        CancellationToken ct)
    {
        await foreach (var envelope in reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            CallProcessResult result;
            try
            {
                var identityError = NatsGatewayIdentity.ValidateHistoryUser(
                    _trust.RequireGatewayIdentity,
                    envelope.TrustedUserId,
                    envelope.Command.ActorUserId);
                if (identityError is not null)
                {
                    result = CallProcessResult.Failed(
                        CallErrorCode.GrantInvalid,
                        "网关身份校验失败，通话信令命令被拒绝。");
                }
                else
                {
                    result = await _processor.ProcessAsync(envelope.Command, ct).ConfigureAwait(false);
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
                    "通话信令命令处理异常：命令={CommandId}；通话={CallId}",
                    envelope.Command.CommandId,
                    envelope.Command.CallId);
                result = CallProcessResult.Failed(
                    CallErrorCode.StateStoreUnavailable,
                    "通话信令命令处理失败，请稍后重试。");
            }

            try
            {
                await envelope.ReplyAsync(result, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "通话信令命令响应发送失败：命令={CommandId}；通话={CallId}",
                    envelope.Command.CommandId,
                    envelope.Command.CallId);
            }

            _readinessState.MarkHeartbeat(WorkerName);
        }
    }
}