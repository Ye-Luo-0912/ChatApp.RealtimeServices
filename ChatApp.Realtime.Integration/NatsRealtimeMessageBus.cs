using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Serialization;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ChatApp.Realtime.Integration;

public sealed class NatsRealtimeMessageBus : IRealtimeMessageBus, IAsyncDisposable
{
    private readonly RealtimeIntegrationOptions _options;
    private readonly NatsTransportMetrics _metrics;
    private readonly bool _ownsMetrics;
    private readonly ILogger<NatsRealtimeMessageBus> _logger;
    private readonly Lazy<NatsConnection> _client;
    private readonly Lazy<INatsJSContext> _context;
    private readonly ConcurrentDictionary<string, Task<INatsJSStream>> _streams = new(StringComparer.Ordinal);

    public NatsRealtimeMessageBus(
        RealtimeIntegrationOptions options,
        ILogger<NatsRealtimeMessageBus> logger)
        : this(
            options,
            logger,
            new NatsTransportMetrics(RealtimeIntegrationTelemetry.ActivitySourceName),
            ownsMetrics: true)
    {
    }

    public NatsRealtimeMessageBus(
        RealtimeIntegrationOptions options,
        ILogger<NatsRealtimeMessageBus> logger,
        NatsTransportMetrics metrics)
        : this(options, logger, metrics, ownsMetrics: false)
    {
    }

    private NatsRealtimeMessageBus(
        RealtimeIntegrationOptions options,
        ILogger<NatsRealtimeMessageBus> logger,
        NatsTransportMetrics metrics,
        bool ownsMetrics)
    {
        _options = options;
        _logger = logger;
        _metrics = metrics;
        _ownsMetrics = ownsMetrics;
        _client = new Lazy<NatsConnection>(CreateClient);
        _context = new Lazy<INatsJSContext>(() => new NatsJSContext(_client.Value));
    }

    public async Task PublishIncomingMessageAsync(
        IncomingMessageCommand command,
        CancellationToken ct = default)
    {
        using var activity = RealtimeIntegrationTelemetry.StartProducer(
            "incoming_message.publish",
            _options.IncomingMessagesSubject);
        try
        {
            await EnsureStreamAsync(
                _options.IncomingMessagesStream,
                _options.IncomingMessagesSubject,
                _options.MaxAgeHours,
                ct).ConfigureAwait(false);
            await PublishJetStreamWithReconnectRetryAsync(
                _options.IncomingMessagesSubject,
                RealtimeWireSerializer.Serialize(command),
                CreateMessageId(command.SenderUserId, command.ClientMessageId),
                RealtimeIntegrationTelemetry.CreatePropagationHeaders(),
                ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task PublishMessageReceiptAsync(
        MessageReceiptCommand command,
        CancellationToken ct = default)
    {
        using var activity = RealtimeIntegrationTelemetry.StartProducer(
            "message_receipt.publish",
            _options.MessageReceiptsSubject);
        try
        {
            await EnsureStreamAsync(
                _options.MessageReceiptsStream,
                _options.MessageReceiptsSubject,
                _options.MaxAgeHours,
                ct).ConfigureAwait(false);
            await PublishJetStreamWithReconnectRetryAsync(
                _options.MessageReceiptsSubject,
                RealtimeWireSerializer.Serialize(command),
                CreateMessageId(command.ReceiverUserId, command.CommandId),
                RealtimeIntegrationTelemetry.CreatePropagationHeaders(),
                ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task<MessageHistoryPage> QueryMessageHistoryAsync(
        MessageHistoryQuery query,
        CancellationToken ct = default)
    {
        using var activity = RealtimeIntegrationTelemetry.StartClient(
            "message_history.request",
            _options.MessageHistoryQueriesSubject);
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromMilliseconds(
                _options.HistoryRequestTimeoutMs));

            var response = await _client.Value.RequestAsync<string, string>(
                    _options.MessageHistoryQueriesSubject,
                    RealtimeWireSerializer.Serialize(query),
                    headers: RealtimeIntegrationTelemetry.CreatePropagationHeaders(),
                    cancellationToken: timeout.Token)
                .ConfigureAwait(false);
            response.EnsureSuccess();

            if (string.IsNullOrWhiteSpace(response.Data))
                throw new JsonException("历史消息查询返回了空响应。");

            return RealtimeWireSerializer.DeserializeMessageHistoryPage(response.Data)
                   ?? throw new JsonException("历史消息查询响应无法反序列化。");
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(
        long userId,
        string messageId,
        CancellationToken ct = default)
    {
        var page = await QueryMessageHistoryAsync(
                new MessageHistoryQuery
                {
                    RequestId = Guid.NewGuid().ToString("N")[..16],
                    UserId = userId,
                    MessageId = messageId,
                    Limit = 1,
                },
                ct)
            .ConfigureAwait(false);

        if (!page.Succeeded || page.Items.Count == 0)
            return null;
        return page.Items[0];
    }

    public async Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        var subject = evt.Type is RealtimeEventType.UserAccountDeleted or RealtimeEventType.AccountCleanupCompleted
            ? _options.AccountCleanupSubject
            : _options.RealtimeEventsSubject;
        using var activity = RealtimeIntegrationTelemetry.StartProducer(
            "realtime_event.publish",
            subject);
        try
        {
            await EnsureStreamAsync(
                _options.RealtimeEventsStream,
                subject,
                _options.MaxAgeHours,
                ct).ConfigureAwait(false);
            await PublishJetStreamWithReconnectRetryAsync(
                subject,
                RealtimeWireSerializer.Serialize(evt),
                evt.EventId,
                RealtimeIntegrationTelemetry.CreatePropagationHeaders(),
                ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }
    public IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(
        CancellationToken ct = default) => ConsumeEventSubjectAsync(
            _options.RealtimeEventsSubject,
            CreateConsumerName(_options.GatewayConsumerPrefix, _options.InstanceId),
            ct);

    public IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync(
        CancellationToken ct = default)
    {
        var consumerName = string.IsNullOrWhiteSpace(_options.AccountCleanupConsumerName)
            ? CreateConsumerName(_options.GatewayConsumerPrefix, _options.InstanceId)
            : NormalizeConsumerName(_options.AccountCleanupConsumerName);
        return ConsumeEventSubjectAsync(_options.AccountCleanupSubject, consumerName, ct);
    }

    private async IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventSubjectAsync(
        string subject,
        string consumerName,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var stream = await EnsureStreamAsync(
            _options.RealtimeEventsStream,
            subject,
            _options.MaxAgeHours,
            ct).ConfigureAwait(false);
        var consumer = await stream.CreateOrUpdateConsumerAsync(
            new ConsumerConfig(consumerName)
            {
                FilterSubject = subject,
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = TimeSpan.FromSeconds(_options.AckWaitSeconds),
                MaxDeliver = _options.MaxDeliver,
                MaxAckPending = _options.MaxAckPending,
                Backoff = _options.BackoffSeconds.Select(seconds => TimeSpan.FromSeconds(seconds)).ToArray(),
                DeliverPolicy = _options.ReplayRetainedEventsOnConsumerCreation
                    ? ConsumerConfigDeliverPolicy.All
                    : ConsumerConfigDeliverPolicy.New
            },
            ct).ConfigureAwait(false);
        var consumeOptions = new NatsJSConsumeOpts
        {
            MaxMsgs = Math.Max(1, _options.MaxAckPending),
            Expires = TimeSpan.FromSeconds(Math.Max(5, _options.AckWaitSeconds)),
            IdleHeartbeat = TimeSpan.FromSeconds(10),
            ThresholdMsgs = Math.Max(1, _options.MaxAckPending / 2)
        };

        while (!ct.IsCancellationRequested)
        {
            await foreach (var msg in consumer
                               .ConsumeAsync<string>(
                                   opts: consumeOptions,
                                   cancellationToken: ct)
                               .ConfigureAwait(false))
            {
                var observation = IntegrationJetStreamMetricAck.Observe(
                    _metrics,
                    msg.Metadata,
                    consumerName);
                RealtimeEvent? evt;
                try
                {
                    evt = string.IsNullOrWhiteSpace(msg.Data)
                        ? null
                        : RealtimeWireSerializer.DeserializeEvent(msg.Data);
                    if (evt is null)
                        throw new JsonException("实时事件负载为空或无法反序列化。");
                }
                catch (JsonException ex)
                {
                    await PublishDeadLetterAsync(
                        new DeadLetterMessage
                        {
                            DeadLetterId = $"gateway-event-{msg.Metadata?.Sequence.Stream ?? 0}-invalid-json",
                            SourceSubject = msg.Subject,
                            ReasonCode = "invalid_event_json",
                            Reason = ex.Message,
                            Payload = msg.Data,
                            DeliveryCount = msg.Metadata?.NumDelivered
                        },
                        ct).ConfigureAwait(false);
                    await IntegrationJetStreamMetricAck.TerminateAsync(
                        msg,
                        _metrics,
                        observation,
                        "invalid_event_json",
                        ct).ConfigureAwait(false);
                    continue;
                }

                var jsMsg = msg;
                yield return new RealtimeEventDelivery(
                    evt,
                    ack: ackCt => IntegrationJetStreamMetricAck.AckAsync(
                        jsMsg, _metrics, observation, ackCt),
                    nak: (delay, nakCt) => IntegrationJetStreamMetricAck.NakAsync(
                        jsMsg, _metrics, observation, delay, nakCt),
                    deliveryCount: msg.Metadata?.NumDelivered,
                    parentContext: RealtimeIntegrationTelemetry.ExtractParentContext(msg.Headers));
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), ct)
                .ConfigureAwait(false);
        }
    }
    public async Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
        await _client.Value.PingAsync(ct).ConfigureAwait(false);

    private async Task PublishJetStreamWithReconnectRetryAsync(
        string subject,
        string payload,
        string messageId,
        NatsHeaders? headers,
        CancellationToken ct)
    {
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await _context.Value.PublishAsync(
                    subject,
                    payload,
                    opts: CreatePublishOptions(messageId),
                    headers: headers,
                    cancellationToken: ct).ConfigureAwait(false);
                return;
            }
            catch (NatsJSPublishNoResponseException ex) when (!ct.IsCancellationRequested)
            {
                attempt++;
                var delay = TimeSpan.FromMilliseconds(
                    Math.Min(2_000, 250 * Math.Pow(2, Math.Min(attempt - 1, 3))) +
                    Random.Shared.Next(0, 250));
                _logger.LogWarning(
                    ex,
                    "JetStream 发布未收到响应，将使用相同 MsgId 重试。Subject={Subject}；尝试={Attempt}；延迟={Delay}",
                    subject,
                    attempt,
                    delay);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
    }
    private async Task PublishDeadLetterAsync(DeadLetterMessage message, CancellationToken ct)
    {
        await EnsureStreamAsync(
            _options.DeadLettersStream,
            _options.DeadLettersSubject,
            _options.DeadLetterMaxAgeHours,
            ct).ConfigureAwait(false);
        await _context.Value.PublishAsync(
            _options.DeadLettersSubject,
            RealtimeWireSerializer.Serialize(message),
            opts: CreatePublishOptions(message.DeadLetterId),
            cancellationToken: ct).ConfigureAwait(false);
    }

    private async Task<INatsJSStream> EnsureStreamAsync(
        string streamName,
        string subject,
        int maxAgeHours,
        CancellationToken ct)
    {
        while (true)
        {
            var task = _streams.GetOrAdd(streamName, _ => CreateOrUpdateStreamAsync(
                streamName,
                subject,
                maxAgeHours));
            try
            {
                return await task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                _streams.TryRemove(new KeyValuePair<string, Task<INatsJSStream>>(streamName, task));
                throw;
            }
        }
    }

    private async Task<INatsJSStream> CreateOrUpdateStreamAsync(
        string streamName,
        string subject,
        int maxAgeHours)
    {
        if (!_options.ManageStreams)
        {
            return await _context.Value.GetStreamAsync(
                streamName,
                new StreamInfoRequest(),
                CancellationToken.None).ConfigureAwait(false);
        }

        var subjects = streamName.Equals(_options.RealtimeEventsStream, StringComparison.Ordinal)
            ? new[] { _options.RealtimeEventsSubject, _options.AccountCleanupSubject }
            : new[] { subject };
        var config = new StreamConfig(streamName, subjects)
        {
            Storage = StreamConfigStorage.File,
            Retention = StreamConfigRetention.Limits,
            Discard = StreamConfigDiscard.Old,
            DuplicateWindow = TimeSpan.FromMinutes(_options.DuplicateWindowMinutes),
            MaxAge = TimeSpan.FromHours(maxAgeHours),
            MaxBytes = _options.MaxBytes,
            MaxMsgSize = _options.MaxMessageSize,
            NumReplicas = _options.Replicas
        };
        return await _context.Value.CreateOrUpdateStreamAsync(config, CancellationToken.None).ConfigureAwait(false);
    }

    private NatsConnection CreateClient()
    {
        _logger.LogInformation("创建实时集成 NATS 客户端。Client={Client}; Url={Url}", _options.ClientName, _options.Url);
        var connection = new NatsConnection(new NatsOpts
        {
            Url = _options.Url,
            Name = $"{_options.ClientName}-{_options.InstanceId}",
            ConnectTimeout = TimeSpan.FromSeconds(5),
            RequestTimeout = TimeSpan.FromSeconds(5),
            CommandTimeout = TimeSpan.FromSeconds(5),
            PingInterval = TimeSpan.FromSeconds(20),
            MaxPingOut = 2,
            MaxReconnectRetry = -1,
            ReconnectWaitMin = TimeSpan.FromMilliseconds(500),
            ReconnectWaitMax = TimeSpan.FromSeconds(5),
            ReconnectJitter = TimeSpan.FromMilliseconds(500),
            PublishTimeoutOnDisconnected = false
        });
        Subscribe(connection);
        return connection;
    }

    private void Subscribe(INatsConnection connection)
    {
        connection.ConnectionOpened += OnConnectionOpenedAsync;
        connection.ConnectionDisconnected += OnConnectionDisconnectedAsync;
        connection.ReconnectFailed += OnReconnectFailedAsync;
        connection.MessageDropped += OnMessageDroppedAsync;
        connection.SlowConsumerDetected += OnSlowConsumerDetectedAsync;
        connection.ServerError += OnServerErrorAsync;
    }

    private void Unsubscribe(INatsConnection connection)
    {
        connection.ConnectionOpened -= OnConnectionOpenedAsync;
        connection.ConnectionDisconnected -= OnConnectionDisconnectedAsync;
        connection.ReconnectFailed -= OnReconnectFailedAsync;
        connection.MessageDropped -= OnMessageDroppedAsync;
        connection.SlowConsumerDetected -= OnSlowConsumerDetectedAsync;
        connection.ServerError -= OnServerErrorAsync;
    }

    private ValueTask OnConnectionOpenedAsync(object? sender, NatsEventArgs args)
    {
        _metrics.RecordConnectionOpened();
        _logger.LogInformation("实时集成 NATS 连接已建立。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnConnectionDisconnectedAsync(object? sender, NatsEventArgs args)
    {
        _metrics.RecordConnectionDisconnected();
        _logger.LogWarning("实时集成 NATS 连接已断开，将自动重连。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnReconnectFailedAsync(object? sender, NatsEventArgs args)
    {
        _metrics.RecordReconnectFailure();
        _logger.LogDebug("实时集成 NATS 重连尝试失败。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnMessageDroppedAsync(object? sender, NatsMessageDroppedEventArgs args)
    {
        _metrics.RecordMessageDropped(args.Subject, args.Pending);
        _logger.LogError(
            "实时集成 NATS 本地订阅消息被丢弃。Subject={Subject}；Pending={Pending}",
            args.Subject,
            args.Pending);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnSlowConsumerDetectedAsync(object? sender, NatsSlowConsumerEventArgs args)
    {
        _metrics.RecordSlowConsumer();
        _logger.LogWarning("实时集成 NATS 检测到慢消费者。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnServerErrorAsync(object? sender, NatsServerErrorEventArgs args)
    {
        _metrics.RecordServerError(args.Kind.ToString());
        _logger.LogWarning(
            "实时集成 NATS 服务端返回错误。类型={Kind}；错误={Error}",
            args.Kind,
            args.Error);
        return ValueTask.CompletedTask;
    }

    private static NatsJSPubOpts CreatePublishOptions(string messageId) => new()
    {
        MsgId = messageId,
        RetryAttempts = 3,
        RetryWaitBetweenAttempts = TimeSpan.FromMilliseconds(200)
    };

    private static string CreateMessageId(long senderUserId, string clientMessageId)
    {
        var bytes = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(SHA256.HashData(bytes));
    }

    private static string CreateConsumerName(string prefix, string instanceId)
        => NormalizeConsumerName($"{prefix}-{instanceId}");

    private static string NormalizeConsumerName(string raw)
    {
        var normalized = new string(raw.Select(character =>
            char.IsLetterOrDigit(character) || character is '-' or '_' ? character : '-').ToArray());
        return normalized.Length <= 128 ? normalized : normalized[..128];
    }

    public async ValueTask DisposeAsync()
    {
        if (_client.IsValueCreated)
        {
            Unsubscribe(_client.Value);
            await _client.Value.DisposeAsync().ConfigureAwait(false);
        }

        if (_ownsMetrics)
            _metrics.Dispose();
    }
}
