using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using ChatApp.Realtime.Infrastructure.Nats.Queueing;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamContextManager
{
    private readonly NatsConnectionClient _connectionClient;
    private readonly RealtimeQueueOptions _queueOptions;
    private readonly JetStreamOptions _options;
    private readonly ILogger<JetStreamContextManager> _logger;
    private readonly string? _shardSubjectPattern;
    private readonly Lazy<INatsJSContext> _context;
    private readonly ConcurrentDictionary<string, Task<INatsJSStream>> _streams =
        new(StringComparer.Ordinal);

    public JetStreamContextManager(
        NatsConnectionClient connectionClient,
        RealtimeQueueOptions queueOptions,
        JetStreamOptions options,
        ILogger<JetStreamContextManager> logger,
        string? shardSubjectPattern = null)
    {
        _connectionClient = connectionClient;
        _queueOptions = queueOptions;
        _options = options;
        _logger = logger;
        _shardSubjectPattern = string.IsNullOrWhiteSpace(shardSubjectPattern)
            ? null
            : shardSubjectPattern;
        _context = new Lazy<INatsJSContext>(CreateContext);
    }

    private INatsJSContext Context => _context.Value;

    public async Task<INatsJSConsumer> GetOrCreateIncomingMessagesConsumerAsync(
        CancellationToken ct = default)
    {
        var stream = await GetOrCreateStreamAsync(
            _options.Streams.IncomingMessages,
            [_queueOptions.Topics.IncomingMessages],
            _options.MaxAgeHours,
            ct).ConfigureAwait(false);
        return await CreateOrUpdateConsumerAsync(
            stream,
            _queueOptions.ConsumerGroup,
            _queueOptions.Topics.IncomingMessages,
            ct).ConfigureAwait(false);
    }

    public async Task<INatsJSConsumer> GetOrCreateMessageReceiptsConsumerAsync(
        CancellationToken ct = default)
    {
        var stream = await GetOrCreateStreamAsync(
            _options.Streams.MessageReceipts,
            [_queueOptions.Topics.MessageReceipts],
            _options.MaxAgeHours,
            ct).ConfigureAwait(false);
        return await CreateOrUpdateConsumerAsync(
            stream,
            $"{_queueOptions.ConsumerGroup}-receipts",
            _queueOptions.Topics.MessageReceipts,
            ct).ConfigureAwait(false);
    }

    public async Task EnsureStreamsAsync(CancellationToken ct = default)
    {
        await Task.WhenAll(
            GetOrCreateStreamAsync(
                _options.Streams.IncomingMessages,
                [_queueOptions.Topics.IncomingMessages],
                _options.MaxAgeHours,
                ct),
            GetOrCreateStreamAsync(
                _options.Streams.MessageReceipts,
                [_queueOptions.Topics.MessageReceipts],
                _options.MaxAgeHours,
                ct),
            GetOrCreateStreamAsync(
                _options.Streams.RealtimeEvents,
                BuildRealtimeEventsStreamSubjects(),
                _options.MaxAgeHours,
                ct),
            GetOrCreateStreamAsync(
                _options.Streams.DeadLetters,
                [_queueOptions.Topics.DeadLetters],
                _options.DeadLetterMaxAgeHours,
                ct)).ConfigureAwait(false);
    }

    public async Task<INatsJSConsumer> GetOrCreateRealtimeEventsConsumerAsync(
        CancellationToken ct = default)
    {
        var stream = await GetOrCreateStreamAsync(
            _options.Streams.RealtimeEvents,
            BuildRealtimeEventsStreamSubjects(),
            _options.MaxAgeHours,
            ct).ConfigureAwait(false);
        return await CreateOrUpdateConsumerAsync(
            stream,
            $"{_queueOptions.ConsumerGroup}-events",
            _queueOptions.Topics.RealtimeEvents,
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 账号清理专用 durable，仅订阅 AccountCleanup subject。
    /// </summary>
    public async Task<INatsJSConsumer> GetOrCreateAccountCleanupConsumerAsync(
        CancellationToken ct = default)
    {
        var stream = await GetOrCreateStreamAsync(
            _options.Streams.RealtimeEvents,
            BuildRealtimeEventsStreamSubjects(),
            _options.MaxAgeHours,
            ct).ConfigureAwait(false);
        return await CreateOrUpdateConsumerAsync(
            stream,
            $"{_queueOptions.ConsumerGroup}-account-cleanup",
            _queueOptions.Topics.AccountCleanup,
            ct).ConfigureAwait(false);
    }

    public async Task PublishRealtimeEventAsync(
        string eventId,
        string payload,
        CancellationToken ct = default)
        => await PublishToSubjectAsync(
            _queueOptions.Topics.RealtimeEvents,
            eventId,
            payload,
            ct).ConfigureAwait(false);

    /// <summary>
    /// P0-8：bytes 重载，避免 UTF-8 → UTF-16 string → UTF-8 的冗余编码转换。
    /// </summary>
    public async Task PublishRealtimeEventAsync(
        string eventId,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
        => await PublishToSubjectAsync(
            _queueOptions.Topics.RealtimeEvents,
            eventId,
            payload,
            ct).ConfigureAwait(false);

    /// <summary>
    /// 将实时事件发布到指定 subject（用于分片投递）。
    /// </summary>
    public async Task PublishRealtimeEventToSubjectAsync(
        string subject,
        string eventId,
        string payload,
        CancellationToken ct = default)
        => await PublishToSubjectAsync(subject, eventId, payload, ct)
            .ConfigureAwait(false);

    /// <summary>
    /// P0-8：bytes 重载，用于分片投递时直接传字节给 NATS。
    /// </summary>
    public async Task PublishRealtimeEventToSubjectAsync(
        string subject,
        string eventId,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
        => await PublishToSubjectAsync(subject, eventId, payload, ct)
            .ConfigureAwait(false);

    public async Task PublishAccountCleanupEventAsync(
        string eventId,
        string payload,
        CancellationToken ct = default)
        => await PublishToSubjectAsync(
            _queueOptions.Topics.AccountCleanup,
            eventId,
            payload,
            ct).ConfigureAwait(false);

    /// <summary>
    /// P0-8：bytes 重载，账号清理事件直接传字节。
    /// </summary>
    public async Task PublishAccountCleanupEventAsync(
        string eventId,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
        => await PublishToSubjectAsync(
            _queueOptions.Topics.AccountCleanup,
            eventId,
            payload,
            ct).ConfigureAwait(false);

    private async Task PublishToSubjectAsync(
        string subject,
        string eventId,
        string payload,
        CancellationToken ct)
    {
        await GetOrCreateStreamAsync(
            _options.Streams.RealtimeEvents,
            [_queueOptions.Topics.RealtimeEvents, _queueOptions.Topics.AccountCleanup],
            _options.MaxAgeHours,
            ct).ConfigureAwait(false);
        await Context.PublishAsync(
            subject,
            payload,
            opts: CreatePublishOptions(eventId),
            headers: NatsTraceContext.CreatePropagationHeaders(),
            cancellationToken: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// P0-8：bytes 路径，直接传 byte[] 给 NATS，避免 UTF-16 中间 string。
    /// </summary>
    private async Task PublishToSubjectAsync(
        string subject,
        string eventId,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        await GetOrCreateStreamAsync(
            _options.Streams.RealtimeEvents,
            [_queueOptions.Topics.RealtimeEvents, _queueOptions.Topics.AccountCleanup],
            _options.MaxAgeHours,
            ct).ConfigureAwait(false);
        // NATS 默认序列化器对 byte[] 使用 raw bytes 直写，无编码转换。
        await Context.PublishAsync(
            subject,
            payload.ToArray(),
            opts: CreatePublishOptions(eventId),
            headers: NatsTraceContext.CreatePropagationHeaders(),
            cancellationToken: ct).ConfigureAwait(false);
    }

    public async Task PublishDeadLetterAsync(
        string deadLetterId,
        string payload,
        CancellationToken ct = default)
    {
        await GetOrCreateStreamAsync(
            _options.Streams.DeadLetters,
            [_queueOptions.Topics.DeadLetters],
            _options.DeadLetterMaxAgeHours,
            ct).ConfigureAwait(false);
        await Context.PublishAsync(
            _queueOptions.Topics.DeadLetters,
            payload,
            opts: CreatePublishOptions(deadLetterId),
            cancellationToken: ct).ConfigureAwait(false);
    }

    private INatsJSContext CreateContext()
    {
        _logger.LogInformation("正在创建 JetStream 上下文。");
        return _connectionClient.Client.CreateJetStreamContext();
    }

    private async Task<INatsJSStream> GetOrCreateStreamAsync(
        string streamName,
        IReadOnlyCollection<string> subjects,
        int maxAgeHours,
        CancellationToken ct)
    {
        while (true)
        {
            var task = _streams.GetOrAdd(
                streamName,
                _ => CreateOrUpdateStreamAsync(
                    streamName,
                    subjects,
                    maxAgeHours));
            try
            {
                return await task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                _streams.TryRemove(
                    new KeyValuePair<string, Task<INatsJSStream>>(
                        streamName,
                        task));
                throw;
            }
        }
    }

    private async Task<INatsJSStream> CreateOrUpdateStreamAsync(
        string streamName,
        IReadOnlyCollection<string> subjects,
        int maxAgeHours)
    {
        _logger.LogInformation(
            "正在校准 JetStream 流配置。流名={Stream}；Subjects={Subjects}；副本={Replicas}",
            streamName,
            string.Join(",", subjects),
            _options.Replicas);
        var config = new StreamConfig(
            streamName,
            subjects.ToArray())
        {
            Storage = StreamConfigStorage.File,
            Retention = StreamConfigRetention.Limits,
            Discard = StreamConfigDiscard.Old,
            DuplicateWindow = TimeSpan.FromMinutes(
                _options.DuplicateWindowMinutes),
            MaxAge = TimeSpan.FromHours(maxAgeHours),
            MaxBytes = _options.MaxBytes,
            MaxMsgSize = _options.MaxMessageSize,
            NumReplicas = _options.Replicas
        };
        return await Context.CreateOrUpdateStreamAsync(
            config,
            CancellationToken.None).ConfigureAwait(false);
    }

    private async Task<INatsJSConsumer> CreateOrUpdateConsumerAsync(
        INatsJSStream stream,
        string consumerName,
        string filterSubject,
        CancellationToken ct)
    {
        var config = new ConsumerConfig(consumerName)
        {
            FilterSubject = filterSubject,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(
                _options.Consumer.AckWaitSeconds),
            MaxDeliver = _options.Consumer.MaxDeliver,
            MaxAckPending = _options.Consumer.MaxAckPending,
            Backoff = _options.Consumer.BackoffSeconds
                .Select(seconds => TimeSpan.FromSeconds(seconds))
                .ToArray(),
            DeliverPolicy = ConsumerConfigDeliverPolicy.All
        };
        return await stream.CreateOrUpdateConsumerAsync(
            config,
            ct).ConfigureAwait(false);
    }

    private static NatsJSPubOpts CreatePublishOptions(
        string messageId) => new()
    {
        MsgId = messageId,
        RetryAttempts = 3,
        RetryWaitBetweenAttempts = TimeSpan.FromMilliseconds(200)
    };

    private IReadOnlyCollection<string> BuildRealtimeEventsStreamSubjects()
    {
        var subjects = new List<string>(3)
        {
            _queueOptions.Topics.RealtimeEvents,
            _queueOptions.Topics.AccountCleanup
        };

        if (!string.IsNullOrWhiteSpace(_shardSubjectPattern))
        {
            subjects.Add(ShardedSubjectFormatter.ToWildcard(_shardSubjectPattern));
        }

        return subjects;
    }
}
