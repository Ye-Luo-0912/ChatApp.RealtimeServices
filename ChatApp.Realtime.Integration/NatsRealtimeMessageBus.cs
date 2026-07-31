using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.Realtime.Integration.JetStream;
using ChatApp.Realtime.Integration.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Integration;

/// <summary>
/// 基于 NATS JetStream / NATS Core 的 <see cref="IRealtimeMessageBus"/> 实现。
/// <para>
/// 该类作为 facade，将职责委托给以下内部组件：
/// <list type="bullet">
/// <item><see cref="NatsConnectionProvider"/>：连接管理与事件订阅。</item>
/// <item><see cref="JetStreamTopologyManager"/>：JetStream 流拓扑与持久化发布。</item>
/// <item><see cref="NatsDeadLetterPublisher"/>：死信发布。</item>
/// <item><see cref="RealtimeCommandPublisher"/>：入站消息 / 回执 / Realtime Event 发布。</item>
/// <item><see cref="RealtimeRequestClient"/>：request/reply 查询与变更。</item>
/// <item><see cref="RealtimeEventSubscriber"/>：Realtime Event JetStream 消费。</item>
/// <item><see cref="EphemeralEventBus"/>：Typing / Presence 临时事件与 Presence 鉴权服务。</item>
/// </list>
/// </para>
/// </summary>
public sealed class NatsRealtimeMessageBus : IRealtimeMessageBus, IAsyncDisposable
{
    private readonly NatsTransportMetrics _metrics;
    private readonly bool _ownsMetrics;
    private readonly NatsConnectionProvider _connectionProvider;
    private readonly JetStreamTopologyManager _topology;
    private readonly NatsDeadLetterPublisher _deadLetterPublisher;
    private readonly RealtimeCommandPublisher _commandPublisher;
    private readonly RealtimeRequestClient _requestClient;
    private readonly RealtimeEventSubscriber _eventSubscriber;
    private readonly EphemeralEventBus _ephemeralEventBus;

    public NatsRealtimeMessageBus(
        RealtimeIntegrationOptions options,
        ILogger<NatsRealtimeMessageBus> logger)
        : this(
            options,
            logger,
            new NatsTransportMetrics(RealtimeIntegrationTelemetry.ActivitySourceName),
            gatewayDirectory: NullGatewayDirectory.Instance,
            watcherGatewayDirectory: NullWatcherGatewayDirectory.Instance,
            ownsMetrics: true)
    {
    }

    public NatsRealtimeMessageBus(
        RealtimeIntegrationOptions options,
        ILogger<NatsRealtimeMessageBus> logger,
        NatsTransportMetrics metrics)
        : this(options, logger, metrics, NullGatewayDirectory.Instance, NullWatcherGatewayDirectory.Instance, ownsMetrics: false)
    {
    }

    public NatsRealtimeMessageBus(
        RealtimeIntegrationOptions options,
        ILogger<NatsRealtimeMessageBus> logger,
        NatsTransportMetrics metrics,
        IGatewayDirectory gatewayDirectory)
        : this(options, logger, metrics, gatewayDirectory, NullWatcherGatewayDirectory.Instance, ownsMetrics: false)
    {
    }

    public NatsRealtimeMessageBus(
        RealtimeIntegrationOptions options,
        ILogger<NatsRealtimeMessageBus> logger,
        NatsTransportMetrics metrics,
        IGatewayDirectory gatewayDirectory,
        IWatcherGatewayDirectory watcherGatewayDirectory,
        RoutingMetrics? routingMetrics)
        : this(options, logger, metrics, gatewayDirectory, watcherGatewayDirectory, ownsMetrics: false, routingMetrics)
    {
    }

    private NatsRealtimeMessageBus(
        RealtimeIntegrationOptions options,
        ILogger<NatsRealtimeMessageBus> logger,
        NatsTransportMetrics metrics,
        IGatewayDirectory gatewayDirectory,
        IWatcherGatewayDirectory watcherGatewayDirectory,
        bool ownsMetrics,
        RoutingMetrics? routingMetrics = null)
    {
        _metrics = metrics;
        _ownsMetrics = ownsMetrics;

        _connectionProvider = new NatsConnectionProvider(options, metrics, logger);
        _topology = new JetStreamTopologyManager(_connectionProvider, options, logger);
        _deadLetterPublisher = new NatsDeadLetterPublisher(_topology, options);
        _commandPublisher = new RealtimeCommandPublisher(
            _topology,
            options,
            gatewayDirectory ?? NullGatewayDirectory.Instance,
            routingMetrics);
        _requestClient = new RealtimeRequestClient(_connectionProvider, options);
        _eventSubscriber = new RealtimeEventSubscriber(
            _topology,
            _deadLetterPublisher,
            metrics,
            options);
        _ephemeralEventBus = new EphemeralEventBus(
            _connectionProvider,
            options,
            gatewayDirectory ?? NullGatewayDirectory.Instance,
            watcherGatewayDirectory ?? NullWatcherGatewayDirectory.Instance,
            routingMetrics,
            logger);
    }

    public Task PublishIncomingMessageAsync(
        IncomingMessageCommand command,
        CancellationToken ct = default)
        => _commandPublisher.PublishIncomingMessageAsync(command, ct);

    public Task PublishMessageReceiptAsync(
        MessageReceiptCommand command,
        CancellationToken ct = default)
        => _commandPublisher.PublishMessageReceiptAsync(command, ct);

    public Task<MessageHistoryPage> QueryMessageHistoryAsync(
        MessageHistoryQuery query,
        CancellationToken ct = default)
        => _requestClient.QueryMessageHistoryAsync(query, ct);

    public Task<ConversationListPage> QueryConversationListAsync(
        ConversationListQuery query,
        CancellationToken ct = default)
        => _requestClient.QueryConversationListAsync(query, ct);

    public Task<ConversationMarkReadResult> MarkConversationReadAsync(
        ConversationMarkReadCommand command,
        CancellationToken ct = default)
        => _requestClient.MarkConversationReadAsync(command, ct);

    public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(
        ConversationSetPrefsCommand command,
        CancellationToken ct = default)
        => _requestClient.SetConversationPrefsAsync(command, ct);

    public Task<GroupConversationResult> MutateGroupConversationAsync(
        GroupConversationCommand command,
        CancellationToken ct = default)
        => _requestClient.MutateGroupConversationAsync(command, ct);

    public Task<MessageRecallResult> RecallMessageAsync(
        MessageRecallCommand command,
        CancellationToken ct = default)
        => _requestClient.RecallMessageAsync(command, ct);

    public Task<MessageEditResult> EditMessageAsync(
        MessageEditCommand command,
        CancellationToken ct = default)
        => _requestClient.EditMessageAsync(command, ct);

    public Task<MessageReactionResult> ReactToMessageAsync(
        MessageReactionCommand command,
        CancellationToken ct = default)
        => _requestClient.ReactToMessageAsync(command, ct);

    public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
        SyncBootstrapQuery query,
        CancellationToken ct = default)
        => _requestClient.QuerySyncBootstrapAsync(query, ct);

    public Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(
        long userId,
        string messageId,
        CancellationToken ct = default)
        => _requestClient.TryGetMessageByIdAsync(userId, messageId, ct);

    public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default)
        => _commandPublisher.PublishEventAsync(evt, ct);

    public IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(
        CancellationToken ct = default)
        => _eventSubscriber.ConsumeEventsAsync(ct);

    public IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync(
        CancellationToken ct = default)
        => _eventSubscriber.ConsumeAccountCleanupEventsAsync(ct);

    public Task PublishPushDeliveryAsync(PushDeliveryCommand command, CancellationToken ct = default)
        => _commandPublisher.PublishPushDeliveryAsync(command, ct);

    public IAsyncEnumerable<PushDelivery> ConsumePushDeliveriesAsync(CancellationToken ct = default)
        => _eventSubscriber.ConsumePushDeliveriesAsync(ct);

    public Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default)
        => _ephemeralEventBus.PublishEphemeralTypingAsync(evt, ct);

    public Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default)
        => _ephemeralEventBus.PublishEphemeralPresenceAsync(evt, ct);

    public IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync(
        CancellationToken ct = default)
        => _ephemeralEventBus.ConsumeEphemeralTypingAsync(ct);

    public IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync(
        CancellationToken ct = default)
        => _ephemeralEventBus.ConsumeEphemeralPresenceAsync(ct);

    public Task<PresenceAuthorizeResponse> AuthorizePresenceAsync(
        PresenceAuthorizeQuery query,
        CancellationToken ct = default)
        => _requestClient.AuthorizePresenceAsync(query, ct);

    public Task ServePresenceAuthorizeAsync(
        Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler,
        CancellationToken ct = default)
        => _ephemeralEventBus.ServePresenceAuthorizeAsync(handler, ct);

    public Task<TimeSpan> PingAsync(CancellationToken ct = default)
        => _connectionProvider.PingAsync(ct);

    public async ValueTask DisposeAsync()
    {
        await _connectionProvider.DisposeAsync().ConfigureAwait(false);

        if (_ownsMetrics)
            _metrics.Dispose();
    }
}
