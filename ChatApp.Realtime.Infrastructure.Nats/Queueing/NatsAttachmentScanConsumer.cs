using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

/// <summary>
/// 附件扫描消费者：从 NATS 订阅 <see cref="RealtimeQueueTopics.AttachmentScan"/>，
/// 反序列化为 <see cref="AttachmentScanCommand"/> 流。扫描结果不回执（fire-and-forget），
/// 由扫描服务/审核回调发布命令；落库的状态转换全部带 state_version 条件更新，
/// 旧结果或重复命令不会覆盖新状态。
/// </summary>
public sealed class NatsAttachmentScanConsumer : IAttachmentScanConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsAttachmentScanConsumer> _logger;

    public NatsAttachmentScanConsumer(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsAttachmentScanConsumer> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async IAsyncEnumerable<AttachmentScanCommand> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 附件扫描端点已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.AttachmentScan,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.AttachmentScan,
                           _options.ConsumerGroup,
                           cancellationToken: ct))
        {
            AttachmentScanCommand? command = null;
            try
            {
                msg.EnsureSuccess();
                if (!string.IsNullOrWhiteSpace(msg.Data))
                {
                    command = JsonSerializer.Deserialize(
                        msg.Data,
                        RealtimeJsonSerializerContext.Default.AttachmentScanCommand);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "NATS 附件扫描命令反序列化失败。Subject={Subject}",
                    msg.Subject);
            }

            if (command is null)
            {
                _logger.LogWarning(
                    "NATS 附件扫描命令负载为空或格式无效，已丢弃。Subject={Subject}",
                    msg.Subject);
                continue;
            }

            yield return command;
        }
    }
}