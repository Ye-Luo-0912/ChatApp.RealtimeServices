using System.Security.Cryptography;
using System.Text;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class DeadLetterMessage
{
    /// <summary>
    /// JetStream 单条消息默认上限 1 MiB。DLQ 会把原始 Payload 嵌入 JSON 包装，
    /// 截断到 768 KiB 为包装字段和 JSON 转义预留 ~256 KiB 余量。
    /// </summary>
    public const int DefaultMaxPayloadBytes = 768 * 1024;

    public required string DeadLetterId { get; init; }
    public string? CommandId { get; init; }
    public required string SourceSubject { get; init; }
    public required string ReasonCode { get; init; }
    public required string Reason { get; init; }
    public string? Payload { get; init; }
    public ulong? DeliveryCount { get; init; }
    public long FailedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>Reliability-5：原始 Payload 的 SHA-256（小写 hex）。仅截断时非空，用于对账与去重。</summary>
    public string? PayloadSha256 { get; init; }

    /// <summary>Reliability-5：原始 Payload 的 UTF-8 字节长度。仅截断时非空。</summary>
    public int? OriginalPayloadLength { get; init; }

    /// <summary>Reliability-5：Payload 是否被截断以适应 JetStream 单消息上限。</summary>
    public bool PayloadTruncated { get; init; }

    /// <summary>
    /// Reliability-5：返回 Payload 被截断到 <paramref name="maxPayloadBytes"/> 的副本。
    /// <para>
    /// 原始 Payload 未超限时返回原对象；超限时截断到合法 UTF-8 边界，
    /// 并记录 SHA-256、原始字节长度、截断标记，便于后续对账或对象存储回填。
    /// </para>
    /// </summary>
    public DeadLetterMessage WithBoundedPayload(int maxPayloadBytes = DefaultMaxPayloadBytes)
    {
        if (string.IsNullOrEmpty(Payload))
            return this;

        var payloadBytes = Encoding.UTF8.GetBytes(Payload);
        if (payloadBytes.Length <= maxPayloadBytes)
            return this;

        // 回退到最后一个合法 UTF-8 字符边界，避免切断多字节序列。
        var end = maxPayloadBytes;
        while (end > 0 && (payloadBytes[end] & 0xC0) == 0x80)
            end--;

        var truncated = Encoding.UTF8.GetString(payloadBytes, 0, end);
        var sha256 = Convert.ToHexString(SHA256.HashData(payloadBytes)).ToLowerInvariant();

        return new DeadLetterMessage
        {
            DeadLetterId = DeadLetterId,
            CommandId = CommandId,
            SourceSubject = SourceSubject,
            ReasonCode = ReasonCode,
            Reason = Reason,
            Payload = truncated,
            DeliveryCount = DeliveryCount,
            FailedAtMs = FailedAtMs,
            PayloadSha256 = sha256,
            OriginalPayloadLength = payloadBytes.Length,
            PayloadTruncated = true
        };
    }
}
