using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Infrastructure.Core.Serialization;

/// <summary>
/// RealtimeEvent 线格式序列化快路径。聊天 payload 保持现有 PayloadJson 字符串协议，
/// 但直接从对象写入 UTF-8，避免先创建 UTF-16 payload 字符串再序列化整个 envelope。
/// </summary>
public static class RealtimeEventWireSerializer
{
    public static byte[] SerializeToUtf8Bytes(RealtimeEvent evt)
    {
        if (evt.Payload is not RealtimeChatMessagePayload chatPayload)
        {
            return JsonSerializer.SerializeToUtf8Bytes(
                evt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent);
        }

        using var payloadBuffer = PooledByteBufferWriter.Rent(512);
        using (var payloadWriter = new Utf8JsonWriter(payloadBuffer.BufferWriter))
        {
            JsonSerializer.Serialize(
                payloadWriter,
                chatPayload,
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload);
        }

        using var envelopeBuffer = PooledByteBufferWriter.Rent(payloadBuffer.WrittenCount + 512);
        using (var writer = new Utf8JsonWriter(envelopeBuffer.BufferWriter))
        {
            writer.WriteStartObject();
            writer.WriteString(nameof(RealtimeEvent.EventId), evt.EventId);
            writer.WriteNumber(nameof(RealtimeEvent.Type), (int)evt.Type);
            writer.WriteNumber(nameof(RealtimeEvent.TargetUserId), evt.TargetUserId);
            WriteNullableNumber(writer, nameof(RealtimeEvent.ActorUserId), evt.ActorUserId);
            WriteNullableString(writer, nameof(RealtimeEvent.MessageId), evt.MessageId);
            WriteNullableString(writer, nameof(RealtimeEvent.SessionId), evt.SessionId);
            writer.WriteString(nameof(RealtimeEvent.PayloadJson), payloadBuffer.WrittenSpan);
            WriteNullableString(writer, nameof(RealtimeEvent.TraceParent), evt.TraceParent);
            WriteNullableString(writer, nameof(RealtimeEvent.TraceState), evt.TraceState);
            writer.WriteNumber(nameof(RealtimeEvent.OccurredAtMs), evt.OccurredAtMs);
            WriteNullableLongArray(writer, nameof(RealtimeEvent.TargetUserIds), evt.TargetUserIds);
            WriteNullableNumber(writer, nameof(RealtimeEvent.AudienceKind), evt.AudienceKind is null ? null : (int)evt.AudienceKind.Value);
            WriteNullableString(writer, nameof(RealtimeEvent.ConversationId), evt.ConversationId);
            WriteNullableNumber(writer, nameof(RealtimeEvent.ExcludeUserId), evt.ExcludeUserId);
            WriteNullableNumber(writer, nameof(RealtimeEvent.ProtocolVersion), evt.ProtocolVersion);
            WriteNullableNumber(writer, nameof(RealtimeEvent.AudienceVersion), evt.AudienceVersion);
            WriteNullableNumber(writer, nameof(RealtimeEvent.MinProtocolVersion), evt.MinProtocolVersion);
            writer.WriteEndObject();
        }

        return envelopeBuffer.ToArray();
    }

    private static void WriteNullableString(Utf8JsonWriter writer, string name, string? value)
    {
        if (value is null)
            writer.WriteNull(name);
        else
            writer.WriteString(name, value);
    }

    private static void WriteNullableNumber(Utf8JsonWriter writer, string name, long? value)
    {
        if (value.HasValue)
            writer.WriteNumber(name, value.Value);
        else
            writer.WriteNull(name);
    }

    private static void WriteNullableNumber(Utf8JsonWriter writer, string name, int? value)
    {
        if (value.HasValue)
            writer.WriteNumber(name, value.Value);
        else
            writer.WriteNull(name);
    }

    private static void WriteNullableLongArray(Utf8JsonWriter writer, string name, long[]? values)
    {
        if (values is null)
        {
            writer.WriteNull(name);
            return;
        }

        writer.WriteStartArray(name);
        for (var i = 0; i < values.Length; i++)
            writer.WriteNumberValue(values[i]);
        writer.WriteEndArray();
    }
}
