namespace ChatApp.Realtime.Abstractions.Stores;

public sealed class RealtimeAttachmentRecord
{
    public required string AttachmentId { get; init; }
    public required long UploaderUserId { get; init; }
    public required string ObjectKey { get; init; }
    public string? PublicUrl { get; init; }
    public required string ContentType { get; init; }
    public required long SizeBytes { get; init; }
    public string? OriginalName { get; init; }
    public required AttachmentStatus Status { get; init; }
    public string? MessageId { get; init; }
    public string? ConversationId { get; init; }
    public string? ClientAttachmentId { get; init; }
    public long CreatedAtMs { get; init; }
    public long? ConfirmedAtMs { get; init; }
    public long? BoundAtMs { get; init; }

    /// <summary>SHA-256 十六进制（小写），上传或扫描写入；可空。</summary>
    public string? ContentHash { get; init; }

    /// <summary>
    /// 状态版本号。每次状态转换必须递增并使用条件更新（<c>WHERE state_version = @旧值</c>），
    /// 防止旧扫描结果/旧回调覆盖新状态（ABA 防护）。
    /// </summary>
    public long StateVersion { get; init; }

    /// <summary>是否为语音附件。为 true 时 VoiceCodec/VoiceContainer/VoiceDurationMs/VoiceSampleRateHz/VoiceChannels 必须非空且为正。</summary>
    public bool IsVoice { get; init; }

    /// <summary>音频编解码器（如 opus、aac）。仅语音附件有值。</summary>
    public string? VoiceCodec { get; init; }

    /// <summary>音频容器格式（如 ogg、m4a）。仅语音附件有值。</summary>
    public string? VoiceContainer { get; init; }

    /// <summary>语音时长（毫秒）。仅语音附件有值。</summary>
    public long? VoiceDurationMs { get; init; }

    /// <summary>采样率（Hz）。仅语音附件有值。</summary>
    public int? VoiceSampleRateHz { get; init; }

    /// <summary>声道数。仅语音附件有值。</summary>
    public short? VoiceChannels { get; init; }

    /// <summary>
    /// 语音波形采样峰值包络（VOICE-MSG-2 可选 waveform，有界 bytea）：每字节 0–255 归一化幅度。
    /// 可选字段——缺省/空表示无波形；绑定语音消息时随发送方元数据快照写入（BOUND-V2）。
    /// </summary>
    public byte[]? VoiceWaveformPeaks { get; init; }
}