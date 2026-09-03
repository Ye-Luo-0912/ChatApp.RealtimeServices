namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// 消息附件线协议引用（版本化）。下载经 Server API，不含永久公网 URL。
/// </summary>
public sealed class AttachmentRef
{
    public const int CurrentVersion = 1;

    /// <summary>引用结构版本；旧客户端可忽略未知字段。</summary>
    public int RefVersion { get; init; } = CurrentVersion;

    public required string AttachmentId { get; init; }

    public string? FileName { get; init; }

    public required string ContentType { get; init; }

    public long SizeBytes { get; init; }

    /// <summary>客户端可见状态：扫描中 / 可下载。</summary>
    public AttachmentWireStatus Status { get; init; }

    /// <summary>
    /// 下载提示：通常为 attachmentId，客户端请求
    /// <c>GET /api/attachments/{id}/download</c>（非永久公网 URL）。
    /// </summary>
    public string? DownloadApiHint { get; init; }

    /// <summary>可选短时下载令牌（由 Server 签发时填充）。</summary>
    public string? DownloadToken { get; init; }

    /// <summary>可选缩略图提示（同 DownloadApiHint 语义，路径由 Server 约定）。</summary>
    public string? ThumbnailApiHint { get; init; }

    /// <summary>是否为语音附件。仅语音附件携带语音元数据（不携带音频包）。</summary>
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
    /// 语音波形采样峰值包络（VOICE-MSG-2 可选 waveform）：每字节 0–255 归一化幅度，
    /// 由录音端降采样生成。可选字段——缺省/空表示无波形（旧客户端/旧录音），
    /// 消费端必须以进度条降级渲染。仅语音附件可能携带。
    /// </summary>
    public byte[]? VoiceWaveformPeaks { get; init; }
}

/// <summary>附件对客户端的可用性（与库内 Ticketed/Confirmed/Bound 生命周期解耦）。</summary>
public enum AttachmentWireStatus : short
{
    Scanning = 0,
    Available = 1
}
