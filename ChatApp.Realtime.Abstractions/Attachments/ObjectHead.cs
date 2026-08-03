namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>对象存储 HEAD 探测结果（用于 Finalize/扫描时与票证元数据核对）。</summary>
public readonly record struct ObjectHead(
    string ObjectKey,
    long SizeBytes,
    string? ContentHash,
    string? ContentType);