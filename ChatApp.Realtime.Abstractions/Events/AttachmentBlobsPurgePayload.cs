namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 账号删除后通知 Server GC 附件 blob 的载荷（可分片）。
/// </summary>
public sealed class AttachmentBlobsPurgePayload
{
    public required long UserId { get; init; }
    public required IReadOnlyList<string> ObjectKeys { get; init; }
    public int ChunkIndex { get; init; }
    public int ChunkCount { get; init; }
}
