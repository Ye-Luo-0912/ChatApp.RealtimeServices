using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Abstractions.Messaging;

public static class AttachmentRefMapper
{
    /// <summary>
    /// 将存储行映射为线协议引用。Bound → Available；其余 → Scanning。
    /// DownloadApiHint 使用 attachmentId（GET /api/attachments/{id}/download）。
    /// </summary>
    public static AttachmentRef FromRecord(RealtimeAttachmentRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return new AttachmentRef
        {
            AttachmentId = record.AttachmentId,
            FileName = record.OriginalName,
            ContentType = record.ContentType,
            SizeBytes = record.SizeBytes,
            Status = record.Status == AttachmentStatus.Bound
                ? AttachmentWireStatus.Available
                : AttachmentWireStatus.Scanning,
            DownloadApiHint = record.AttachmentId,
            DownloadToken = null,
            ThumbnailApiHint = null
        };
    }

    public static IReadOnlyList<AttachmentRef> FromRecords(
        IEnumerable<RealtimeAttachmentRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return records.Select(FromRecord).ToArray();
    }
}
