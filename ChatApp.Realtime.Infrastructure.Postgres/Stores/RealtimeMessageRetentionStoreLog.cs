using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// Realtime 回收审计。EventId 段 4200-4299 为 Realtime 回收审计专用：
/// 每批解绑/入队须有 Information 级留痕，供附件回收链路与 Server 侧对账。
/// </summary>
internal static partial class RealtimeMessageRetentionLog
{
    /// <summary>保留清理批次提交完成：删除消息数、解绑附件数、入队 blob 回收事件数。</summary>
    [LoggerMessage(EventId = 4201, Level = LogLevel.Information,
        Message = "消息保留清理批次完成 Deleted={Deleted} TipsRepaired={TipsRepaired} " +
                  "AttachmentsAbandoned={AttachmentsAbandoned} PurgeEvents={PurgeEvents} CutoffMs={CutoffMs}")]
    public static partial void PurgeBatchCompleted(
        ILogger logger,
        int deleted,
        int tipsRepaired,
        int attachmentsAbandoned,
        int purgeEvents,
        long cutoffMs);
}
