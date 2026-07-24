namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeOpsQueryStore
{
    /// <summary>迁移目录 vs 已应用版本、未完成 checkpoint、可推迟标记。</summary>
    Task<RealtimeMigrationProgressDto> GetMigrationProgressAsync(CancellationToken ct = default);

    /// <summary>轻量积压：Outbox + 未完成迁移相关行计数 + 附件状态计数（表存在时）。</summary>
    Task<RealtimeOpsBacklogDto> GetBacklogsAsync(CancellationToken ct = default);
}

public sealed record RealtimeMigrationProgressDto(
    IReadOnlyList<RealtimeMigrationCatalogEntryDto> Catalog,
    IReadOnlyList<RealtimeAppliedMigrationDto> Applied,
    IReadOnlyList<RealtimeMigrationCheckpointDto> OpenCheckpoints,
    IReadOnlyList<int> NotFullyAppliedVersions,
    bool HasDeferredInProgress,
    long GeneratedAtMs);

public sealed record RealtimeMigrationCatalogEntryDto(int Version, string Name);

public sealed record RealtimeAppliedMigrationDto(int Version, string Name, long AppliedAtMs);

public sealed record RealtimeMigrationCheckpointDto(
    int MigrationVersion,
    string Phase,
    string CheckpointKey,
    string? CheckpointValue,
    long UpdatedAtMs);

public sealed record RealtimeOpsBacklogDto(
    long OutboxPendingCount,
    long OutboxDeadCount,
    long? OldestOutboxPendingAtMs,
    long? OldestOutboxPendingAgeMs,
    int OutboxMaxAttemptCount,
    bool Migration009Applied,
    long MessagesMissingConversationIdCount,
    bool AttachmentsTableAvailable,
    long AttachmentTicketedCount,
    long AttachmentConfirmedUnboundCount,
    long AttachmentScanningCount,
    long AttachmentAbandonedCount,
    string CleanupNote,
    long GeneratedAtMs);
