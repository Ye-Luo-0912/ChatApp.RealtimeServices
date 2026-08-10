using System.Collections.Concurrent;

namespace ChatApp.Realtime.Infrastructure.Postgres.Data;

public sealed class RealtimeDatabaseSchema
{
    private readonly ConcurrentDictionary<string, Lazy<string>> _commandTexts =
        new(StringComparer.Ordinal);

    public RealtimeDatabaseSchema(string schema)
    {
        Schema = string.IsNullOrWhiteSpace(schema) ? "realtime" : schema.Trim();
        QuotedSchema = QuoteIdentifier(Schema);

        MessagesTableSql = $"{QuotedSchema}.\"messages\"";
        MessageStateTableSql = $"{QuotedSchema}.\"message_state\"";
        AttachmentsTableSql = $"{QuotedSchema}.\"attachments\"";
        MessageReactionsTableSql = $"{QuotedSchema}.\"message_reactions\"";
        ConversationsTableSql = $"{QuotedSchema}.\"conversations\"";
        ConversationMembersTableSql = $"{QuotedSchema}.\"conversation_members\"";
        DeviceSyncCursorsTableSql = $"{QuotedSchema}.\"device_sync_cursors\"";
        MessageMutationRequestsTableSql = $"{QuotedSchema}.\"message_mutation_requests\"";
        GroupMutationRequestsTableSql = $"{QuotedSchema}.\"group_mutation_requests\"";
        UserDeletionTombstonesTableSql = $"{QuotedSchema}.\"user_deletion_tombstones\"";
        CommandIdempotencyLedgerTableSql = $"{QuotedSchema}.\"command_idempotency_ledger\"";
        GroupOperationAuditTableSql = $"{QuotedSchema}.\"group_operation_audit\"";
        MembershipPeriodsTableSql = $"{QuotedSchema}.\"conversation_membership_periods\"";
        AccountCleanupJobsTableSql = $"{QuotedSchema}.\"account_cleanup_jobs\"";
        OutboxTableSql = $"{QuotedSchema}.\"outbox\"";
        SchemaMigrationsTableSql = $"{QuotedSchema}.\"schema_migrations\"";
        SchemaMigrationCheckpointsTableSql = $"{QuotedSchema}.\"schema_migration_checkpoints\"";

        FriendRequestsTableSql = $"{QuotedSchema}.\"friend_requests\"";
        FriendshipsTableSql = $"{QuotedSchema}.\"friendships\"";
        RelationshipMutationRequestsTableSql = $"{QuotedSchema}.\"relationship_mutation_requests\"";
        RelationshipSyncCursorsTableSql = $"{QuotedSchema}.\"relationship_sync_cursors\"";
        RelationshipChangeLogTableSql = $"{QuotedSchema}.\"relationship_change_log\"";
        RelationshipChangeLogSequenceSql = $"{QuotedSchema}.\"relationship_change_seq\"";
        RelationshipProjectionVersionsTableSql = $"{QuotedSchema}.\"relationship_projection_versions\"";
        RelationshipProjectionItemsTableSql = $"{QuotedSchema}.\"relationship_projection_items\"";
        RelationshipProjectionInboxTableSql = $"{QuotedSchema}.\"relationship_projection_inbox\"";
        RelationshipProjectionSnapshotsTableSql = $"{QuotedSchema}.\"relationship_projection_snapshots\"";
        RelationshipProjectionRebuildStateTableSql = $"{QuotedSchema}.\"relationship_projection_rebuild_state\"";
    }

    public string Schema { get; }

    public string QuotedSchema { get; }

    public string MessagesTableSql { get; }
    public string MessageStateTableSql { get; }
    public string AttachmentsTableSql { get; }
    public string MessageReactionsTableSql { get; }
    public string ConversationsTableSql { get; }
    public string ConversationMembersTableSql { get; }
    public string DeviceSyncCursorsTableSql { get; }
    public string MessageMutationRequestsTableSql { get; }
    public string GroupMutationRequestsTableSql { get; }
    public string UserDeletionTombstonesTableSql { get; }
    public string CommandIdempotencyLedgerTableSql { get; }
    public string GroupOperationAuditTableSql { get; }
    public string MembershipPeriodsTableSql { get; }
    public string AccountCleanupJobsTableSql { get; }
    public string OutboxTableSql { get; }
    public string SchemaMigrationsTableSql { get; }
    public string SchemaMigrationCheckpointsTableSql { get; }

    public string FriendRequestsTableSql { get; }
    public string FriendshipsTableSql { get; }
    public string RelationshipMutationRequestsTableSql { get; }
    public string RelationshipSyncCursorsTableSql { get; }
    public string RelationshipChangeLogTableSql { get; }
    public string RelationshipChangeLogSequenceSql { get; }
    public string RelationshipProjectionVersionsTableSql { get; }
    public string RelationshipProjectionItemsTableSql { get; }
    public string RelationshipProjectionInboxTableSql { get; }
    public string RelationshipProjectionSnapshotsTableSql { get; }
    public string RelationshipProjectionRebuildStateTableSql { get; }

    /// <summary>
    /// 按 schema 实例缓存只依赖表名的不可变 SQL。factory 必须是 static lambda；
    /// 这样热路径既不重复插值整段 SQL，也不跨 schema 共享错误表名。
    /// </summary>
    internal string GetOrAddCommandText(
        string key,
        Func<RealtimeDatabaseSchema, string> factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(factory);
        return _commandTexts.GetOrAdd(
            key,
            static (_, state) => new Lazy<string>(
                () => state.Factory(state.Schema),
                LazyThreadSafetyMode.ExecutionAndPublication),
            (Schema: this, Factory: factory)).Value;
    }

    public static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new InvalidOperationException("数据库架构名不能为空。");
        }

        return identifier.Any(ch => !char.IsAsciiLetterOrDigit(ch) && ch != '_') ? throw new InvalidOperationException("数据库架构名只能包含英文字母、数字和下划线。") : $"\"{identifier}\"";
    }
}
