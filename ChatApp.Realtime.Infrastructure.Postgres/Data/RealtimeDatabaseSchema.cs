namespace ChatApp.Realtime.Infrastructure.Postgres.Data;

public sealed class RealtimeDatabaseSchema
{
    public RealtimeDatabaseSchema(string schema)
    {
        Schema = string.IsNullOrWhiteSpace(schema) ? "realtime" : schema.Trim();
    }

    public string Schema { get; }

    public string QuotedSchema => QuoteIdentifier(Schema);

    public string MessagesTableSql => $"{QuotedSchema}.\"messages\"";
    public string MessageStateTableSql => $"{QuotedSchema}.\"message_state\"";
    public string AttachmentsTableSql => $"{QuotedSchema}.\"attachments\"";
    public string MessageReactionsTableSql => $"{QuotedSchema}.\"message_reactions\"";
    public string ConversationsTableSql => $"{QuotedSchema}.\"conversations\"";
    public string ConversationMembersTableSql => $"{QuotedSchema}.\"conversation_members\"";
    public string DeviceSyncCursorsTableSql => $"{QuotedSchema}.\"device_sync_cursors\"";
    public string MessageMutationRequestsTableSql => $"{QuotedSchema}.\"message_mutation_requests\"";
    public string GroupMutationRequestsTableSql => $"{QuotedSchema}.\"group_mutation_requests\"";
    public string UserDeletionTombstonesTableSql => $"{QuotedSchema}.\"user_deletion_tombstones\"";
    public string CommandIdempotencyLedgerTableSql => $"{QuotedSchema}.\"command_idempotency_ledger\"";
    public string GroupOperationAuditTableSql => $"{QuotedSchema}.\"group_operation_audit\"";
    public string MembershipPeriodsTableSql => $"{QuotedSchema}.\"conversation_membership_periods\"";
    public string AccountCleanupJobsTableSql => $"{QuotedSchema}.\"account_cleanup_jobs\"";
    public string OutboxTableSql => $"{QuotedSchema}.\"outbox\"";
    public string SchemaMigrationsTableSql => $"{QuotedSchema}.\"schema_migrations\"";
    public string SchemaMigrationCheckpointsTableSql =>
        $"{QuotedSchema}.\"schema_migration_checkpoints\"";

    public string FriendRequestsTableSql => $"{QuotedSchema}.\"friend_requests\"";
    public string FriendshipsTableSql => $"{QuotedSchema}.\"friendships\"";
    public string RelationshipMutationRequestsTableSql => $"{QuotedSchema}.\"relationship_mutation_requests\"";
    public string RelationshipSyncCursorsTableSql => $"{QuotedSchema}.\"relationship_sync_cursors\"";
    public string RelationshipChangeLogTableSql => $"{QuotedSchema}.\"relationship_change_log\"";
    public string RelationshipChangeLogSequenceSql => $"{QuotedSchema}.\"relationship_change_seq\"";

    public static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new InvalidOperationException("数据库架构名不能为空。");
        }

        return identifier.Any(ch => !char.IsAsciiLetterOrDigit(ch) && ch != '_') ? throw new InvalidOperationException("数据库架构名只能包含英文字母、数字和下划线。") : $"\"{identifier}\"";
    }
}