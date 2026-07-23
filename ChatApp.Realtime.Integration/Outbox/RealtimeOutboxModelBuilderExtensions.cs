using Microsoft.EntityFrameworkCore;

namespace ChatApp.Realtime.Integration.Outbox;

public static class RealtimeOutboxModelBuilderExtensions
{
    public static ModelBuilder AddChatAppRealtimeOutbox(this ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RealtimeIntegrationOutboxItem>(entity =>
        {
            entity.ToTable("outbox", "realtime");
            entity.HasKey(item => item.EventId);
            entity.Property(item => item.EventId).HasColumnName("event_id").HasMaxLength(64);
            entity.Property(item => item.PayloadJson).HasColumnName("payload_json").IsRequired();
            entity.Property(item => item.TargetUserId).HasColumnName("target_user_id");
            entity.Property(item => item.EventType).HasColumnName("event_type");
            entity.Property(item => item.Status).HasColumnName("status");
            entity.Property(item => item.CreatedAtMs).HasColumnName("created_at_ms");
            entity.Property(item => item.NextAttemptAtMs).HasColumnName("next_attempt_at_ms");
            entity.Property(item => item.PublishedAtMs).HasColumnName("published_at_ms");
            entity.Property(item => item.AttemptCount).HasColumnName("attempt_count");
            entity.Property(item => item.LockedBy).HasColumnName("locked_by").HasMaxLength(128);
            entity.Property(item => item.LockedUntilMs).HasColumnName("locked_until_ms");
            entity.Property(item => item.LastError).HasColumnName("last_error").HasMaxLength(2048);
            entity.HasIndex(item => new { item.PublishedAtMs, item.NextAttemptAtMs });
            entity.HasIndex(item => item.TargetUserId)
                .HasDatabaseName("ix_outbox_target_user_id");
            entity.HasIndex(item => new { item.TargetUserId, item.EventType })
                .HasDatabaseName("ix_outbox_target_user_event_type");
        });

        return modelBuilder;
    }
}
