using ChatApp.Realtime.Infrastructure.Postgres.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace ChatApp.Realtime.Infrastructure.Postgres.Data;

public sealed class RealtimeDbContext : DbContext
{
    private static string _schema = "realtime";

    public RealtimeDbContext(DbContextOptions<RealtimeDbContext> options)
        : base(options)
    {
    }

    public DbSet<RealtimeMessageEntity> Messages => Set<RealtimeMessageEntity>();
    public DbSet<RealtimeOutboxEntity> Outbox => Set<RealtimeOutboxEntity>();

    public static void ConfigureSchema(string schema)
    {
        _schema = string.IsNullOrWhiteSpace(schema) ? "realtime" : schema.Trim();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(_schema);

        modelBuilder.Entity<RealtimeMessageEntity>(entity =>
        {
            entity.ToTable("messages");
            entity.HasKey(message => message.MessageId);

            entity.Property(message => message.MessageId)
                .HasColumnName("message_id")
                .HasMaxLength(64);

            entity.Property(message => message.ClientMessageId)
                .HasColumnName("client_message_id")
                .HasMaxLength(128)
                .IsRequired();

            entity.Property(message => message.SenderUserId)
                .HasColumnName("sender_user_id");

            entity.Property(message => message.SenderSessionId)
                .HasColumnName("sender_session_id")
                .HasMaxLength(128)
                .IsRequired();

            entity.Property(message => message.ReceiverUserId)
                .HasColumnName("receiver_user_id");

            entity.Property(message => message.Content)
                .HasColumnName("content")
                .IsRequired();

            entity.Property(message => message.ReceivedAtMs)
                .HasColumnName("received_at_ms");

            entity.Property(message => message.DeliveredAtMs)
                .HasColumnName("delivered_at_ms");

            entity.Property(message => message.ReadAtMs)
                .HasColumnName("read_at_ms");

            entity.Property(message => message.CreatedAtMs)
                .HasColumnName("created_at_ms");

            entity.HasIndex(message => new { message.SenderUserId, message.ClientMessageId })
                .IsUnique();
            entity.HasIndex(message => new
                {
                    message.ReceiverUserId,
                    message.ReceivedAtMs,
                    message.MessageId
                });
            entity.HasIndex(message => new
                {
                    message.SenderUserId,
                    message.ReceivedAtMs,
                    message.MessageId
                });
        });

        modelBuilder.Entity<RealtimeOutboxEntity>(entity =>
        {
            entity.ToTable("outbox");
            entity.HasKey(item => item.EventId);
            entity.Property(item => item.EventId).HasColumnName("event_id").HasMaxLength(64);
            entity.Property(item => item.PayloadJson).HasColumnName("payload_json").IsRequired();
            entity.Property(item => item.TargetUserId).HasColumnName("target_user_id");
            entity.Property(item => item.EventType).HasColumnName("event_type");
            entity.Property(item => item.CreatedAtMs).HasColumnName("created_at_ms");
            entity.Property(item => item.NextAttemptAtMs).HasColumnName("next_attempt_at_ms");
            entity.Property(item => item.PublishedAtMs).HasColumnName("published_at_ms");
            entity.Property(item => item.AttemptCount).HasColumnName("attempt_count");
            entity.Property(item => item.LockedBy).HasColumnName("locked_by").HasMaxLength(128);
            entity.Property(item => item.LockedUntilMs).HasColumnName("locked_until_ms");
            entity.Property(item => item.LastError).HasColumnName("last_error").HasMaxLength(2048);
            entity.HasIndex(item => new { item.PublishedAtMs, item.NextAttemptAtMs });
            entity.HasIndex(item => item.TargetUserId);
            entity.HasIndex(item => new { item.TargetUserId, item.EventType });
        });
    }
}
