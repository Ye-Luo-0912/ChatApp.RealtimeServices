using ChatApp.Realtime.Infrastructure.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace ChatApp.Realtime.Infrastructure.Data;

public sealed class RealtimeDbContext : DbContext
{
    private static string _schema = "realtime";

    public RealtimeDbContext(DbContextOptions<RealtimeDbContext> options)
        : base(options)
    {
    }

    public DbSet<RealtimeMessageEntity> Messages => Set<RealtimeMessageEntity>();

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

            entity.Property(message => message.CreatedAtMs)
                .HasColumnName("created_at_ms");

            entity.HasIndex(message => message.ClientMessageId);
            entity.HasIndex(message => message.SenderUserId);
            entity.HasIndex(message => message.ReceiverUserId);
            entity.HasIndex(message => message.ReceivedAtMs);
            entity.HasIndex(message => new { message.SenderUserId, message.ClientMessageId })
                .IsUnique();
        });
    }
}
