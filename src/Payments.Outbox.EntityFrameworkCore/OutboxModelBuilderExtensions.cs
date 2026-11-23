using Microsoft.EntityFrameworkCore;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Adds the outbox and inbox tables to an EF Core model.</summary>
public static class OutboxModelBuilderExtensions
{
    /// <summary>Default outbox table name.</summary>
    public const string DefaultOutboxTableName = "OutboxMessages";

    /// <summary>Default inbox table name.</summary>
    public const string DefaultInboxTableName = "InboxMessages";

    /// <summary>
    /// Maps <see cref="OutboxMessage"/> with its indexes and concurrency token. Call it from
    /// <c>OnModelCreating</c>.
    /// </summary>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="tableName">Table name.</param>
    /// <param name="schema">Optional schema.</param>
    /// <returns>The same model builder.</returns>
    public static ModelBuilder AddOutbox(this ModelBuilder modelBuilder, string tableName = DefaultOutboxTableName, string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        modelBuilder.Entity<OutboxMessage>(builder =>
        {
            builder.ToTable(tableName, schema);
            builder.HasKey(m => m.Sequence);
            builder.Property(m => m.Sequence).ValueGeneratedOnAdd();
            builder.Property(m => m.Id).IsRequired();
            builder.Property(m => m.Type).HasMaxLength(256).IsRequired();
            builder.Property(m => m.Payload).IsRequired();
            builder.Property(m => m.AggregateId).HasMaxLength(256);
            builder.Property(m => m.PartitionKey).HasMaxLength(256);
            builder.Property(m => m.LockedBy).HasMaxLength(256);
            builder.Property(m => m.Error).HasMaxLength(OutboxLimits.MaxErrorLength);
            builder.Property(m => m.Version).IsConcurrencyToken();

            builder.HasIndex(m => m.Id).IsUnique();
            // Relay scan: pending, due messages.
            builder.HasIndex(m => new { m.ProcessedAt, m.DeadLetteredAt, m.NextAttemptAt });
            // Per-partition ordering check ("is there an older pending message in my partition?").
            builder.HasIndex(m => new { m.PartitionKey, m.Sequence });
        });

        return modelBuilder;
    }

    /// <summary>
    /// Maps <see cref="InboxMessage"/>, keyed by message identifier and consumer name. Call it from
    /// <c>OnModelCreating</c> in services that consume messages.
    /// </summary>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="tableName">Table name.</param>
    /// <param name="schema">Optional schema.</param>
    /// <returns>The same model builder.</returns>
    public static ModelBuilder AddInbox(this ModelBuilder modelBuilder, string tableName = DefaultInboxTableName, string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        modelBuilder.Entity<InboxMessage>(builder =>
        {
            builder.ToTable(tableName, schema);
            builder.HasKey(m => new { m.MessageId, m.Consumer });
            builder.Property(m => m.MessageId).HasMaxLength(256);
            builder.Property(m => m.Consumer).HasMaxLength(256);
            builder.HasIndex(m => m.ProcessedAt);
        });

        return modelBuilder;
    }
}
