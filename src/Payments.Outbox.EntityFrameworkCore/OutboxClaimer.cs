using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace Payments.Outbox.EntityFrameworkCore;

internal enum ClaimStrategy
{
    /// <summary>Per-row compare-and-swap on <see cref="OutboxMessage.Version"/>. Works on every relational provider.</summary>
    Optimistic,

    /// <summary><c>SELECT ... FOR UPDATE SKIP LOCKED</c> inside a short transaction (PostgreSQL).</summary>
    SkipLocked,
}

/// <summary>Selects due messages and leases them to one relay instance.</summary>
internal static class OutboxClaimer
{
    internal const string NpgsqlProviderName = "Npgsql.EntityFrameworkCore.PostgreSQL";

    internal static ClaimStrategy SelectStrategy(DbContext context, OutboxRelayOptions options) =>
        options.UsePostgresSkipLocked && string.Equals(context.Database.ProviderName, NpgsqlProviderName, StringComparison.Ordinal)
            ? ClaimStrategy.SkipLocked
            : ClaimStrategy.Optimistic;

    internal static Task<List<OutboxMessage>> ClaimAsync(
        DbContext context,
        OutboxRelayOptions options,
        DateTime now,
        CancellationToken cancellationToken) =>
        SelectStrategy(context, options) == ClaimStrategy.SkipLocked
            ? ClaimWithSkipLockedAsync(context, options, now, cancellationToken)
            : ClaimOptimisticallyAsync(context, options, now, cancellationToken);

    /// <summary>
    /// Due, unclaimed messages; with partition ordering, only the oldest pending message of each partition.
    /// </summary>
    internal static IQueryable<OutboxMessage> DueMessages(DbContext context, OutboxRelayOptions options, DateTime now)
    {
        var outbox = context.Set<OutboxMessage>();
        var query = outbox.Where(m =>
            m.ProcessedAt == null &&
            m.DeadLetteredAt == null &&
            m.NextAttemptAt <= now &&
            (m.LockedUntil == null || m.LockedUntil < now));

        if (options.EnablePartitionOrdering)
        {
            query = query.Where(m =>
                m.PartitionKey == null ||
                !outbox.Any(o =>
                    o.PartitionKey == m.PartitionKey &&
                    o.Sequence < m.Sequence &&
                    o.ProcessedAt == null &&
                    o.DeadLetteredAt == null));
        }

        return query;
    }

    private static async Task<List<OutboxMessage>> ClaimOptimisticallyAsync(
        DbContext context,
        OutboxRelayOptions options,
        DateTime now,
        CancellationToken cancellationToken)
    {
        var candidates = await DueMessages(context, options, now)
            .OrderBy(m => m.Sequence)
            .Take(options.BatchSize)
            .AsNoTracking()
            .ToListAsync(cancellationToken);

        var owner = options.InstanceId;
        DateTime? lockedUntil = now + options.LockDuration;
        var claimed = new List<OutboxMessage>(candidates.Count);

        foreach (var candidate in candidates)
        {
            var sequence = candidate.Sequence;
            var observedVersion = candidate.Version;
            var newVersion = Guid.NewGuid();

            // Compare-and-swap: succeeds only if nobody changed the row since we read it.
            var affected = await context.Set<OutboxMessage>()
                .Where(m => m.Sequence == sequence && m.Version == observedVersion)
                .ExecuteUpdateAsync(
                    setters => setters
                        .SetProperty(m => m.Version, newVersion)
                        .SetProperty(m => m.LockedBy, owner)
                        .SetProperty(m => m.LockedUntil, lockedUntil),
                    cancellationToken);

            if (affected == 1)
            {
                candidate.Version = newVersion;
                candidate.LockedBy = owner;
                candidate.LockedUntil = lockedUntil;
                claimed.Add(candidate);
            }
        }

        return claimed;
    }

    private static async Task<List<OutboxMessage>> ClaimWithSkipLockedAsync(
        DbContext context,
        OutboxRelayOptions options,
        DateTime now,
        CancellationToken cancellationToken)
    {
        var sql = BuildPostgresClaimSql(context, options.EnablePartitionOrdering);
        var owner = options.InstanceId;
        DateTime? lockedUntil = now + options.LockDuration;
        var strategy = context.Database.CreateExecutionStrategy();

        return await strategy.ExecuteAsync(async () =>
        {
            await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

            var rows = await context.Set<OutboxMessage>()
                .FromSqlRaw(sql, now, options.BatchSize)
                .AsNoTracking()
                .ToListAsync(cancellationToken);

            if (rows.Count > 0)
            {
                var sequences = rows.Select(r => r.Sequence).ToList();
                var newVersion = Guid.NewGuid();
                await context.Set<OutboxMessage>()
                    .Where(m => sequences.Contains(m.Sequence))
                    .ExecuteUpdateAsync(
                        setters => setters
                            .SetProperty(m => m.Version, newVersion)
                            .SetProperty(m => m.LockedBy, owner)
                            .SetProperty(m => m.LockedUntil, lockedUntil),
                        cancellationToken);

                foreach (var row in rows)
                {
                    row.Version = newVersion;
                    row.LockedBy = owner;
                    row.LockedUntil = lockedUntil;
                }
            }

            await transaction.CommitAsync(cancellationToken);
            return rows;
        });
    }

    /// <summary>
    /// Builds the PostgreSQL claim query from the EF Core model, so custom table names, schemas and
    /// naming conventions are honoured. Parameters: {0} = now, {1} = batch size.
    /// </summary>
    internal static string BuildPostgresClaimSql(DbContext context, bool partitionOrdering)
    {
        var entityType = context.Model.FindEntityType(typeof(OutboxMessage))
            ?? throw new InvalidOperationException("OutboxMessage is not part of the model. Call modelBuilder.AddOutbox().");
        var tableName = entityType.GetTableName()
            ?? throw new InvalidOperationException("OutboxMessage is not mapped to a table.");
        var schema = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schema);
        var sqlHelper = context.GetService<ISqlGenerationHelper>();

        string Column(string propertyName)
        {
            var property = entityType.FindProperty(propertyName)
                ?? throw new InvalidOperationException("OutboxMessage property '" + propertyName + "' is not mapped.");
            var columnName = property.GetColumnName(storeObject)
                ?? property.GetColumnName()
                ?? throw new InvalidOperationException("OutboxMessage property '" + propertyName + "' has no column.");
            return sqlHelper.DelimitIdentifier(columnName);
        }

        var table = sqlHelper.DelimitIdentifier(tableName, schema);
        var sequence = Column(nameof(OutboxMessage.Sequence));
        var processedAt = Column(nameof(OutboxMessage.ProcessedAt));
        var deadLetteredAt = Column(nameof(OutboxMessage.DeadLetteredAt));
        var nextAttemptAt = Column(nameof(OutboxMessage.NextAttemptAt));
        var lockedUntil = Column(nameof(OutboxMessage.LockedUntil));
        var partitionKey = Column(nameof(OutboxMessage.PartitionKey));

        var sql = new StringBuilder();
        sql.Append("SELECT m.* FROM ").Append(table).Append(" AS m");
        sql.Append(" WHERE m.").Append(processedAt).Append(" IS NULL");
        sql.Append(" AND m.").Append(deadLetteredAt).Append(" IS NULL");
        sql.Append(" AND m.").Append(nextAttemptAt).Append(" <= {0}");
        sql.Append(" AND (m.").Append(lockedUntil).Append(" IS NULL OR m.").Append(lockedUntil).Append(" < {0})");

        if (partitionOrdering)
        {
            sql.Append(" AND (m.").Append(partitionKey).Append(" IS NULL OR NOT EXISTS (SELECT 1 FROM ")
                .Append(table).Append(" AS o")
                .Append(" WHERE o.").Append(partitionKey).Append(" = m.").Append(partitionKey)
                .Append(" AND o.").Append(sequence).Append(" < m.").Append(sequence)
                .Append(" AND o.").Append(processedAt).Append(" IS NULL")
                .Append(" AND o.").Append(deadLetteredAt).Append(" IS NULL))");
        }

        sql.Append(" ORDER BY m.").Append(sequence);
        sql.Append(" LIMIT {1}");
        sql.Append(" FOR UPDATE OF m SKIP LOCKED");
        return sql.ToString();
    }
}
