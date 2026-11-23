namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// A message persisted in the outbox table in the same transaction as the business change that
/// produced it. Rows are created by <see cref="OutboxWriter"/> or by
/// <see cref="OutboxSaveChangesInterceptor"/> and are driven to completion by
/// <see cref="OutboxProcessor{TContext}"/>.
/// </summary>
/// <remarks>
/// All timestamps are UTC <see cref="DateTime"/> values. <see cref="DateTimeOffset"/> is avoided on
/// purpose: several providers (SQLite in particular) cannot compare or order it on the server,
/// and the relay relies on server-side comparisons to select due messages.
/// </remarks>
public sealed class OutboxMessage
{
    /// <summary>
    /// Database-generated, monotonically increasing primary key. Defines the publication order
    /// (and therefore the per-partition order) of messages.
    /// </summary>
    public long Sequence { get; set; }

    /// <summary>
    /// Globally unique message identifier (time-ordered, UUID version 7 layout). Consumers use it
    /// for de-duplication; it is stable across publish retries.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>Stable contract name of the message, as mapped in <see cref="OutboxTypeRegistry"/>.</summary>
    public string Type { get; set; } = string.Empty;

    /// <summary>JSON payload produced by <see cref="OutboxSerializer"/>.</summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>JSON object of string headers (for example <c>traceparent</c>), or <see langword="null"/>.</summary>
    public string? Headers { get; set; }

    /// <summary>UTC instant at which the message was written.</summary>
    public DateTime OccurredAt { get; set; }

    /// <summary>Identifier of the aggregate that produced the message, if any.</summary>
    public string? AggregateId { get; set; }

    /// <summary>
    /// Ordering scope. When partition ordering is enabled, a message is not published while an
    /// older message with the same partition key is still pending.
    /// </summary>
    public string? PartitionKey { get; set; }

    /// <summary>Number of failed publish attempts so far.</summary>
    public int Attempts { get; set; }

    /// <summary>UTC instant before which the relay will not try to publish the message.</summary>
    public DateTime NextAttemptAt { get; set; }

    /// <summary>UTC instant at which the message was successfully published.</summary>
    public DateTime? ProcessedAt { get; set; }

    /// <summary>UTC instant at which the message was given up on after exhausting its attempts.</summary>
    public DateTime? DeadLetteredAt { get; set; }

    /// <summary>UTC instant at which the current claim (lease) expires.</summary>
    public DateTime? LockedUntil { get; set; }

    /// <summary>Instance identifier of the relay currently holding the claim.</summary>
    public string? LockedBy { get; set; }

    /// <summary>Last publish error, truncated.</summary>
    public string? Error { get; set; }

    /// <summary>
    /// Optimistic concurrency token. Every state transition performed by the relay replaces it, so
    /// two relay instances can never both claim the same version of a row.
    /// </summary>
    public Guid Version { get; set; }
}
