namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Relay (publisher side) options.</summary>
public sealed class OutboxRelayOptions
{
    /// <summary>
    /// Identifier of this relay instance, stored in <see cref="OutboxMessage.LockedBy"/> while it
    /// holds a claim. Defaults to the machine name plus a random suffix; must be unique per process.
    /// </summary>
    public string InstanceId { get; set; } = Environment.MachineName + ":" + Guid.NewGuid().ToString("N")[..8];

    /// <summary>Delay between polls when the previous poll found nothing to do. Default: 1 second.</summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Maximum number of messages claimed per poll. Default: 100.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Claim lease. If a relay crashes, its claimed messages become available again after this
    /// period. It should comfortably exceed the time needed to publish a batch. Default: 30 seconds.
    /// </summary>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Attempts before a message is dead-lettered. Default: 10.</summary>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>Delay after the first failure; doubled on every further failure. Default: 1 second.</summary>
    public TimeSpan BaseRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Upper bound of the retry delay. Default: 5 minutes.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Fraction (0 to 1) of the retry delay that is randomized downwards, so that a broker outage does
    /// not produce synchronized retry storms. Default: 0.2.
    /// </summary>
    public double RetryJitter { get; set; } = 0.2;

    /// <summary>
    /// When <see langword="true"/> (default), a message is not published while an older message with
    /// the same <see cref="OutboxMessage.PartitionKey"/> is still pending (claimed, failed and waiting
    /// for a retry, or not yet published). Messages without a partition key are unordered.
    /// Dead-lettered messages no longer block their partition.
    /// </summary>
    public bool EnablePartitionOrdering { get; set; } = true;

    /// <summary>
    /// When <see langword="true"/> (default) and the provider is Npgsql, messages are claimed with
    /// <c>SELECT ... FOR UPDATE SKIP LOCKED</c> instead of per-row optimistic claims.
    /// </summary>
    public bool UsePostgresSkipLocked { get; set; } = true;

    /// <summary>How long published messages are kept before cleanup deletes them. Default: 7 days.</summary>
    public TimeSpan ProcessedRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>How long dead-lettered messages are kept. <see langword="null"/> (default) keeps them until handled manually.</summary>
    public TimeSpan? DeadLetterRetention { get; set; }

    /// <summary>
    /// How long inbox records are kept. <see langword="null"/> (default) keeps them forever; a finite
    /// value bounds the de-duplication window.
    /// </summary>
    public TimeSpan? InboxRetention { get; set; }

    /// <summary>Interval between cleanup runs. Default: 1 hour.</summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(InstanceId))
        {
            throw new InvalidOperationException("OutboxRelayOptions.InstanceId must not be empty.");
        }

        if (BatchSize <= 0)
        {
            throw new InvalidOperationException("OutboxRelayOptions.BatchSize must be positive.");
        }

        if (MaxAttempts <= 0)
        {
            throw new InvalidOperationException("OutboxRelayOptions.MaxAttempts must be positive.");
        }

        if (PollingInterval <= TimeSpan.Zero || LockDuration <= TimeSpan.Zero || CleanupInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "OutboxRelayOptions.PollingInterval, LockDuration and CleanupInterval must be positive.");
        }

        if (BaseRetryDelay < TimeSpan.Zero || MaxRetryDelay < BaseRetryDelay)
        {
            throw new InvalidOperationException(
                "OutboxRelayOptions.BaseRetryDelay must be non-negative and not greater than MaxRetryDelay.");
        }

        if (RetryJitter is < 0d or > 1d || double.IsNaN(RetryJitter))
        {
            throw new InvalidOperationException("OutboxRelayOptions.RetryJitter must be between 0 and 1.");
        }
    }
}
