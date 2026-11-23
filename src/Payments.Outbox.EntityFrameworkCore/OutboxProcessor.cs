using System.Diagnostics;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// One relay instance: claims due messages, publishes them through <see cref="IOutboxPublisher"/>,
/// records the outcome and cleans up old rows. <see cref="OutboxRelayService{TContext}"/> drives it
/// in a loop; it can also be called directly (tests, serverless jobs).
/// </summary>
/// <remarks>
/// Safe to run concurrently in any number of processes. Every claim is a lease
/// (<see cref="OutboxMessage.LockedUntil"/>) taken with a compare-and-swap on
/// <see cref="OutboxMessage.Version"/>, or with <c>FOR UPDATE SKIP LOCKED</c> on PostgreSQL, so a
/// message is handed to one instance at a time. A crashed instance's leases simply expire.
/// </remarks>
/// <typeparam name="TContext">The context that owns the outbox table.</typeparam>
public sealed class OutboxProcessor<TContext>
    where TContext : DbContext
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OutboxRelayOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    /// <summary>Creates a processor.</summary>
    /// <param name="scopeFactory">Creates a scope (context and publisher) per batch.</param>
    /// <param name="options">Relay options.</param>
    /// <param name="timeProvider">Clock.</param>
    /// <param name="logger">Logger.</param>
    public OutboxProcessor(
        IServiceScopeFactory scopeFactory,
        IOptions<OutboxRelayOptions> options,
        TimeProvider timeProvider,
        ILogger<OutboxProcessor<TContext>> logger)
    {
        ArgumentNullException.ThrowIfNull(scopeFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options.Value;
        _options.Validate();
        _scopeFactory = scopeFactory;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <summary>Identifier of this instance, stored in <see cref="OutboxMessage.LockedBy"/>.</summary>
    public string InstanceId => _options.InstanceId;

    /// <summary>
    /// Claims up to <see cref="OutboxRelayOptions.BatchSize"/> due messages and publishes them in
    /// sequence order.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of messages claimed (published or failed). Zero means there was nothing to do.</returns>
    public async Task<int> ProcessBatchAsync(CancellationToken cancellationToken = default)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TContext>();
        var publisher = scope.ServiceProvider.GetRequiredService<IOutboxPublisher>();

        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var claimed = await OutboxClaimer.ClaimAsync(context, _options, now, cancellationToken);

        foreach (var message in claimed)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await PublishAsync(context, publisher, message, cancellationToken);
        }

        return claimed.Count;
    }

    /// <summary>
    /// Deletes published messages older than <see cref="OutboxRelayOptions.ProcessedRetention"/> and,
    /// when configured, dead letters and inbox records past their retention.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of deleted rows.</returns>
    public async Task<int> CleanupAsync(CancellationToken cancellationToken = default)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TContext>();
        var now = _timeProvider.GetUtcNow().UtcDateTime;

        var processedCutoff = now - _options.ProcessedRetention;
        var deleted = await context.Set<OutboxMessage>()
            .Where(m => m.ProcessedAt != null && m.ProcessedAt < processedCutoff)
            .ExecuteDeleteAsync(cancellationToken);

        if (_options.DeadLetterRetention is { } deadLetterRetention)
        {
            var deadLetterCutoff = now - deadLetterRetention;
            deleted += await context.Set<OutboxMessage>()
                .Where(m => m.DeadLetteredAt != null && m.DeadLetteredAt < deadLetterCutoff)
                .ExecuteDeleteAsync(cancellationToken);
        }

        if (_options.InboxRetention is { } inboxRetention && context.Model.FindEntityType(typeof(InboxMessage)) is not null)
        {
            var inboxCutoff = now - inboxRetention;
            deleted += await context.Set<InboxMessage>()
                .Where(m => m.ProcessedAt < inboxCutoff)
                .ExecuteDeleteAsync(cancellationToken);
        }

        return deleted;
    }

    private async Task PublishAsync(DbContext context, IOutboxPublisher publisher, OutboxMessage message, CancellationToken cancellationToken)
    {
        var attempt = message.Attempts + 1;
        var headers = ParseHeaders(message);
        var parent = TraceContextHeaders.Extract(headers);

        using var activity = OutboxTelemetry.ActivitySource.StartActivity(
            "outbox publish " + message.Type,
            ActivityKind.Producer,
            parent);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "outbox");
            activity.SetTag("messaging.operation", "publish");
            activity.SetTag("messaging.message.id", message.Id.ToString());
            activity.SetTag("outbox.message.type", message.Type);
            activity.SetTag("outbox.partition_key", message.PartitionKey);
            activity.SetTag("outbox.attempt", attempt);

            // The broker message continues the trace from the publish span.
            TraceContextHeaders.Inject(activity, headers);
        }

        var envelope = new OutboxEnvelope
        {
            MessageId = message.Id,
            Type = message.Type,
            Payload = message.Payload,
            Headers = headers,
            OccurredAt = new DateTimeOffset(DateTime.SpecifyKind(message.OccurredAt, DateTimeKind.Utc)),
            AggregateId = message.AggregateId,
            PartitionKey = message.PartitionKey,
            Attempt = attempt,
        };

        try
        {
            await publisher.PublishAsync(envelope, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutting down: the lease expires and the message is retried later without counting an attempt.
            throw;
        }
        catch (Exception exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            await RecordFailureAsync(context, message, attempt, exception);
            return;
        }

        // Persist the outcome even if shutdown was requested meanwhile, to avoid a needless re-publish.
        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var sequence = message.Sequence;
        var newVersion = Guid.NewGuid();
        DateTime? processedAt = now;
        await context.Set<OutboxMessage>()
            .Where(m => m.Sequence == sequence && m.ProcessedAt == null)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(m => m.ProcessedAt, processedAt)
                    .SetProperty(m => m.LockedBy, (string?)null)
                    .SetProperty(m => m.LockedUntil, (DateTime?)null)
                    .SetProperty(m => m.Error, (string?)null)
                    .SetProperty(m => m.Version, newVersion),
                CancellationToken.None);

        var typeTag = OutboxTelemetry.TypeTag(message.Type);
        OutboxTelemetry.Published.Add(1, typeTag);
        OutboxTelemetry.Lag.Record(Math.Max(0, (now - message.OccurredAt).TotalSeconds), typeTag);
    }

    private async Task RecordFailureAsync(DbContext context, OutboxMessage message, int attempt, Exception exception)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var sequence = message.Sequence;
        var newVersion = Guid.NewGuid();
        var error = OutboxLimits.Truncate(exception.GetType().FullName + ": " + exception.Message, OutboxLimits.MaxErrorLength);
        var typeTag = OutboxTelemetry.TypeTag(message.Type);
        OutboxTelemetry.Failed.Add(1, typeTag);

        if (attempt >= _options.MaxAttempts)
        {
            DateTime? deadLetteredAt = now;
            await context.Set<OutboxMessage>()
                .Where(m => m.Sequence == sequence && m.ProcessedAt == null)
                .ExecuteUpdateAsync(
                    setters => setters
                        .SetProperty(m => m.Attempts, attempt)
                        .SetProperty(m => m.DeadLetteredAt, deadLetteredAt)
                        .SetProperty(m => m.Error, error)
                        .SetProperty(m => m.LockedBy, (string?)null)
                        .SetProperty(m => m.LockedUntil, (DateTime?)null)
                        .SetProperty(m => m.Version, newVersion),
                    CancellationToken.None);

            OutboxTelemetry.DeadLettered.Add(1, typeTag);
            _logger.LogError(
                exception,
                "Outbox message {MessageId} ({MessageType}) dead-lettered after {Attempts} attempts",
                message.Id,
                message.Type,
                attempt);
            return;
        }

        var delay = RetryPolicy.ComputeDelay(attempt, _options, Random.Shared.NextDouble());
        var nextAttemptAt = now + delay;
        await context.Set<OutboxMessage>()
            .Where(m => m.Sequence == sequence && m.ProcessedAt == null)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(m => m.Attempts, attempt)
                    .SetProperty(m => m.NextAttemptAt, nextAttemptAt)
                    .SetProperty(m => m.Error, error)
                    .SetProperty(m => m.LockedBy, (string?)null)
                    .SetProperty(m => m.LockedUntil, (DateTime?)null)
                    .SetProperty(m => m.Version, newVersion),
                CancellationToken.None);

        _logger.LogWarning(
            exception,
            "Publishing outbox message {MessageId} ({MessageType}) failed on attempt {Attempt}; next attempt at {NextAttemptAt:O}",
            message.Id,
            message.Type,
            attempt,
            nextAttemptAt);
    }

    private Dictionary<string, string> ParseHeaders(OutboxMessage message)
    {
        try
        {
            return TraceContextHeaders.Parse(message.Headers);
        }
        catch (JsonException exception)
        {
            _logger.LogWarning(exception, "Ignoring malformed headers of outbox message {MessageId}", message.Id);
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }
    }
}
