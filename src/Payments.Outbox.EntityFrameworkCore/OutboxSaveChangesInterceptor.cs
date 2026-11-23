using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Options;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Collects domain events from tracked <see cref="IHasDomainEvents"/> entities and adds them as
/// <see cref="OutboxMessage"/> rows to the unit of work being saved, so the events commit or roll
/// back together with the business change. Also backs <see cref="OutboxDbContextExtensions.Outbox(DbContext)"/>.
/// </summary>
/// <remarks>Register it with <see cref="OutboxDbContextExtensions.UseOutbox(DbContextOptionsBuilder, IServiceProvider)"/>.</remarks>
public sealed class OutboxSaveChangesInterceptor : SaveChangesInterceptor
{
    private readonly OutboxSerializer _serializer;
    private readonly OutboxOptions _options;

    /// <summary>Creates the interceptor.</summary>
    /// <param name="serializer">Message serializer.</param>
    /// <param name="timeProvider">Clock used for message timestamps.</param>
    /// <param name="options">Outbox options.</param>
    public OutboxSaveChangesInterceptor(OutboxSerializer serializer, TimeProvider timeProvider, IOptions<OutboxOptions> options)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);
        _serializer = serializer;
        TimeProvider = timeProvider;
        _options = options.Value;
    }

    internal TimeProvider TimeProvider { get; }

    /// <inheritdoc />
    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        if (eventData.Context is not null)
        {
            CollectDomainEvents(eventData.Context);
        }

        return result;
    }

    /// <inheritdoc />
    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        if (eventData.Context is not null)
        {
            CollectDomainEvents(eventData.Context);
        }

        return ValueTask.FromResult(result);
    }

    internal OutboxMessage CreateMessage(object message, OutboxMessageOptions? messageOptions)
    {
        var payload = _serializer.Serialize(message, out var typeName);
        var now = TimeProvider.GetUtcNow();

        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        if (messageOptions?.Headers is { } extra)
        {
            foreach (var pair in extra)
            {
                headers[pair.Key] = pair.Value;
            }
        }

        if (_options.CaptureTraceContext)
        {
            TraceContextHeaders.Inject(Activity.Current, headers);
        }

        var partitionKey = messageOptions?.PartitionKey ?? _serializer.Registry.GetPartitionKey(message);

        return new OutboxMessage
        {
            Id = messageOptions?.MessageId ?? OutboxIds.NewId(now),
            Type = typeName,
            Payload = payload,
            Headers = TraceContextHeaders.Serialize(headers),
            OccurredAt = now.UtcDateTime,
            AggregateId = messageOptions?.AggregateId ?? partitionKey,
            PartitionKey = partitionKey,
            Attempts = 0,
            NextAttemptAt = now.UtcDateTime,
            Version = Guid.NewGuid(),
        };
    }

    private void CollectDomainEvents(DbContext context)
    {
        var sources = context.ChangeTracker.Entries<IHasDomainEvents>()
            .Select(entry => entry.Entity)
            .ToList();

        if (sources.Count == 0)
        {
            return;
        }

        var outbox = context.Set<OutboxMessage>();
        foreach (var source in sources)
        {
            foreach (var domainEvent in source.DequeueDomainEvents())
            {
                outbox.Add(CreateMessage(domainEvent, null));
            }
        }
    }
}
