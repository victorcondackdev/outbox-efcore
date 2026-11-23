namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>A message handed to <see cref="IOutboxPublisher"/>.</summary>
public sealed class OutboxEnvelope
{
    /// <summary>Unique, retry-stable message identifier. Use it as the broker message id / de-duplication key.</summary>
    public required Guid MessageId { get; init; }

    /// <summary>Stable contract name, for example <c>payments.charge.paid.v1</c>.</summary>
    public required string Type { get; init; }

    /// <summary>JSON payload.</summary>
    public required string Payload { get; init; }

    /// <summary>
    /// Headers stored with the message. When tracing is active, <c>traceparent</c>/<c>tracestate</c>
    /// describe the publish activity, a child of the activity that wrote the message.
    /// </summary>
    public required IReadOnlyDictionary<string, string> Headers { get; init; }

    /// <summary>When the message was written.</summary>
    public required DateTimeOffset OccurredAt { get; init; }

    /// <summary>Aggregate identifier, if any.</summary>
    public string? AggregateId { get; init; }

    /// <summary>Partition (ordering) key, if any. Map it to the broker partition/message key.</summary>
    public string? PartitionKey { get; init; }

    /// <summary>1-based number of this publish attempt.</summary>
    public required int Attempt { get; init; }
}
