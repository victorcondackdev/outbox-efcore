namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Per-message overrides used when writing to the outbox.</summary>
public sealed class OutboxMessageOptions
{
    /// <summary>Explicit message identifier. Defaults to a new time-ordered identifier.</summary>
    public Guid? MessageId { get; set; }

    /// <summary>Ordering scope. Defaults to the partition key selector registered for the message type.</summary>
    public string? PartitionKey { get; set; }

    /// <summary>Aggregate identifier. Defaults to the partition key.</summary>
    public string? AggregateId { get; set; }

    /// <summary>Additional headers stored with the message and handed to the publisher.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; set; }
}
