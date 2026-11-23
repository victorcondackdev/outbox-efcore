namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Delivers outbox messages to a broker (Kafka, RabbitMQ, Azure Service Bus, ...). Implementations
/// must only return once the broker has durably accepted the message and must throw otherwise; a
/// thrown exception schedules a retry.
/// </summary>
/// <remarks>
/// Delivery is at-least-once: a message can be published more than once (for example when the
/// process dies between the broker acknowledgement and the database update). Consumers must
/// de-duplicate on <see cref="OutboxEnvelope.MessageId"/>, for example with
/// <see cref="InboxDbContextExtensions"/>.
/// </remarks>
public interface IOutboxPublisher
{
    /// <summary>Publishes a message.</summary>
    /// <param name="envelope">The message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the broker has accepted the message.</returns>
    Task PublishAsync(OutboxEnvelope envelope, CancellationToken cancellationToken);
}
