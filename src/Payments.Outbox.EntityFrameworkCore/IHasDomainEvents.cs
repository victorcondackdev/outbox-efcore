namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Implemented by entities (typically aggregate roots) that raise domain events. On
/// <c>SaveChanges</c>, <see cref="OutboxSaveChangesInterceptor"/> dequeues the events of every
/// tracked entity and writes them to the outbox in the same transaction as the entity changes.
/// </summary>
/// <remarks>
/// This is a method rather than a property on purpose, so that EF Core conventions never try to map
/// the event collection. Keep the events in a private field.
/// </remarks>
public interface IHasDomainEvents
{
    /// <summary>Returns the pending domain events and clears them.</summary>
    /// <returns>The events raised since the last call, in the order they were raised.</returns>
    IReadOnlyCollection<object> DequeueDomainEvents();
}
