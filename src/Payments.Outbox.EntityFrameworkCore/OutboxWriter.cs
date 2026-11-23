using Microsoft.EntityFrameworkCore;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Adds messages to the outbox of a <see cref="DbContext"/>. Messages are inserted by the next
/// <c>SaveChanges</c> call, inside the same transaction as every other pending change.
/// </summary>
public sealed class OutboxWriter
{
    private readonly DbContext _context;
    private readonly OutboxSaveChangesInterceptor _interceptor;

    internal OutboxWriter(DbContext context, OutboxSaveChangesInterceptor interceptor)
    {
        _context = context;
        _interceptor = interceptor;
    }

    /// <summary>Adds a message to the outbox.</summary>
    /// <typeparam name="TMessage">Message type; its runtime type must be mapped in <see cref="OutboxTypeRegistry"/>.</typeparam>
    /// <param name="message">The message.</param>
    /// <param name="options">Optional per-message overrides.</param>
    /// <returns>The tracked outbox row (state <c>Added</c>).</returns>
    /// <exception cref="InvalidOperationException">The message type is not mapped.</exception>
    public OutboxMessage Add<TMessage>(TMessage message, OutboxMessageOptions? options = null)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        var row = _interceptor.CreateMessage(message, options);
        _context.Set<OutboxMessage>().Add(row);
        return row;
    }
}
