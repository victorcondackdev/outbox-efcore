using Microsoft.EntityFrameworkCore;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Consumer-side de-duplication. Brokers and the outbox relay deliver at least once; wrapping a
/// handler in <c>TryProcessOnceAsync</c> turns that into effectively-once processing for handlers
/// whose side effects live in the same database.
/// </summary>
public static class InboxDbContextExtensions
{
    /// <summary>Consumer name used by the overload that does not take one.</summary>
    public const string DefaultConsumer = "default";

    /// <summary>
    /// Runs <paramref name="handler"/> and records <paramref name="messageId"/> for the default
    /// consumer in one transaction, unless the message was already processed.
    /// </summary>
    /// <param name="context">The context. The model must include <see cref="OutboxModelBuilderExtensions.AddInbox"/>.</param>
    /// <param name="messageId">Identifier of the incoming message.</param>
    /// <param name="handler">The handler. It should only stage changes on <paramref name="context"/>; they are saved for it.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> when the handler ran and committed; <see langword="false"/> for a duplicate.</returns>
    public static Task<bool> TryProcessOnceAsync(
        this DbContext context,
        string messageId,
        Func<CancellationToken, Task> handler,
        CancellationToken cancellationToken = default) =>
        TryProcessOnceAsync(context, messageId, DefaultConsumer, handler, cancellationToken);

    /// <summary>
    /// Runs <paramref name="handler"/> and records (<paramref name="messageId"/>, <paramref name="consumer"/>)
    /// in the inbox in one transaction, unless that pair was already recorded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The handler's staged changes, any outbox messages it writes and the inbox record are saved by a
    /// single <c>SaveChanges</c> inside a transaction. If two instances race on the same message, the
    /// inbox primary key rejects the second one, its transaction is rolled back and the method returns
    /// <see langword="false"/>. On rollback the change tracker is cleared, because the staged entities
    /// were never persisted.
    /// </para>
    /// <para>
    /// When the context already has an active transaction, the method joins it and does not commit;
    /// a concurrent duplicate then surfaces as a <see cref="DbUpdateException"/> from this call or
    /// from the caller's commit.
    /// </para>
    /// </remarks>
    /// <param name="context">The context. The model must include <see cref="OutboxModelBuilderExtensions.AddInbox"/>.</param>
    /// <param name="messageId">Identifier of the incoming message.</param>
    /// <param name="consumer">Logical consumer name.</param>
    /// <param name="handler">The handler. It should only stage changes on <paramref name="context"/>; they are saved for it.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> when the handler ran and committed; <see langword="false"/> for a duplicate.</returns>
    public static async Task<bool> TryProcessOnceAsync(
        this DbContext context,
        string messageId,
        string consumer,
        Func<CancellationToken, Task> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ArgumentNullException.ThrowIfNull(handler);

        if (context.Model.FindEntityType(typeof(InboxMessage)) is null)
        {
            throw new InvalidOperationException(
                "InboxMessage is not part of the model of " + context.GetType().Name +
                ". Call modelBuilder.AddInbox() in OnModelCreating.");
        }

        if (await IsProcessedAsync(context, messageId, consumer, cancellationToken))
        {
            return false;
        }

        var timeProvider = OutboxDbContextExtensions.FindInterceptor(context)?.TimeProvider ?? TimeProvider.System;

        if (context.Database.CurrentTransaction is not null)
        {
            await handler(cancellationToken);
            context.Set<InboxMessage>().Add(NewRecord(messageId, consumer, timeProvider));
            await context.SaveChangesAsync(cancellationToken);
            return true;
        }

        var strategy = context.Database.CreateExecutionStrategy();
        return await strategy.ExecuteAsync(async () =>
        {
            await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);
            try
            {
                await handler(cancellationToken);
                context.Set<InboxMessage>().Add(NewRecord(messageId, consumer, timeProvider));
                await context.SaveChangesAsync(cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return true;
            }
            catch (DbUpdateException)
            {
                await transaction.RollbackAsync(CancellationToken.None);
                context.ChangeTracker.Clear();
                if (await IsProcessedAsync(context, messageId, consumer, CancellationToken.None))
                {
                    return false;
                }

                throw;
            }
            catch
            {
                context.ChangeTracker.Clear();
                throw;
            }
        });
    }

    private static InboxMessage NewRecord(string messageId, string consumer, TimeProvider timeProvider) => new()
    {
        MessageId = messageId,
        Consumer = consumer,
        ProcessedAt = timeProvider.GetUtcNow().UtcDateTime,
    };

    private static Task<bool> IsProcessedAsync(DbContext context, string messageId, string consumer, CancellationToken cancellationToken) =>
        context.Set<InboxMessage>()
            .AsNoTracking()
            .AnyAsync(m => m.MessageId == messageId && m.Consumer == consumer, cancellationToken);
}
