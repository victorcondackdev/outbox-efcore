using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Outbox extensions for <see cref="DbContext"/> and <see cref="DbContextOptionsBuilder"/>.</summary>
public static class OutboxDbContextExtensions
{
    /// <summary>
    /// Returns a writer that adds messages to this context's outbox, to be saved atomically with the
    /// other changes of the unit of work.
    /// </summary>
    /// <param name="context">The context.</param>
    /// <returns>An outbox writer bound to <paramref name="context"/>.</returns>
    /// <exception cref="InvalidOperationException">
    /// The model does not contain the outbox, or the context was not configured with
    /// <see cref="UseOutbox(DbContextOptionsBuilder, IServiceProvider)"/>.
    /// </exception>
    public static OutboxWriter Outbox(this DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        EnsureOutboxMapped(context);
        var interceptor = FindInterceptor(context)
            ?? throw new InvalidOperationException(
                "The outbox interceptor is not registered on this DbContext. Configure the context with " +
                "options.UseOutbox(serviceProvider) after services.AddOutbox(...).");
        return new OutboxWriter(context, interceptor);
    }

    /// <summary>
    /// Registers <see cref="OutboxSaveChangesInterceptor"/> (resolved from <paramref name="serviceProvider"/>)
    /// on the context options. Use inside <c>AddDbContext((sp, options) =&gt; ...)</c>.
    /// </summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="serviceProvider">The application service provider.</param>
    /// <returns>The same options builder.</returns>
    public static DbContextOptionsBuilder UseOutbox(this DbContextOptionsBuilder builder, IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(serviceProvider);
        return builder.AddInterceptors(serviceProvider.GetRequiredService<OutboxSaveChangesInterceptor>());
    }

    /// <summary>Registers an explicit <see cref="OutboxSaveChangesInterceptor"/> instance on the context options.</summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="interceptor">The interceptor.</param>
    /// <returns>The same options builder.</returns>
    public static DbContextOptionsBuilder UseOutbox(this DbContextOptionsBuilder builder, OutboxSaveChangesInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(interceptor);
        return builder.AddInterceptors(interceptor);
    }

    internal static OutboxSaveChangesInterceptor? FindInterceptor(DbContext context)
    {
        var options = context.GetService<IDbContextOptions>();
        var interceptors = options.FindExtension<CoreOptionsExtension>()?.Interceptors;
        if (interceptors is null)
        {
            return null;
        }

        foreach (var interceptor in interceptors)
        {
            if (interceptor is OutboxSaveChangesInterceptor outbox)
            {
                return outbox;
            }
        }

        return null;
    }

    internal static void EnsureOutboxMapped(DbContext context)
    {
        if (context.Model.FindEntityType(typeof(OutboxMessage)) is null)
        {
            throw new InvalidOperationException(
                "OutboxMessage is not part of the model of " + context.GetType().Name +
                ". Call modelBuilder.AddOutbox() in OnModelCreating.");
        }
    }
}
