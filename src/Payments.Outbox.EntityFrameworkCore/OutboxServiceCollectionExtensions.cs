using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Registers the outbox in a service collection.</summary>
public static class OutboxServiceCollectionExtensions
{
    /// <summary>
    /// Registers the write side of the outbox: the type registry, the serializer and the
    /// <see cref="OutboxSaveChangesInterceptor"/>. Then configure the context with
    /// <see cref="OutboxDbContextExtensions.UseOutbox(DbContextOptionsBuilder, IServiceProvider)"/> and, in
    /// the process that relays messages, call <see cref="OutboxBuilder.AddRelay{TContext}"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureTypes">Maps every message type to a stable contract name.</param>
    /// <param name="configure">Optional write-side options.</param>
    /// <returns>A builder for further outbox configuration.</returns>
    public static OutboxBuilder AddOutbox(
        this IServiceCollection services,
        Action<OutboxTypeRegistry> configureTypes,
        Action<OutboxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureTypes);

        var registry = new OutboxTypeRegistry();
        configureTypes(registry);

        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton(registry);
        services.AddOptions<OutboxOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<OutboxSerializer>();
        services.TryAddSingleton<OutboxSaveChangesInterceptor>();
        return new OutboxBuilder(services);
    }
}

/// <summary>Fluent configuration returned by <see cref="OutboxServiceCollectionExtensions.AddOutbox"/>.</summary>
public sealed class OutboxBuilder
{
    internal OutboxBuilder(IServiceCollection services) => Services = services;

    /// <summary>The service collection.</summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Registers <see cref="OutboxProcessor{TContext}"/> and the <see cref="OutboxRelayService{TContext}"/>
    /// hosted service that publishes and cleans up the outbox of <typeparamref name="TContext"/>.
    /// An <see cref="IOutboxPublisher"/> must also be registered (see <see cref="AddPublisher{TPublisher}"/>).
    /// </summary>
    /// <typeparam name="TContext">The context that owns the outbox table.</typeparam>
    /// <param name="configure">Optional relay options.</param>
    /// <returns>The same builder.</returns>
    public OutboxBuilder AddRelay<TContext>(Action<OutboxRelayOptions>? configure = null)
        where TContext : DbContext
    {
        Services.AddOptions<OutboxRelayOptions>();
        if (configure is not null)
        {
            Services.Configure(configure);
        }

        Services.TryAddSingleton<OutboxProcessor<TContext>>();
        Services.AddHostedService<OutboxRelayService<TContext>>();
        return this;
    }

    /// <summary>Registers <typeparamref name="TPublisher"/> as the scoped <see cref="IOutboxPublisher"/>.</summary>
    /// <typeparam name="TPublisher">The publisher implementation.</typeparam>
    /// <returns>The same builder.</returns>
    public OutboxBuilder AddPublisher<TPublisher>()
        where TPublisher : class, IOutboxPublisher
    {
        Services.TryAddScoped<IOutboxPublisher, TPublisher>();
        return this;
    }
}
