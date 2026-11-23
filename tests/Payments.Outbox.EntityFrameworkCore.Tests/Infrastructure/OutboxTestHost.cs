using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Npgsql;

namespace Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

/// <summary>
/// A DI container with a real relational database (a temporary SQLite file, or a throw-away
/// PostgreSQL database), a frozen clock and an in-memory publisher.
/// </summary>
public sealed class OutboxTestHost : IAsyncDisposable
{
    private readonly Action<OutboxRelayOptions> _configureRelay;
    private readonly Func<Task> _cleanup;

    private OutboxTestHost(
        ServiceProvider services,
        FakeTimeProvider time,
        InMemoryPublisher publisher,
        Action<OutboxRelayOptions> configureRelay,
        Func<Task> cleanup)
    {
        Services = services;
        Time = time;
        Publisher = publisher;
        _configureRelay = configureRelay;
        _cleanup = cleanup;
    }

    public static readonly DateTimeOffset Start = new(2026, 1, 15, 12, 0, 0, TimeSpan.Zero);

    public ServiceProvider Services { get; }

    public FakeTimeProvider Time { get; }

    public InMemoryPublisher Publisher { get; }

    public OutboxProcessor<TestDbContext> Processor => Services.GetRequiredService<OutboxProcessor<TestDbContext>>();

    public OutboxSerializer Serializer => Services.GetRequiredService<OutboxSerializer>();

    public static async Task<OutboxTestHost> CreateSqliteAsync(Action<OutboxRelayOptions>? configureRelay = null)
    {
        var path = Path.Combine(Path.GetTempPath(), "outbox-tests-" + Guid.NewGuid().ToString("N") + ".db");
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Pooling = false,
            DefaultTimeout = 60,
        }.ToString();

        var host = Create(options => options.UseSqlite(connectionString), configureRelay, () =>
        {
            foreach (var file in new[] { path, path + "-wal", path + "-shm" })
            {
                try
                {
                    File.Delete(file);
                }
                catch (IOException)
                {
                    // Best effort: temporary files are harmless.
                }
            }

            return Task.CompletedTask;
        });

        await using (var context = host.NewContext())
        {
            await context.Database.EnsureCreatedAsync();
            // WAL lets readers proceed while another relay instance writes.
            await context.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
        }

        return host;
    }

    public static async Task<OutboxTestHost> CreatePostgresAsync(Action<OutboxRelayOptions>? configureRelay = null)
    {
        var baseConnectionString = PostgresFactAttribute.ConnectionString
            ?? throw new InvalidOperationException(PostgresFactAttribute.EnvironmentVariable + " is not set.");
        var connectionString = new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Database = "outbox_test_" + Guid.NewGuid().ToString("N"),
        }.ConnectionString;

        OutboxTestHost? created = null;
        var host = Create(options => options.UseNpgsql(connectionString), configureRelay, async () =>
        {
            if (created is null)
            {
                return;
            }

            await using var context = created.NewContext();
            await context.Database.EnsureDeletedAsync();
        });
        created = host;

        await using (var context = host.NewContext())
        {
            await context.Database.EnsureCreatedAsync();
        }

        return host;
    }

    private static OutboxTestHost Create(
        Action<DbContextOptionsBuilder> configureProvider,
        Action<OutboxRelayOptions>? configureRelay,
        Func<Task> cleanup)
    {
        var time = new FakeTimeProvider(Start);
        var publisher = new InMemoryPublisher();

        void ConfigureRelay(OutboxRelayOptions options)
        {
            options.InstanceId = "relay-1";
            options.RetryJitter = 0;
            options.BaseRetryDelay = TimeSpan.FromSeconds(1);
            options.MaxRetryDelay = TimeSpan.FromMinutes(1);
            options.BatchSize = 50;
            configureRelay?.Invoke(options);
        }

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(time);
        services.AddSingleton<IOutboxPublisher>(publisher);
        services.AddOutbox(TestTypes.Register).AddRelay<TestDbContext>(ConfigureRelay);
        services.AddDbContext<TestDbContext>(
            (serviceProvider, options) =>
            {
                configureProvider(options);
                options.UseOutbox(serviceProvider);
            },
            ServiceLifetime.Scoped,
            ServiceLifetime.Singleton);

        return new OutboxTestHost(services.BuildServiceProvider(), time, publisher, ConfigureRelay, cleanup);
    }

    public TestDbContext NewContext() => new(Services.GetRequiredService<DbContextOptions<TestDbContext>>());

    /// <summary>A second, independent relay instance sharing the same database.</summary>
    public OutboxProcessor<TestDbContext> CreateProcessor(string instanceId)
    {
        var options = new OutboxRelayOptions();
        _configureRelay(options);
        options.InstanceId = instanceId;
        return new OutboxProcessor<TestDbContext>(
            Services.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(options),
            Time,
            NullLogger<OutboxProcessor<TestDbContext>>.Instance);
    }

    /// <summary>Writes one message in its own unit of work (one SaveChanges, so one sequence value).</summary>
    public async Task<OutboxMessage> WriteAsync(object message, OutboxMessageOptions? options = null)
    {
        await using var context = NewContext();
        var row = context.Outbox().Add(message, options);
        await context.SaveChangesAsync();
        return row;
    }

    public async Task<List<OutboxMessage>> GetMessagesAsync()
    {
        await using var context = NewContext();
        return await context.OutboxMessages.AsNoTracking().OrderBy(m => m.Sequence).ToListAsync();
    }

    public async Task<OutboxMessage> GetMessageAsync(Guid id)
    {
        await using var context = NewContext();
        return await context.OutboxMessages.AsNoTracking().SingleAsync(m => m.Id == id);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _cleanup();
        }
        catch (Exception)
        {
            // Best effort: a leftover temporary database must not fail the test run.
        }
        finally
        {
            await Services.DisposeAsync();
        }
    }
}
