using Microsoft.EntityFrameworkCore;

namespace Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

public sealed record ChargePaid(Guid ChargeId, long AmountInCents);

public sealed record ChargeRefunded(Guid ChargeId, long AmountInCents);

public sealed record UnmappedEvent(string Value);

public static class TestTypes
{
    public const string ChargePaidName = "payments.charge.paid.v1";
    public const string ChargeRefundedName = "payments.charge.refunded.v1";

    public static void Register(OutboxTypeRegistry registry)
    {
        registry
            .Map<ChargePaid>(ChargePaidName, e => e.ChargeId.ToString())
            .Map<ChargeRefunded>(ChargeRefundedName, e => e.ChargeId.ToString());
    }
}

public sealed class Charge : IHasDomainEvents
{
    private readonly List<object> _domainEvents = new();

    public Guid Id { get; set; }

    public string Reference { get; set; } = string.Empty;

    public long AmountInCents { get; set; }

    public string Status { get; set; } = "pending";

    public static Charge Create(string reference, long amountInCents) => new()
    {
        Id = Guid.NewGuid(),
        Reference = reference,
        AmountInCents = amountInCents,
    };

    public void MarkPaid()
    {
        Status = "paid";
        _domainEvents.Add(new ChargePaid(Id, AmountInCents));
    }

    public IReadOnlyCollection<object> DequeueDomainEvents()
    {
        var events = _domainEvents.ToArray();
        _domainEvents.Clear();
        return events;
    }
}

public sealed class LedgerEntry
{
    public Guid Id { get; set; }

    public Guid ChargeId { get; set; }

    public long AmountInCents { get; set; }
}

public sealed class TestDbContext : DbContext
{
    public TestDbContext(DbContextOptions<TestDbContext> options)
        : base(options)
    {
    }

    public DbSet<Charge> Charges => Set<Charge>();

    public DbSet<LedgerEntry> LedgerEntries => Set<LedgerEntry>();

    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Charge>(builder =>
        {
            builder.HasKey(c => c.Id);
            builder.Property(c => c.Reference).HasMaxLength(64);
            builder.Property(c => c.Status).HasMaxLength(32);
            builder.HasIndex(c => c.Reference).IsUnique();
        });

        modelBuilder.Entity<LedgerEntry>(builder => builder.HasKey(e => e.Id));

        modelBuilder.AddOutbox();
        modelBuilder.AddInbox();
    }
}
