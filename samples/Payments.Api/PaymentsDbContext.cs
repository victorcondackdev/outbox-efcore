using Microsoft.EntityFrameworkCore;
using Payments.Outbox.EntityFrameworkCore;

namespace Payments.Api;

public sealed class PaymentsDbContext : DbContext
{
    public PaymentsDbContext(DbContextOptions<PaymentsDbContext> options)
        : base(options)
    {
    }

    public DbSet<Charge> Charges => Set<Charge>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Charge>(builder =>
        {
            builder.HasKey(c => c.Id);
            builder.Property(c => c.Currency).HasMaxLength(3);
            builder.Property(c => c.Status).HasMaxLength(16);
        });

        modelBuilder.AddOutbox();
    }
}
