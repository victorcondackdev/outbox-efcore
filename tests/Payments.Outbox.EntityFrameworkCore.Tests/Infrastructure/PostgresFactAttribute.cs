namespace Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

/// <summary>
/// A fact that runs only when the <c>OUTBOX_PG</c> environment variable holds a PostgreSQL
/// connection string (CI provides one through a postgres service container).
/// </summary>
public sealed class PostgresFactAttribute : FactAttribute
{
    public const string EnvironmentVariable = "OUTBOX_PG";

    public PostgresFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
        {
            Skip = "Set " + EnvironmentVariable + " to a PostgreSQL connection string to run integration tests.";
        }
    }

    public static string? ConnectionString => Environment.GetEnvironmentVariable(EnvironmentVariable);
}
