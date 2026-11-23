namespace Payments.Outbox.EntityFrameworkCore;

internal static class OutboxLimits
{
    internal const int MaxErrorLength = 4000;

    internal static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength];
}
