namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Exponential backoff with downward jitter.</summary>
internal static class RetryPolicy
{
    /// <param name="failedAttempts">Number of failures so far, including the one just observed (1-based).</param>
    /// <param name="options">Relay options.</param>
    /// <param name="sample">Uniform random sample in [0, 1).</param>
    internal static TimeSpan ComputeDelay(int failedAttempts, OutboxRelayOptions options, double sample)
    {
        var exponent = Math.Clamp(failedAttempts - 1, 0, 30);
        var raw = options.BaseRetryDelay.TotalMilliseconds * Math.Pow(2, exponent);
        var capped = Math.Min(raw, options.MaxRetryDelay.TotalMilliseconds);
        var jittered = capped * (1 - (options.RetryJitter * sample));
        return TimeSpan.FromMilliseconds(Math.Max(0, jittered));
    }
}
