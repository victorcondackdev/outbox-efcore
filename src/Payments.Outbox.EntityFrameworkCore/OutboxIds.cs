using System.Security.Cryptography;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Generates time-ordered identifiers with the UUID version 7 layout (RFC 9562).</summary>
internal static class OutboxIds
{
    internal static Guid NewId(DateTimeOffset timestamp)
    {
        Span<byte> random = stackalloc byte[10];
        RandomNumberGenerator.Fill(random);

        var milliseconds = timestamp.ToUnixTimeMilliseconds();

        // Fields are laid out so that Guid.ToString() reads: 48-bit timestamp, version nibble 7,
        // 12 random bits, variant 0b10, 62 random bits.
        var a = (int)(milliseconds >> 16);
        var b = (short)(milliseconds & 0xFFFF);
        var c = (short)(0x7000 | (((random[0] & 0x0F) << 8) | random[1]));
        var d = (byte)(0x80 | (random[2] & 0x3F));

        return new Guid(a, b, c, d, random[3], random[4], random[5], random[6], random[7], random[8], random[9]);
    }
}
