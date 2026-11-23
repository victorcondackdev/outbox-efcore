using System.Diagnostics.CodeAnalysis;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Explicit allow-list mapping CLR message types to stable contract names such as
/// <c>payments.charge.paid.v1</c>.
/// </summary>
/// <remarks>
/// <para>
/// The outbox never stores CLR type names and never resolves types from data. Polymorphic
/// deserialization driven by a type name embedded in the payload (<c>$type</c>, assembly-qualified
/// names) lets whoever can write a row choose which class gets instantiated, which is the classic
/// deserialization gadget vector. Here only types registered at startup can ever be produced, and
/// the wire name is a versioned contract that survives class renames and namespace moves.
/// </para>
/// <para>Configure the registry at startup; it is read-only afterwards and safe for concurrent reads.</para>
/// </remarks>
public sealed class OutboxTypeRegistry
{
    private readonly Dictionary<Type, Registration> _byType = new();
    private readonly Dictionary<string, Type> _byName = new(StringComparer.Ordinal);

    /// <summary>Registered contract names.</summary>
    public IReadOnlyCollection<string> TypeNames => _byName.Keys;

    /// <summary>Maps <typeparamref name="TMessage"/> to a stable contract name.</summary>
    /// <typeparam name="TMessage">The CLR message type.</typeparam>
    /// <param name="typeName">Stable, versioned contract name, for example <c>payments.charge.paid.v1</c>.</param>
    /// <param name="partitionKey">
    /// Optional selector of the ordering scope (typically the aggregate identifier). It is also used
    /// as the default aggregate identifier.
    /// </param>
    /// <returns>The same registry, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The type or the name is already mapped.</exception>
    public OutboxTypeRegistry Map<TMessage>(string typeName, Func<TMessage, string?>? partitionKey = null)
        where TMessage : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(typeName);

        var clrType = typeof(TMessage);
        if (_byType.ContainsKey(clrType))
        {
            throw new InvalidOperationException("Message type '" + clrType.FullName + "' is already mapped.");
        }

        if (_byName.TryGetValue(typeName, out var existing))
        {
            throw new InvalidOperationException(
                "Contract name '" + typeName + "' is already mapped to '" + existing.FullName + "'.");
        }

        Func<object, string?>? selector = null;
        if (partitionKey is not null)
        {
            Func<TMessage, string?> typed = partitionKey;
            selector = message => typed((TMessage)message);
        }

        _byType.Add(clrType, new Registration(typeName, selector));
        _byName.Add(typeName, clrType);
        return this;
    }

    /// <summary>Gets the contract name mapped to a CLR type.</summary>
    /// <param name="type">The CLR type.</param>
    /// <param name="typeName">The contract name, when mapped.</param>
    /// <returns><see langword="true"/> when the type is mapped.</returns>
    public bool TryGetTypeName(Type type, [NotNullWhen(true)] out string? typeName)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (_byType.TryGetValue(type, out var registration))
        {
            typeName = registration.TypeName;
            return true;
        }

        typeName = null;
        return false;
    }

    /// <summary>Gets the CLR type mapped to a contract name.</summary>
    /// <param name="typeName">The contract name.</param>
    /// <param name="type">The CLR type, when mapped.</param>
    /// <returns><see langword="true"/> when the name is mapped.</returns>
    public bool TryGetType(string typeName, [NotNullWhen(true)] out Type? type)
    {
        ArgumentNullException.ThrowIfNull(typeName);
        if (_byName.TryGetValue(typeName, out var found))
        {
            type = found;
            return true;
        }

        type = null;
        return false;
    }

    internal string? GetPartitionKey(object message)
    {
        return _byType.TryGetValue(message.GetType(), out var registration) && registration.PartitionKey is not null
            ? registration.PartitionKey(message)
            : null;
    }

    private sealed record Registration(string TypeName, Func<object, string?>? PartitionKey);
}
