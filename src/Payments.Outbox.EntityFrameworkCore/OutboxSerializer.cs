using System.Text.Json;
using Microsoft.Extensions.Options;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Serializes messages to JSON payloads and back, restricted to the types in
/// <see cref="OutboxTypeRegistry"/>.
/// </summary>
public sealed class OutboxSerializer
{
    private readonly JsonSerializerOptions _jsonOptions;

    /// <summary>Creates a serializer.</summary>
    /// <param name="registry">The type registry.</param>
    /// <param name="options">Outbox options.</param>
    public OutboxSerializer(OutboxTypeRegistry registry, IOptions<OutboxOptions> options)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(options);
        Registry = registry;
        _jsonOptions = options.Value.JsonSerializerOptions;
    }

    /// <summary>The type registry.</summary>
    public OutboxTypeRegistry Registry { get; }

    /// <summary>Serializes a message using its runtime type.</summary>
    /// <param name="message">The message.</param>
    /// <param name="typeName">The contract name of the message type.</param>
    /// <returns>The JSON payload.</returns>
    /// <exception cref="InvalidOperationException">The runtime type is not mapped.</exception>
    public string Serialize(object message, out string typeName)
    {
        ArgumentNullException.ThrowIfNull(message);
        var clrType = message.GetType();
        if (!Registry.TryGetTypeName(clrType, out var name))
        {
            throw new InvalidOperationException(
                "Message type '" + clrType.FullName + "' is not mapped. Register it with " +
                "OutboxTypeRegistry.Map<T>(\"<stable contract name>\") before writing it to the outbox.");
        }

        typeName = name;
        return JsonSerializer.Serialize(message, clrType, _jsonOptions);
    }

    /// <summary>Deserializes a payload into the CLR type mapped to <paramref name="typeName"/>.</summary>
    /// <param name="typeName">The contract name.</param>
    /// <param name="payload">The JSON payload.</param>
    /// <returns>The message.</returns>
    /// <exception cref="InvalidOperationException">The contract name is not mapped.</exception>
    /// <exception cref="JsonException">The payload is invalid.</exception>
    public object Deserialize(string typeName, string payload)
    {
        ArgumentNullException.ThrowIfNull(typeName);
        ArgumentNullException.ThrowIfNull(payload);
        if (!Registry.TryGetType(typeName, out var clrType))
        {
            throw new InvalidOperationException("Contract name '" + typeName + "' is not mapped.");
        }

        return JsonSerializer.Deserialize(payload, clrType, _jsonOptions)
            ?? throw new JsonException("Payload of '" + typeName + "' deserialized to null.");
    }

    /// <summary>Deserializes the payload of an envelope.</summary>
    /// <typeparam name="TMessage">Expected message type.</typeparam>
    /// <param name="envelope">The envelope.</param>
    /// <returns>The message.</returns>
    /// <exception cref="InvalidOperationException">The envelope does not carry a <typeparamref name="TMessage"/>.</exception>
    public TMessage Deserialize<TMessage>(OutboxEnvelope envelope)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var message = Deserialize(envelope.Type, envelope.Payload);
        return message as TMessage
            ?? throw new InvalidOperationException(
                "Envelope of type '" + envelope.Type + "' does not carry a " + typeof(TMessage).FullName + ".");
    }
}
