using Google.Protobuf;
using System;
using System.IO;
using System.Threading.Tasks;

namespace HubClient.Core.Serialization
{
    /// <summary>
    /// Interface for message serialization implementations
    /// </summary>
    /// <typeparam name="T">The message type to serialize/deserialize</typeparam>
    public interface IMessageSerializer<T> where T : IMessage<T>
    {
        /// <summary>
        /// Serializes a message to a byte array
        /// </summary>
        byte[] Serialize(T message);

        /// <summary>
        /// Serializes a message to a pre-allocated buffer
        /// </summary>
        /// <param name="message">The message to serialize</param>
        /// <param name="buffer">The buffer to write to</param>
        /// <returns>The number of bytes written</returns>
        int Serialize(T message, Span<byte> buffer);

        /// <summary>
        /// Serializes a message to a stream
        /// </summary>
        /// <param name="message">The message to serialize</param>
        /// <param name="stream">The stream to write to</param>
        void Serialize(T message, Stream stream);

        /// <summary>
        /// Deserializes a message from a byte array
        /// </summary>
        T Deserialize(byte[] data);

        /// <summary>
        /// Deserializes a message from a byte span
        /// </summary>
        T Deserialize(ReadOnlySpan<byte> data);

        /// <summary>
        /// Deserializes a message from a stream
        /// </summary>
        T Deserialize(Stream stream);

        /// <summary>
        /// Tries to deserialize a message from a byte span, returning a success indicator
        /// </summary>
        bool TryDeserialize(ReadOnlySpan<byte> data, out T? message);
        
        /// <summary>
        /// Asynchronously deserializes a message from a stream
        /// </summary>
        /// <param name="stream">The stream to read from</param>
        /// <returns>The deserialized message</returns>
        ValueTask<T> DeserializeAsync(Stream stream);
        
        /// <summary>
        /// Asynchronously serializes a message to a stream
        /// </summary>
        /// <param name="message">The message to serialize</param>
        /// <param name="stream">The stream to write to</param>
        ValueTask SerializeAsync(T message, Stream stream);
    }
} 