using System;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using HubClient.Core.Serialization;

namespace HubClient.Production.Serialization
{
    /// <summary>
    /// Standard message serializer with no optimizations (for comparison/baseline)
    /// </summary>
    /// <typeparam name="T">The message type to serialize</typeparam>
    public class StandardMessageSerializer<T> : IMessageSerializer<T> where T : IMessage<T>, new()
    {
        /// <summary>
        /// Deserializes a message from a byte array
        /// </summary>
        public T Deserialize(byte[] data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            
            T message = new T();
            message.MergeFrom(data);
            return message;
        }

        /// <summary>
        /// Deserializes a message from a read-only span
        /// </summary>
        public T Deserialize(ReadOnlySpan<byte> data)
        {
            T message = new T();
            message.MergeFrom(data);
            return message;
        }

        /// <summary>
        /// Attempts to deserialize a message, returning success/failure
        /// </summary>
        public bool TryDeserialize(ReadOnlySpan<byte> data, out T? message)
        {
            message = new T();
            try
            {
                message.MergeFrom(data);
                return true;
            }
            catch
            {
                message = default;
                return false;
            }
        }

        /// <summary>
        /// Deserializes a message from a stream
        /// </summary>
        public T Deserialize(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new ArgumentException("Stream must be readable", nameof(stream));
            
            T message = new T();
            message.MergeFrom(stream);
            return message;
        }

        /// <summary>
        /// Asynchronously deserializes a message from a stream
        /// </summary>
        public ValueTask<T> DeserializeAsync(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new ArgumentException("Stream must be readable", nameof(stream));
            
            // No async API in Protobuf, so just do it synchronously
            T message = new T();
            message.MergeFrom(stream);
            return new ValueTask<T>(message);
        }

        /// <summary>
        /// Serializes a message to a new byte array
        /// </summary>
        public byte[] Serialize(T message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            
            return message.ToByteArray();
        }

        /// <summary>
        /// Serializes a message to an existing span
        /// </summary>
        public int Serialize(T message, Span<byte> destination)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            
            int size = message.CalculateSize();
            
            if (destination.Length < size)
                throw new ArgumentException($"Destination span is too small. Required: {size}, Available: {destination.Length}");
            
            // Create a temporary buffer that we can convert to CodedOutputStream
            byte[] buffer = new byte[destination.Length];
            using var stream = new CodedOutputStream(buffer);
            message.WriteTo(stream);
            // Copy the result to the destination span
            buffer.AsSpan(0, size).CopyTo(destination);
            return size;
        }

        /// <summary>
        /// Serializes a message to a stream
        /// </summary>
        public void Serialize(T message, Stream stream)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanWrite) throw new ArgumentException("Stream must be writable", nameof(stream));
            
            message.WriteTo(stream);
        }

        /// <summary>
        /// Asynchronously serializes a message to a stream
        /// </summary>
        public ValueTask SerializeAsync(T message, Stream stream)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanWrite) throw new ArgumentException("Stream must be writable", nameof(stream));
            
            // No async API in Protobuf, so just do it synchronously
            message.WriteTo(stream);
            return ValueTask.CompletedTask;
        }
    }
} 