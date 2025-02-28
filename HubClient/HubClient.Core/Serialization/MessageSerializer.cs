using Google.Protobuf;
using System;
using System.IO;
using System.Threading.Tasks;

namespace HubClient.Core.Serialization
{
    /// <summary>
    /// Standard implementation of IMessageSerializer using Google Protobuf's built-in serialization
    /// This serves as the baseline implementation
    /// </summary>
    /// <typeparam name="T">The message type to serialize/deserialize</typeparam>
    public class MessageSerializer<T> : IMessageSerializer<T> where T : IMessage<T>, new()
    {
        private readonly MessageParser<T> _parser;

        /// <summary>
        /// Creates a new standard message serializer
        /// </summary>
        public MessageSerializer()
        {
            _parser = new MessageParser<T>(() => new T());
        }

        /// <inheritdoc />
        public byte[] Serialize(T message)
        {
            return message.ToByteArray();
        }

        /// <inheritdoc />
        public int Serialize(T message, Span<byte> buffer)
        {
            byte[] data = message.ToByteArray();
            if (data.Length > buffer.Length)
            {
                throw new ArgumentException($"Buffer too small. Required {data.Length} bytes, but buffer only has {buffer.Length} bytes.");
            }
            
            data.CopyTo(buffer);
            return data.Length;
        }

        /// <inheritdoc />
        public void Serialize(T message, Stream stream)
        {
            message.WriteTo(stream);
        }

        /// <inheritdoc />
        public ValueTask SerializeAsync(T message, Stream stream)
        {
            message.WriteTo(stream);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        public T Deserialize(byte[] data)
        {
            return _parser.ParseFrom(data);
        }

        /// <inheritdoc />
        public T Deserialize(ReadOnlySpan<byte> data)
        {
            return _parser.ParseFrom(data.ToArray());
        }

        /// <inheritdoc />
        public T Deserialize(Stream stream)
        {
            return _parser.ParseFrom(stream);
        }

        /// <inheritdoc />
        public ValueTask<T> DeserializeAsync(Stream stream)
        {
            return ValueTask.FromResult(_parser.ParseFrom(stream));
        }

        /// <inheritdoc />
        public bool TryDeserialize(ReadOnlySpan<byte> data, out T? message)
        {
            try
            {
                message = _parser.ParseFrom(data.ToArray());
                return true;
            }
            catch
            {
                message = default;
                return false;
            }
        }
    }
} 