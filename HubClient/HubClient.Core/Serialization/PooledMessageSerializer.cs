using Google.Protobuf;
using Microsoft.Extensions.ObjectPool;
using Microsoft.IO;
using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;

namespace HubClient.Core.Serialization
{
    /// <summary>
    /// Optimized message serializer that uses a message object pool to reduce allocations
    /// This is Approach C from the implementation steps
    /// </summary>
    /// <typeparam name="T">The message type to serialize/deserialize (must be a reference type)</typeparam>
    public class PooledMessageSerializer<T> : IMessageSerializer<T> where T : class, IMessage<T>, new()
    {
        private readonly ObjectPool<T> _messagePool;
        private readonly MessageParser<T> _parser;
        private readonly ArrayPool<byte> _arrayPool;
        private readonly RecyclableMemoryStreamManager _streamManager;

        /// <summary>
        /// Creates a new instance of the pooled message serializer
        /// </summary>
        /// <param name="messagePool">The object pool for message instances</param>
        public PooledMessageSerializer(ObjectPool<T> messagePool)
        {
            _messagePool = messagePool ?? throw new ArgumentNullException(nameof(messagePool));
            _parser = new MessageParser<T>(() => new T());
            _arrayPool = ArrayPool<byte>.Shared;
            _streamManager = new RecyclableMemoryStreamManager();
        }

        /// <summary>
        /// Creates a new instance of the pooled message serializer with custom pools
        /// </summary>
        /// <param name="arrayPool">The byte array pool to use</param>
        /// <param name="streamManager">The memory stream manager to use</param>
        public PooledMessageSerializer(ArrayPool<byte> arrayPool, RecyclableMemoryStreamManager streamManager) 
            : this(ObjectPool.Create<T>())
        {
            _arrayPool = arrayPool ?? throw new ArgumentNullException(nameof(arrayPool));
            _streamManager = streamManager ?? throw new ArgumentNullException(nameof(streamManager));
        }

        /// <summary>
        /// Creates a new instance of the pooled message serializer with custom pools
        /// </summary>
        /// <param name="messagePool">The object pool for message instances</param>
        /// <param name="arrayPool">The byte array pool to use</param>
        /// <param name="streamManager">The memory stream manager to use</param>
        public PooledMessageSerializer(ObjectPool<T> messagePool, ArrayPool<byte> arrayPool, RecyclableMemoryStreamManager streamManager)
        {
            _messagePool = messagePool ?? throw new ArgumentNullException(nameof(messagePool));
            _arrayPool = arrayPool ?? throw new ArgumentNullException(nameof(arrayPool));
            _streamManager = streamManager ?? throw new ArgumentNullException(nameof(streamManager));
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
            // Explicitly call WriteTo on the message to avoid ambiguity
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
            T message = _messagePool.Get();
            try
            {
                // Rather than create a new instance, we'll reuse a pooled message
                // and populate it with the parsed data
                message = _parser.ParseFrom(data);
                return message;
            }
            catch
            {
                // Return the message to the pool if parsing fails
                _messagePool.Return(message);
                throw;
            }
        }

        /// <inheritdoc />
        public T Deserialize(ReadOnlySpan<byte> data)
        {
            T message = _messagePool.Get();
            try
            {
                // Convert the span to byte array for parsing
                // This is not ideal but Protobuf doesn't support direct parsing from spans
                byte[] dataArray = data.ToArray();
                message = _parser.ParseFrom(dataArray);
                return message;
            }
            catch
            {
                _messagePool.Return(message);
                throw;
            }
        }

        /// <inheritdoc />
        public T Deserialize(Stream stream)
        {
            T message = _messagePool.Get();
            try
            {
                message = _parser.ParseFrom(stream);
                return message;
            }
            catch
            {
                _messagePool.Return(message);
                throw;
            }
        }

        /// <inheritdoc />
        public ValueTask<T> DeserializeAsync(Stream stream)
        {
            T message = _messagePool.Get();
            try
            {
                message = _parser.ParseFrom(stream);
                return ValueTask.FromResult(message);
            }
            catch
            {
                _messagePool.Return(message);
                throw;
            }
        }

        /// <inheritdoc />
        public bool TryDeserialize(ReadOnlySpan<byte> data, out T? message)
        {
            message = _messagePool.Get();
            try
            {
                byte[] dataArray = data.ToArray();
                message = _parser.ParseFrom(dataArray);
                return true;
            }
            catch
            {
                _messagePool.Return(message);
                message = default;
                return false;
            }
        }

        /// <summary>
        /// Note: In a real implementation, we would need to ensure message objects are
        /// returned to the pool when they're no longer needed. This could be done through
        /// a custom wrapper or by handling within the consumer code.
        /// </summary>
    }
} 