using Google.Protobuf;
using Microsoft.IO;
using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;

namespace HubClient.Core.Serialization
{
    /// <summary>
    /// Optimized Protobuf serializer implementation using buffer pooling to reduce allocations
    /// This is Approach A from the implementation steps
    /// </summary>
    /// <typeparam name="T">The message type to serialize/deserialize</typeparam>
    public class PooledBufferSerializer<T> : IMessageSerializer<T> where T : IMessage<T>, new()
    {
        private readonly MessageParser<T> _parser;
        private readonly RecyclableMemoryStreamManager _streamManager;
        private readonly int _initialBufferSize;
        private readonly int _maximumBufferSize;

        /// <summary>
        /// Creates a new instance of the pooled buffer serializer
        /// </summary>
        /// <param name="initialBufferSize">Initial buffer size (default: 8KB)</param>
        /// <param name="maximumBufferSize">Maximum buffer size (default: 1MB)</param>
        public PooledBufferSerializer(int initialBufferSize = 8192, int maximumBufferSize = 1048576)
        {
            _parser = new MessageParser<T>(() => new T());
            _initialBufferSize = initialBufferSize;
            _maximumBufferSize = maximumBufferSize;
            
            // The Microsoft.IO.RecyclableMemoryStream library might have different constructor parameters
            // depending on the version, so we'll use the simplest constructor
            _streamManager = new RecyclableMemoryStreamManager();
        }

        /// <inheritdoc />
        public byte[] Serialize(T message)
        {
            using var stream = _streamManager.GetStream();
            WriteMessageToStream(message, stream);
            return stream.ToArray();
        }

        /// <inheritdoc />
        public int Serialize(T message, Span<byte> buffer)
        {
            using var stream = _streamManager.GetStream();
            WriteMessageToStream(message, stream);
            
            if (stream.Length > buffer.Length)
            {
                throw new ArgumentException($"Buffer too small. Required {stream.Length} bytes, but buffer only has {buffer.Length} bytes.");
            }

            stream.Position = 0;
            int bytesRead = stream.Read(buffer);
            return bytesRead;
        }

        /// <inheritdoc />
        public void Serialize(T message, Stream stream)
        {
            WriteMessageToStream(message, stream);
        }

        /// <inheritdoc />
        public ValueTask SerializeAsync(T message, Stream stream)
        {
            WriteMessageToStream(message, stream);
            return ValueTask.CompletedTask;
        }
        
        /// <summary>
        /// Helper method to write message to stream to avoid ambiguous method calls
        /// </summary>
        private void WriteMessageToStream(T message, Stream stream)
        {
            // Call the specific overload to avoid ambiguity
            message.WriteTo(stream);
        }

        /// <inheritdoc />
        public T Deserialize(byte[] data)
        {
            return _parser.ParseFrom(data);
        }

        /// <inheritdoc />
        public T Deserialize(ReadOnlySpan<byte> data)
        {
            using var stream = _streamManager.GetStream();
            stream.Write(data);
            stream.Position = 0;
            return _parser.ParseFrom(stream);
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
                using var stream = _streamManager.GetStream();
                stream.Write(data);
                stream.Position = 0;
                message = _parser.ParseFrom(stream);
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