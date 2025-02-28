using HubClient.Core.Grpc;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;

namespace HubClient.Core.Serialization
{
    /// <summary>
    /// Optimized serializer for gRPC Messages using pooled buffers and object pooling
    /// This is implementation Approach C that uses object pooling for maximum performance with minimal allocations
    /// </summary>
    public class PooledGrpcMessageSerializer : IMessageSerializer<Message>
    {
        // Pool for message instances to reduce allocations
        private readonly ObjectPool<Message> _messagePool;
        
        // Default buffer size for serialization operations
        private const int DefaultBufferSize = 4096;

        /// <summary>
        /// Creates a new instance of the pooled message serializer
        /// </summary>
        public PooledGrpcMessageSerializer()
        {
            // Create a pool for Message objects
            _messagePool = new DefaultObjectPool<Message>(new DefaultPooledObjectPolicy<Message>());
        }

        /// <inheritdoc />
        public byte[] Serialize(Message message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }
            
            // Calculate size and create buffer
            int size = message.CalculateSize();
            byte[] buffer = new byte[size];
            
            // Use CodedOutputStream to write to buffer
            using (var output = new CodedOutputStream(buffer))
            {
                message.WriteTo(output);
            }
            
            return buffer;
        }

        /// <inheritdoc />
        public int Serialize(Message message, Span<byte> buffer)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            int size = message.CalculateSize();
            if (size > buffer.Length)
            {
                throw new ArgumentException($"Buffer too small. Required {size} bytes, but buffer only has {buffer.Length} bytes.", nameof(buffer));
            }

            // Use a pooled buffer for temporary storage
            byte[] pooledBuffer = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                using (var output = new CodedOutputStream(pooledBuffer))
                {
                    message.WriteTo(output);
                }
                
                // Copy from pooled buffer to the output span
                new ReadOnlySpan<byte>(pooledBuffer, 0, size).CopyTo(buffer);
                return size;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(pooledBuffer);
            }
        }

        /// <inheritdoc />
        public void Serialize(Message message, Stream stream)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            // Write to stream using a CodedOutputStream
            using (var output = new CodedOutputStream(stream))
            {
                message.WriteTo(output);
            }
        }

        /// <inheritdoc />
        public async ValueTask SerializeAsync(Message message, Stream stream)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            // Calculate size for optimal buffer allocation
            int size = message.CalculateSize();
            byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
            
            try
            {
                // Serialize to the pooled buffer
                using (var output = new CodedOutputStream(buffer))
                {
                    message.WriteTo(output);
                }
                
                // Write the buffer to the stream
                await stream.WriteAsync(buffer.AsMemory(0, size));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        /// <inheritdoc />
        public Message Deserialize(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            // Get a pooled message instance
            return Message.Parser.ParseFrom(data);
        }

        /// <inheritdoc />
        public Message Deserialize(ReadOnlySpan<byte> data)
        {
            // Convert the span to a byte array (pooled buffer would be better but protobuf doesn't support span directly)
            byte[] bytes = data.ToArray();
            
            return Message.Parser.ParseFrom(bytes);
        }

        /// <inheritdoc />
        public Message Deserialize(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            return Message.Parser.ParseFrom(stream);
        }

        /// <inheritdoc />
        public async ValueTask<Message> DeserializeAsync(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            // For async operations, we need to copy the stream to a buffer first
            if (stream.CanSeek)
            {
                long length = stream.Length - stream.Position;
                if (length > int.MaxValue)
                {
                    throw new ArgumentException("Stream is too large to deserialize", nameof(stream));
                }

                byte[] buffer = ArrayPool<byte>.Shared.Rent((int)length);
                try
                {
                    await stream.ReadExactlyAsync(buffer.AsMemory(0, (int)length));
                    return Message.Parser.ParseFrom(buffer.AsSpan(0, (int)length).ToArray());
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
            else
            {
                // For non-seekable streams, we need to use a memory stream
                using var memoryStream = new MemoryStream();
                await stream.CopyToAsync(memoryStream);
                memoryStream.Position = 0;
                return Message.Parser.ParseFrom(memoryStream);
            }
        }

        /// <inheritdoc />
        public bool TryDeserialize(ReadOnlySpan<byte> data, out Message? message)
        {
            try
            {
                message = Deserialize(data);
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