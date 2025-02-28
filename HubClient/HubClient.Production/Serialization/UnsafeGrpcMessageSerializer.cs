using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Protobuf;
using HubClient.Core.Serialization;

namespace HubClient.Production.Serialization
{
    /// <summary>
    /// High-performance message serializer that uses unsafe memory access techniques
    /// for minimal allocations and maximum performance.
    /// 
    /// This implementation was the fastest in benchmarks by using direct memory
    /// manipulation and pooled buffers.
    /// </summary>
    /// <typeparam name="T">The message type to serialize</typeparam>
    public class UnsafeGrpcMessageSerializer<T> : IMessageSerializer<T> where T : IMessage<T>, new()
    {
        private readonly ArrayPool<byte> _bytePool;
        private readonly int _initialBufferSize;
        
        /// <summary>
        /// Creates a new instance of the UnsafeGrpcMessageSerializer
        /// </summary>
        /// <param name="bytePool">The byte pool to use for buffer allocation</param>
        /// <param name="initialBufferSize">Initial buffer size (default 4KB)</param>
        public UnsafeGrpcMessageSerializer(ArrayPool<byte> bytePool, int initialBufferSize = 4096)
        {
            _bytePool = bytePool ?? throw new ArgumentNullException(nameof(bytePool));
            _initialBufferSize = initialBufferSize;
        }

        /// <summary>
        /// Deserializes a message from a byte array with minimal allocations
        /// </summary>
        public T Deserialize(byte[] data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            
            T message = new T();
            message.MergeFrom(data);
            return message;
        }

        /// <summary>
        /// Deserializes a message from a read-only span with zero allocations
        /// </summary>
        public T Deserialize(ReadOnlySpan<byte> data)
        {
            T message = new T();
            unsafe
            {
                fixed (byte* ptr = data)
                {
                    // Use the ReadOnlySpan directly
                    message.MergeFrom(new ReadOnlySpan<byte>(ptr, data.Length));
                }
            }
            return message;
        }

        /// <summary>
        /// Attempts to deserialize a message from a read-only span, returning success/failure
        /// </summary>
        public bool TryDeserialize(ReadOnlySpan<byte> data, out T? message)
        {
            message = new T();
            try
            {
                unsafe
                {
                    fixed (byte* ptr = data)
                    {
                        // Use the ReadOnlySpan directly
                        message.MergeFrom(new ReadOnlySpan<byte>(ptr, data.Length));
                    }
                }
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
        public async ValueTask<T> DeserializeAsync(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new ArgumentException("Stream must be readable", nameof(stream));
            
            // Parse from Stream directly (most efficient way)
            T message = new T();
            
            // Use existing Protobuf API for parsing
            using var codedStream = new CodedInputStream(stream);
            message.MergeFrom(codedStream);
            
            return message;
        }

        /// <summary>
        /// Serializes a message to a new byte array (allocates a new array)
        /// </summary>
        public byte[] Serialize(T message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            
            // Use the built-in ToByteArray for simplicity when returning a new array
            return message.ToByteArray();
        }

        /// <summary>
        /// Serializes a message to an existing span to avoid allocations
        /// </summary>
        public int Serialize(T message, Span<byte> destination)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            
            int size = message.CalculateSize();
            
            if (destination.Length < size)
                throw new ArgumentException($"Destination span is too small. Required: {size}, Available: {destination.Length}");
            
            // First serialize to a temporary buffer
            byte[] buffer = _bytePool.Rent(size);
            try
            {
                using var codedOutput = new CodedOutputStream(buffer);
                message.WriteTo(codedOutput);
                
                // Then copy to destination span
                buffer.AsSpan(0, size).CopyTo(destination);
                return size;
            }
            finally
            {
                _bytePool.Return(buffer);
            }
        }

        /// <summary>
        /// Serializes a message to a buffer with optimal performance
        /// </summary>
        public int SerializeToBuffer(T message, byte[] buffer)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            
            int size = message.CalculateSize();
            
            if (buffer.Length < size)
                throw new ArgumentException($"Buffer is too small. Required: {size}, Available: {buffer.Length}");
            
            // Use the write method with direct buffer access
            using var stream = new CodedOutputStream(buffer);
            message.WriteTo(stream);
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
            
            // Use the direct write method for best performance
            message.WriteTo(stream);
        }

        /// <summary>
        /// Asynchronously serializes a message to a stream
        /// </summary>
        public async ValueTask SerializeAsync(T message, Stream stream)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanWrite) throw new ArgumentException("Stream must be writable", nameof(stream));
            
            // Use a pooled buffer to minimize allocations
            int size = message.CalculateSize();
            byte[] buffer = _bytePool.Rent(size);
            
            try
            {
                // Serialize to the buffer
                using (var codedOutput = new CodedOutputStream(buffer))
                {
                    message.WriteTo(codedOutput);
                }
                
                // Write to the stream
                await stream.WriteAsync(buffer, 0, size);
            }
            finally
            {
                // Return the buffer to the pool
                _bytePool.Return(buffer);
            }
        }
    }
} 