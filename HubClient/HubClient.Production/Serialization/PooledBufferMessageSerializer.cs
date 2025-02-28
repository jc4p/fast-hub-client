using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using HubClient.Core.Serialization;
using Microsoft.IO;

namespace HubClient.Production.Serialization
{
    /// <summary>
    /// Message serializer that uses pooled buffers for efficient memory management
    /// </summary>
    /// <typeparam name="T">The message type to serialize</typeparam>
    public class PooledBufferMessageSerializer<T> : IMessageSerializer<T> where T : IMessage<T>, new()
    {
        private readonly RecyclableMemoryStreamManager _streamManager;
        private readonly ArrayPool<byte> _bufferPool;
        private readonly int _initialBufferSize;
        
        /// <summary>
        /// Creates a new instance of the PooledBufferMessageSerializer
        /// </summary>
        /// <param name="streamManager">The recyclable memory stream manager</param>
        /// <param name="bufferPool">The buffer pool (defaults to ArrayPool.Shared)</param>
        /// <param name="initialBufferSize">Initial buffer size (default 4KB)</param>
        public PooledBufferMessageSerializer(
            RecyclableMemoryStreamManager streamManager,
            ArrayPool<byte>? bufferPool = null,
            int initialBufferSize = 4096)
        {
            _streamManager = streamManager ?? throw new ArgumentNullException(nameof(streamManager));
            _bufferPool = bufferPool ?? ArrayPool<byte>.Shared;
            _initialBufferSize = initialBufferSize;
        }

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
            
            // Use a pooled buffer to read from the stream first
            byte[] buffer = _bufferPool.Rent((int)stream.Length);
            int bytesRead;
            
            try
            {
                // Read the stream into the buffer
                bytesRead = stream.Read(buffer, 0, (int)stream.Length);
                
                // Parse the message
                T message = new T();
                message.MergeFrom(new ReadOnlySpan<byte>(buffer, 0, bytesRead));
                return message;
            }
            finally
            {
                // Return the buffer to the pool
                _bufferPool.Return(buffer);
            }
        }

        /// <summary>
        /// Asynchronously deserializes a message from a stream
        /// </summary>
        public async ValueTask<T> DeserializeAsync(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new ArgumentException("Stream must be readable", nameof(stream));
            
            // Use a recyclable memory stream
            using var memoryStream = _streamManager.GetStream();
            await stream.CopyToAsync(memoryStream);
            memoryStream.Position = 0;
            
            // Get the buffer from the memory stream
            if (memoryStream.TryGetBuffer(out var buffer))
            {
                T message = new T();
                message.MergeFrom(buffer);
                return message;
            }
            else
            {
                // Fallback method if we can't get the buffer directly
                T message = new T();
                memoryStream.Position = 0;
                message.MergeFrom(memoryStream);
                return message;
            }
        }

        /// <summary>
        /// Serializes a message to a new byte array
        /// </summary>
        public byte[] Serialize(T message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            
            int size = message.CalculateSize();
            byte[] buffer = _bufferPool.Rent(size);
            
            try
            {
                // Use the buffer to serialize
                using var stream = new CodedOutputStream(buffer);
                message.WriteTo(stream);
                
                // Create a copy to return (since we need to return the original buffer to the pool)
                byte[] result = new byte[size];
                Buffer.BlockCopy(buffer, 0, result, 0, size);
                return result;
            }
            finally
            {
                // Return the buffer to the pool
                _bufferPool.Return(buffer);
            }
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
            
            // Rent a buffer, write to it, then copy to destination span
            byte[] buffer = _bufferPool.Rent(destination.Length);
            try
            {
                using var codedOutput = new CodedOutputStream(buffer);
                message.WriteTo(codedOutput);
                // Copy the result to the destination span
                buffer.AsSpan(0, size).CopyTo(destination);
                return size;
            }
            finally
            {
                _bufferPool.Return(buffer);
            }
        }

        /// <summary>
        /// Serializes a message to a stream
        /// </summary>
        public void Serialize(T message, Stream stream)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanWrite) throw new ArgumentException("Stream must be writable", nameof(stream));
            
            // Get the size of the message
            int size = message.CalculateSize();
            
            // Rent a buffer from the pool
            byte[] buffer = _bufferPool.Rent(size);
            
            try
            {
                // Serialize to the buffer
                using (var codedStream = new CodedOutputStream(buffer))
                {
                    message.WriteTo(codedStream);
                }
                
                // Write to the stream
                stream.Write(buffer, 0, size);
            }
            finally
            {
                // Return the buffer to the pool
                _bufferPool.Return(buffer);
            }
        }

        /// <summary>
        /// Asynchronously serializes a message to a stream
        /// </summary>
        public async ValueTask SerializeAsync(T message, Stream stream)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanWrite) throw new ArgumentException("Stream must be writable", nameof(stream));
            
            // Get the size of the message
            int size = message.CalculateSize();
            
            // Rent a buffer from the pool
            byte[] buffer = _bufferPool.Rent(size);
            
            try
            {
                // Serialize to the buffer
                using (var codedStream = new CodedOutputStream(buffer))
                {
                    message.WriteTo(codedStream);
                }
                
                // Write to the stream asynchronously
                await stream.WriteAsync(buffer, 0, size);
            }
            finally
            {
                // Return the buffer to the pool
                _bufferPool.Return(buffer);
            }
        }
    }
} 