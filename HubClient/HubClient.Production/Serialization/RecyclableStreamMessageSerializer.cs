using System;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using HubClient.Core.Serialization;
using Microsoft.IO;

namespace HubClient.Production.Serialization
{
    /// <summary>
    /// Message serializer that uses RecyclableMemoryStream for efficient memory management
    /// </summary>
    /// <typeparam name="T">The message type to serialize</typeparam>
    public class RecyclableStreamMessageSerializer<T> : IMessageSerializer<T> where T : IMessage<T>, new()
    {
        private readonly RecyclableMemoryStreamManager _streamManager;
        
        /// <summary>
        /// Creates a new instance of the RecyclableStreamMessageSerializer
        /// </summary>
        /// <param name="streamManager">The recyclable memory stream manager</param>
        public RecyclableStreamMessageSerializer(RecyclableMemoryStreamManager streamManager)
        {
            _streamManager = streamManager ?? throw new ArgumentNullException(nameof(streamManager));
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
            
            // Use recyclable memory stream for the buffer
            using var memoryStream = _streamManager.GetStream();
            await stream.CopyToAsync(memoryStream);
            memoryStream.Position = 0;
            
            T message = new T();
            message.MergeFrom(memoryStream);
            return message;
        }

        /// <summary>
        /// Serializes a message to a byte array
        /// </summary>
        public byte[] Serialize(T message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            
            // Use recyclable memory stream instead of direct byte array
            using var memoryStream = _streamManager.GetStream();
            message.WriteTo((Stream)memoryStream);
            return memoryStream.ToArray();
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
            
            // First serialize to recyclable memory stream
            using var memoryStream = _streamManager.GetStream();
            message.WriteTo((Stream)memoryStream);
            
            // Then copy to destination span
            memoryStream.Position = 0;
            memoryStream.Read(destination);
            
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
            
            // Direct serialization to stream is most efficient
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
            
            // Use recyclable memory stream as intermediate buffer
            using var memoryStream = _streamManager.GetStream();
            message.WriteTo((Stream)memoryStream);
            
            // Reset position and copy to destination stream
            memoryStream.Position = 0;
            await memoryStream.CopyToAsync(stream);
        }
    }
} 