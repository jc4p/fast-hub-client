using HubClient.Core.Grpc;
using System;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;

namespace HubClient.Core.Serialization
{
    /// <summary>
    /// Standard serializer for gRPC Messages that uses the default protobuf serialization methods
    /// </summary>
    public class StandardGrpcMessageSerializer : IMessageSerializer<Message>
    {
        /// <inheritdoc />
        public byte[] Serialize(Message message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }
            
            // Create a byte array the size of the message
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

            // Convert the span to a byte array
            byte[] tempBuffer = new byte[size];
            using (var output = new CodedOutputStream(tempBuffer))
            {
                message.WriteTo(output);
            }
            
            // Copy the bytes to the span
            tempBuffer.AsSpan().CopyTo(buffer);
            
            return size;
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

            using (var output = new CodedOutputStream(stream))
            {
                message.WriteTo(output);
            }
        }

        /// <inheritdoc />
        public ValueTask SerializeAsync(Message message, Stream stream)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            using (var output = new CodedOutputStream(stream))
            {
                message.WriteTo(output);
            }
            
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        public Message Deserialize(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            return Message.Parser.ParseFrom(data);
        }

        /// <inheritdoc />
        public Message Deserialize(ReadOnlySpan<byte> data)
        {
            // Convert the span to a byte array
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
        public ValueTask<Message> DeserializeAsync(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            return ValueTask.FromResult(Message.Parser.ParseFrom(stream));
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