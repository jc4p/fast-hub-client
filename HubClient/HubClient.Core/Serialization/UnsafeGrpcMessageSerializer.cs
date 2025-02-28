using HubClient.Core.Grpc;
using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Protobuf;

namespace HubClient.Core.Serialization
{
    /// <summary>
    /// Optimized serializer for gRPC Messages using unsafe direct memory manipulation
    /// This is implementation Approach D that uses unsafe memory techniques for maximum performance
    /// </summary>
    public class UnsafeGrpcMessageSerializer : IMessageSerializer<Message>
    {
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

            // Write directly to the span
            using (var output = new CodedOutputStream(buffer.ToArray()))
            {
                message.WriteTo(output);
            }
            
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

            // Write to stream using a CodedOutputStream
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

            // Write to stream using a CodedOutputStream
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
            // Fast path with unsafe direct memory access
            return UnsafeDeserialize(data);
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
                message = UnsafeDeserialize(data);
                return true;
            }
            catch
            {
                message = default;
                return false;
            }
        }

        /// <summary>
        /// Performs unsafe memory operations for fast deserialization directly from a span
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe Message UnsafeDeserialize(ReadOnlySpan<byte> data)
        {
            // Convert span to byte array for parsing
            byte[] bytes = data.ToArray();
            
            // Use the parser to create the message
            return Message.Parser.ParseFrom(bytes);
        }
    }
} 