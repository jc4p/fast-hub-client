using System;
using System.Buffers;
using Google.Protobuf;
using HubClient.Core.Serialization;
using Microsoft.Extensions.ObjectPool;
using Microsoft.IO;

namespace HubClient.Production.Serialization
{
    /// <summary>
    /// Factory for creating serializers with optimal performance characteristics
    /// based on benchmark results
    /// </summary>
    public static class MessageSerializerFactory
    {
        /// <summary>
        /// Creates a serializer for the specified message type with the chosen strategy
        /// </summary>
        /// <typeparam name="T">The message type to serialize</typeparam>
        /// <param name="serializerType">The serializer type to create</param>
        /// <param name="memoryStreamManager">Optional memory stream manager for recyclable stream serializers</param>
        /// <returns>A message serializer implementation</returns>
        public static IMessageSerializer<T> Create<T>(
            HubClient.Core.Serialization.MessageSerializerFactory.SerializerType serializerType,
            RecyclableMemoryStreamManager? memoryStreamManager = null) 
            where T : IMessage<T>, new()
        {
            // Create memory stream manager if needed and not provided
            memoryStreamManager ??= new RecyclableMemoryStreamManager(
                new RecyclableMemoryStreamManager.Options
                {
                    BlockSize = 4096,
                    LargeBufferMultiple = 4096,
                    MaximumBufferSize = 4 * 1024 * 1024
                });
                
            // Create the serializer based on the specified type
            return serializerType switch
            {
                HubClient.Core.Serialization.MessageSerializerFactory.SerializerType.UnsafeMemory => 
                    new UnsafeGrpcMessageSerializer<T>(ArrayPool<byte>.Shared),
                    
                HubClient.Core.Serialization.MessageSerializerFactory.SerializerType.PooledBuffer => 
                    new PooledBufferMessageSerializer<T>(memoryStreamManager),
                    
                HubClient.Core.Serialization.MessageSerializerFactory.SerializerType.Standard => 
                    new StandardMessageSerializer<T>(),
                    
                _ => throw new ArgumentException($"Unsupported serializer type: {serializerType}")
            };
        }
    }
} 