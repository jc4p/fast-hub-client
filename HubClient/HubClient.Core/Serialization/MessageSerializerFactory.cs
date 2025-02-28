using Google.Protobuf;
using Microsoft.Extensions.ObjectPool;
using HubClient.Core.Grpc;
using System;
using System.Collections.Generic;

namespace HubClient.Core.Serialization
{
    /// <summary>
    /// Factory for creating message serializers with different optimization strategies
    /// </summary>
    public static class MessageSerializerFactory
    {
        /// <summary>
        /// Serializer type options
        /// </summary>
        public enum SerializerType
        {
            /// <summary>
            /// Standard Google.Protobuf serialization (baseline)
            /// </summary>
            Standard,
            
            /// <summary>
            /// Serialization with pooled buffers (Approach A)
            /// </summary>
            PooledBuffer,
            
            /// <summary>
            /// Span-based serialization with stackalloc (Approach B)
            /// </summary>
            SpanBased,
            
            /// <summary>
            /// Zero-copy deserialization with message pooling (Approach C)
            /// </summary>
            PooledMessage,
            
            /// <summary>
            /// Unsafe direct memory manipulation (Approach D)
            /// </summary>
            UnsafeMemory
        }
        
        /// <summary>
        /// Creates a new message serializer of the specified type
        /// </summary>
        /// <typeparam name="T">The message type to serialize</typeparam>
        /// <param name="type">The type of serializer to create</param>
        /// <returns>A message serializer implementation</returns>
        public static IMessageSerializer<T> Create<T>(SerializerType type = SerializerType.Standard) where T : IMessage<T>, new()
        {
            return type switch
            {
                SerializerType.Standard => new MessageSerializer<T>(),
                SerializerType.PooledBuffer => new PooledBufferSerializer<T>(),
                SerializerType.SpanBased => typeof(T) == typeof(Message) ? 
                    (IMessageSerializer<T>)new UnsafeGrpcMessageSerializer() : 
                    new MessageSerializer<T>(),
                SerializerType.PooledMessage => CreatePooledMessageSerializerSafe<T>(),
                SerializerType.UnsafeMemory => typeof(T) == typeof(Message) ? 
                    (IMessageSerializer<T>)new UnsafeGrpcMessageSerializer() : 
                    new MessageSerializer<T>(),
                _ => throw new ArgumentException($"Unknown serializer type: {type}", nameof(type))
            };
        }
        
        // Helper method to safely create a PooledMessageSerializer
        private static IMessageSerializer<T> CreatePooledMessageSerializerSafe<T>() where T : IMessage<T>, new()
        {
            if (!typeof(T).IsClass)
            {
                throw new InvalidOperationException($"PooledMessageSerializer requires a reference type, but {typeof(T).Name} is not a class.");
            }
            
            var factory = typeof(PooledMessageSerializerFactory<>)
                .MakeGenericType(typeof(T))
                .GetMethod("Create")
                ?.Invoke(null, null);
                
            if (factory == null)
            {
                throw new InvalidOperationException($"Failed to create pooled message serializer for {typeof(T).Name}");
            }
            
            return (IMessageSerializer<T>)factory;
        }
        
        /// <summary>
        /// Creates the standard serializer (baseline)
        /// </summary>
        public static IMessageSerializer<T> CreateStandard<T>() where T : IMessage<T>, new()
            => new MessageSerializer<T>();
            
        /// <summary>
        /// Creates the pooled buffer serializer (Approach A)
        /// </summary>
        public static IMessageSerializer<T> CreatePooledBuffer<T>(
            int initialBufferSize = 8192, 
            int maximumBufferSize = 1048576) where T : IMessage<T>, new()
            => new PooledBufferSerializer<T>(initialBufferSize, maximumBufferSize);
            
        /// <summary>
        /// Creates the span-based serializer (Approach B)
        /// </summary>
        public static IMessageSerializer<T> CreateSpanBased<T>() where T : IMessage<T>, new()
        {
            if (typeof(T) == typeof(Message))
            {
                return (IMessageSerializer<T>)new UnsafeGrpcMessageSerializer();
            }
            return new MessageSerializer<T>();
        }
            
        /// <summary>
        /// Creates the pooled message serializer (Approach C)
        /// </summary>
        public static IMessageSerializer<T> CreatePooledMessage<T>() where T : class, IMessage<T>, new()
        {
            var objectPool = new DefaultObjectPool<T>(new DefaultPooledObjectPolicy<T>());
            return new PooledMessageSerializer<T>(objectPool);
        }
        
        /// <summary>
        /// Creates the unsafe memory serializer (Approach D)
        /// </summary>
        public static IMessageSerializer<Message> CreateUnsafeMemory()
            => new UnsafeGrpcMessageSerializer();
    }
    
    /// <summary>
    /// Helper factory class with proper generic constraints
    /// </summary>
    internal static class PooledMessageSerializerFactory<T> where T : class, IMessage<T>, new()
    {
        public static IMessageSerializer<T> Create()
        {
            var objectPool = new DefaultObjectPool<T>(new DefaultPooledObjectPolicy<T>());
            return new PooledMessageSerializer<T>(objectPool);
        }
    }
} 