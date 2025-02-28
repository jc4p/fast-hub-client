using Grpc.Net.Client;
using HubClient.Core.Resilience;
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace HubClient.Core
{
    /// <summary>
    /// Optimized implementation of gRPC connection management with custom SocketsHttpHandler 
    /// and optimized connection pooling (Approach A in the implementation steps)
    /// </summary>
    public class OptimizedGrpcConnectionManager : IGrpcConnectionManager
    {
        private readonly GrpcChannel _channel;
        private readonly string _serverEndpoint;
        private readonly SocketsHttpHandler _httpHandler;
        private readonly int _maxConnections;
        private readonly Lazy<IGrpcResiliencePolicy> _defaultResiliencePolicy;
        private bool _disposed;

        /// <summary>
        /// Creates a new instance of OptimizedGrpcConnectionManager with specified maximum connections
        /// </summary>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        /// <param name="maxConnections">The maximum number of connections to pool (default: 20)</param>
        public OptimizedGrpcConnectionManager(string serverEndpoint, int maxConnections = 20)
        {
            _serverEndpoint = serverEndpoint ?? throw new ArgumentNullException(nameof(serverEndpoint));
            _maxConnections = maxConnections > 0 ? maxConnections : throw new ArgumentOutOfRangeException(nameof(maxConnections), "Maximum connections must be greater than zero");
            
            // Create an optimized SocketsHttpHandler with connection pooling
            _httpHandler = new SocketsHttpHandler
            {
                // Configure pooling parameters
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),       // Keep connections alive for 5 minutes
                MaxConnectionsPerServer = _maxConnections,                   // Set max connections per server
                EnableMultipleHttp2Connections = true,                       // Allow multiple HTTP/2 connections to same endpoint
                KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests, // Send keep-alive pings during request activity
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),              // Send keep-alive pings after 30 seconds
                KeepAlivePingTimeout = TimeSpan.FromSeconds(5),             // Wait 5 seconds for ping response before considering connection dead
                ConnectTimeout = TimeSpan.FromSeconds(5)                    // Connection timeout
            };

            // Create a GrpcChannelOptions with the custom HttpHandler
            var channelOptions = new GrpcChannelOptions
            {
                HttpHandler = _httpHandler,
                MaxReceiveMessageSize = 16 * 1024 * 1024,  // 16MB max message size (increase based on expected payload)
                MaxSendMessageSize = 4 * 1024 * 1024,      // 4MB max send size
                MaxRetryAttempts = 3,                      // Retry failed requests up to 3 times
                MaxRetryBufferSize = 1 * 1024 * 1024,      // 1MB retry buffer
                MaxRetryBufferPerCallSize = 64 * 1024      // 64KB per-call retry buffer
            };

            // Create the optimized channel
            _channel = GrpcChannel.ForAddress(_serverEndpoint, channelOptions);
            
            // Create an enhanced resilience policy with more retries for this optimized manager
            _defaultResiliencePolicy = new Lazy<IGrpcResiliencePolicy>(() => 
                new PollyGrpcResiliencePolicy(new ResilienceOptions 
                {
                    MaxRetryAttempts = 5, // More retries than default
                    RetryBackoffBaseMs = 150, // Slightly longer base retry delay
                    ExceptionsAllowedBeforeBreaking = 8, // More tolerant circuit breaker
                    DisableCircuitBreaker = true, // Disable circuit breaker to handle server with 5% error rate
                }));
        }

        /// <summary>
        /// Gets the gRPC channel to use for communication with the server
        /// </summary>
        public GrpcChannel Channel => _channel;

        /// <summary>
        /// Creates a client of the specified type using the connection
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <returns>A new instance of the client</returns>
        public T CreateClient<T>() where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OptimizedGrpcConnectionManager));

            // Create client using reflection since we don't know the exact type at compile time
            var clientType = typeof(T);
            var constructor = clientType.GetConstructor(new[] { typeof(GrpcChannel) });

            if (constructor == null)
                throw new ArgumentException($"Type {clientType.Name} does not have a constructor that takes a GrpcChannel parameter");

            return (T)constructor.Invoke(new object[] { _channel });
        }
        
        /// <summary>
        /// Creates a resilient client of the specified type using the connection and resilience policy
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <param name="resiliencePolicy">The resilience policy to use (null to use default)</param>
        /// <returns>A new resilient client wrapper</returns>
        public ResilientGrpcClient<T> CreateResilientClient<T>(IGrpcResiliencePolicy? resiliencePolicy = null) where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OptimizedGrpcConnectionManager));
                
            // Create the underlying client
            var client = CreateClient<T>();
            
            // Create a resilient wrapper with the specified or default policy
            return new ResilientGrpcClient<T>(
                client, 
                resiliencePolicy ?? _defaultResiliencePolicy.Value);
        }

        /// <summary>
        /// Releases all resources used by the connection manager
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _channel.Dispose();
                _httpHandler.Dispose();
                _disposed = true;
            }
            
            GC.SuppressFinalize(this);
        }
    }
} 