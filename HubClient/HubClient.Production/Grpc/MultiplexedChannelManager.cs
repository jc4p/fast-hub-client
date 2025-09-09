using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using HubClient.Core;
using HubClient.Core.Grpc;
using HubClient.Core.Resilience;
using HubClient.Production.Resilience;

namespace HubClient.Production.Grpc
{
    /// <summary>
    /// Manages multiple gRPC channels for efficient load balancing and resilience.
    /// Based on benchmark findings, maintaining multiple channels provides better throughput
    /// and resilience in high-concurrency scenarios.
    /// </summary>
    public class MultiplexedChannelManager : IGrpcConnectionManager, IDisposable
    {
        private readonly List<(GrpcChannel Channel, SemaphoreSlim Limiter)> _channels;
        private readonly string _serverEndpoint;
        private readonly string? _apiKey;
        private int _nextChannelIndex;
        private bool _disposed;
        private readonly Lazy<IGrpcResiliencePolicy> _defaultResiliencePolicy;

        /// <summary>
        /// Gets the primary gRPC channel to use for communication with the server
        /// </summary>
        public GrpcChannel Channel => _channels.Count > 0 ? _channels[0].Channel : throw new InvalidOperationException("No channels available");

        /// <summary>
        /// Creates a new multiplexed channel manager with the specified number of channels
        /// </summary>
        /// <param name="serverEndpoint">The server endpoint</param>
        /// <param name="channelCount">Number of channels to create (default is 8, optimal based on benchmarks)</param>
        /// <param name="maxConcurrentCallsPerChannel">Maximum number of concurrent calls per channel (default is 1000)</param>
        public MultiplexedChannelManager(string serverEndpoint, int channelCount = 8, int maxConcurrentCallsPerChannel = 1000, string? apiKey = null)
        {
            if (string.IsNullOrEmpty(serverEndpoint))
                throw new ArgumentNullException(nameof(serverEndpoint));

            if (channelCount <= 0)
                throw new ArgumentException("Channel count must be greater than zero", nameof(channelCount));
            
            if (maxConcurrentCallsPerChannel <= 0)
                throw new ArgumentException("Max concurrent calls must be greater than zero", nameof(maxConcurrentCallsPerChannel));

            _serverEndpoint = serverEndpoint;
            _apiKey = apiKey;
            _channels = new List<(GrpcChannel, SemaphoreSlim)>(channelCount);
            
            // Create the specified number of channels
            for (int i = 0; i < channelCount; i++)
            {
                var channel = CreateChannel(serverEndpoint, apiKey);
                var limiter = new SemaphoreSlim(maxConcurrentCallsPerChannel, maxConcurrentCallsPerChannel);
                _channels.Add((channel, limiter));
            }
            
            // Create default resilience policy
            _defaultResiliencePolicy = new Lazy<IGrpcResiliencePolicy>(() => new OptimizedResiliencePolicy());
        }

        /// <summary>
        /// Creates a client of the specified type using the connection
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <returns>A new instance of the client</returns>
        public T CreateClient<T>() where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));

            // Create client using reflection since we don't know the exact type at compile time
            var clientType = typeof(T);
            var constructor = clientType.GetConstructor(new[] { typeof(GrpcChannel) });

            if (constructor == null)
                throw new ArgumentException($"Type {clientType.Name} does not have a constructor that takes a GrpcChannel parameter");

            return (T)constructor.Invoke(new object[] { GetNextChannel() });
        }
        
        /// <summary>
        /// Creates a resilient client of the specified type using the connection and resilience policy
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <param name="resiliencePolicy">The resilience policy to use (null to use default)</param>
        /// <returns>A new resilient client wrapper</returns>
        public Core.Resilience.ResilientGrpcClient<T> CreateResilientClient<T>(IGrpcResiliencePolicy? resiliencePolicy = null) where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));
                
            // Create the underlying client
            var client = CreateClient<T>();
            
            // Create a resilient wrapper with the specified or default policy
            return new Core.Resilience.ResilientGrpcClient<T>(
                client, 
                resiliencePolicy ?? _defaultResiliencePolicy.Value);
        }

        /// <summary>
        /// Creates a production-specific resilient gRPC client with automatic retry and circuit breaker policies
        /// </summary>
        /// <typeparam name="TClient">Type of gRPC client to create</typeparam>
        /// <returns>A production-specific resilient gRPC client</returns>
        public Resilience.ResilientGrpcClient<TClient> CreateProductionResilientClient<TClient>() where TClient : ClientBase<TClient>
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));

            // Create client using constructor that takes a channel
            var channel = GetNextChannel();
            var clientType = typeof(TClient);
            var constructor = clientType.GetConstructor(new[] { typeof(GrpcChannel) });

            if (constructor == null)
                throw new ArgumentException($"Type {clientType.Name} does not have a constructor that takes a GrpcChannel parameter");

            var client = (TClient)constructor.Invoke(new object[] { channel });
            
            return new Resilience.ResilientGrpcClient<TClient>(
                client,
                _defaultResiliencePolicy.Value);
        }

        /// <summary>
        /// Gets the server endpoint this manager is connected to
        /// </summary>
        public string ServerEndpoint => _serverEndpoint;

        /// <summary>
        /// Gets the number of managed channels
        /// </summary>
        public int ChannelCount => _channels.Count;

        /// <summary>
        /// Initializes the channel manager asynchronously
        /// </summary>
        /// <returns>A task that completes when initialization is done</returns>
        public async Task InitializeAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));
                
            // This is a basic implementation - in a real-world scenario,
            // we might want to perform channel health checks or other async initialization
            await Task.CompletedTask;
        }

        /// <summary>
        /// Disposes all managed channels and resources
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            foreach (var (channel, limiter) in _channels)
            {
                try
                {
                    channel.Dispose();
                    limiter.Dispose();
                }
                catch
                {
                    // Suppress exceptions during disposal
                }
            }

            _channels.Clear();
            _disposed = true;
        }

        /// <summary>
        /// Gets the next channel in a round-robin fashion
        /// </summary>
        private GrpcChannel GetNextChannel()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));

            // Select a channel using an atomic increment for round-robin distribution
            var index = Interlocked.Increment(ref _nextChannelIndex) % _channels.Count;
            return _channels[index].Channel;
        }

        /// <summary>
        /// Creates a gRPC channel with optimized settings for performance
        /// </summary>
        private static GrpcChannel CreateChannel(string endpoint, string? apiKey)
        {
            var httpClientHandler = new HttpClientHandler
            {
                MaxConnectionsPerServer = 100
            };

            var channelOptions = new GrpcChannelOptions
            {
                HttpHandler = httpClientHandler,
                MaxReceiveMessageSize = 128 * 1024 * 1024, // 128 MB
                MaxSendMessageSize = 128 * 1024 * 1024,    // 128 MB
                MaxRetryAttempts = 5,
                MaxRetryBufferSize = 128 * 1024 * 1024,    // 128 MB
                MaxRetryBufferPerCallSize = 16 * 1024 * 1024 // 16 MB
            };

            if (!string.IsNullOrEmpty(apiKey))
            {
                var credentials = CallCredentials.FromInterceptor((context, metadata) =>
                {
                    metadata.Add("x-api-key", apiKey);
                    return Task.CompletedTask;
                });

                var baseCredentials = endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase)
                    ? ChannelCredentials.SecureSsl
                    : ChannelCredentials.Insecure;

                channelOptions.Credentials = ChannelCredentials.Create(baseCredentials, credentials);
            }
            else if (endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                channelOptions.Credentials = ChannelCredentials.SecureSsl;
            }

            return GrpcChannel.ForAddress(endpoint, channelOptions);
        }
    }
}
