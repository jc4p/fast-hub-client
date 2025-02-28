using Grpc.Core;
using Grpc.Net.Client;
using HubClient.Core.Resilience;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Core
{
    /// <summary>
    /// Direct gRPC channel management with manual connection control (Approach B in the implementation steps)
    /// </summary>
    public class DirectChannelManager : IGrpcConnectionManager, IDisposable
    {
        private readonly string _serverEndpoint;
        private readonly int _maxConcurrentChannels;
        private readonly SemaphoreSlim _channelSemaphore;
        private readonly ConcurrentDictionary<int, GrpcChannel> _activeChannels;
        private readonly ConnectionTracker _connectionTracker;
        private readonly Lazy<IGrpcResiliencePolicy> _defaultResiliencePolicy;
        private GrpcChannel? _mainChannel;
        private int _nextChannelId = 0;
        private bool _disposed = false;

        /// <summary>
        /// Creates a new instance of the DirectChannelManager
        /// </summary>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        /// <param name="maxConcurrentChannels">Maximum number of concurrent channels to maintain</param>
        public DirectChannelManager(string serverEndpoint, int maxConcurrentChannels = 20)
        {
            _serverEndpoint = serverEndpoint ?? throw new ArgumentNullException(nameof(serverEndpoint));
            _maxConcurrentChannels = maxConcurrentChannels > 0 ? maxConcurrentChannels : throw new ArgumentException("Max concurrent channels must be positive", nameof(maxConcurrentChannels));
            _channelSemaphore = new SemaphoreSlim(maxConcurrentChannels, maxConcurrentChannels);
            _activeChannels = new ConcurrentDictionary<int, GrpcChannel>();
            _connectionTracker = new ConnectionTracker();
            
            // Create the main channel for the IGrpcConnectionManager.Channel property
            _mainChannel = CreateNewChannel();
            
            // Create a specialized resilience policy that's optimized for direct channel management
            _defaultResiliencePolicy = new Lazy<IGrpcResiliencePolicy>(() => 
                new PollyGrpcResiliencePolicy(new ResilienceOptions 
                {
                    MaxRetryAttempts = 4, // More retries
                    RetryBackoffBaseMs = 125, // Different backoff strategy
                    RetryBackoffFactor = 1.75, // Less aggressive backoff growth
                    ExceptionsAllowedBeforeBreaking = 7, // Different circuit breaker threshold
                    DisableCircuitBreaker = true, // Disable circuit breaker to handle server with 5% error rate
                }));
        }

        /// <summary>
        /// Gets the primary gRPC channel to use for communication with the server
        /// </summary>
        public GrpcChannel Channel => _mainChannel ?? throw new InvalidOperationException("Channel is not initialized");

        /// <summary>
        /// Gets connection statistics and metrics
        /// </summary>
        public ConnectionTracker ConnectionStats => _connectionTracker;

        /// <summary>
        /// Creates a client of the specified type using the connection
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <returns>A new instance of the client</returns>
        public T CreateClient<T>() where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DirectChannelManager));

            // Create client using reflection since we don't know the exact type at compile time
            var clientType = typeof(T);
            var constructor = clientType.GetConstructor(new[] { typeof(GrpcChannel) });

            if (constructor == null)
                throw new ArgumentException($"Type {clientType.Name} does not have a constructor that takes a GrpcChannel parameter");
            
            if (_mainChannel == null)
                throw new InvalidOperationException("Channel is not initialized");

            return (T)constructor.Invoke(new object[] { _mainChannel });
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
                throw new ObjectDisposedException(nameof(DirectChannelManager));
                
            // Create the underlying client
            var client = CreateClient<T>();
            
            // Create a resilient wrapper with the specified or default policy
            return new ResilientGrpcClient<T>(
                client, 
                resiliencePolicy ?? _defaultResiliencePolicy.Value);
        }
        
        /// <summary>
        /// Creates a client with resilience that uses a dedicated channel
        /// </summary>
        /// <typeparam name="T">The client type</typeparam>
        /// <param name="clientFactory">Factory function to create the client</param>
        /// <param name="resiliencePolicy">Optional resilience policy</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A resilient client, its channel, and a task to release the channel</returns>
        public async Task<(ResilientGrpcClient<T> Client, GrpcChannel Channel, Func<Task> ReleaseChannel)> 
            CreateResilientClientWithDedicatedChannelAsync<T>(
                Func<GrpcChannel, T> clientFactory,
                IGrpcResiliencePolicy? resiliencePolicy = null,
                CancellationToken cancellationToken = default) where T : class
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DirectChannelManager));
            if (clientFactory == null) throw new ArgumentNullException(nameof(clientFactory));
            
            var channel = await GetChannelAsync(cancellationToken).ConfigureAwait(false);
            var client = clientFactory(channel);
            var resilientClient = new ResilientGrpcClient<T>(
                client, 
                resiliencePolicy ?? _defaultResiliencePolicy.Value);
                
            Func<Task> releaseChannel = async () => 
            {
                await ReleaseChannelAsync(channel);
            };
            
            return (resilientClient, channel, releaseChannel);
        }

        /// <summary>
        /// Gets a channel from the pool or creates a new one
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A gRPC channel</returns>
        public async Task<GrpcChannel> GetChannelAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DirectChannelManager));

            // Wait for a channel slot to become available
            await _channelSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                // Create a new channel with a unique ID
                int channelId = Interlocked.Increment(ref _nextChannelId);
                
                var sw = Stopwatch.StartNew();
                var channel = CreateNewChannel();
                sw.Stop();
                
                _connectionTracker.RecordConnectionEstablishment(sw.Elapsed);
                _activeChannels[channelId] = channel;
                
                return channel;
            }
            catch
            {
                // Release the semaphore if channel creation fails
                _channelSemaphore.Release();
                throw;
            }
        }

        /// <summary>
        /// Releases a channel back to the pool
        /// </summary>
        /// <param name="channel">The channel to release</param>
        public ValueTask ReleaseChannelAsync(GrpcChannel channel)
        {
            if (_disposed) return ValueTask.CompletedTask;
            if (channel == null) throw new ArgumentNullException(nameof(channel));

            // Find and remove the channel
            var keyToRemove = -1;
            foreach (var kvp in _activeChannels)
            {
                if (ReferenceEquals(kvp.Value, channel))
                {
                    keyToRemove = kvp.Key;
                    break;
                }
            }

            if (keyToRemove != -1 && _activeChannels.TryRemove(keyToRemove, out _))
            {
                // Release the semaphore slot
                _channelSemaphore.Release();
            }

            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Creates a new channel with direct configuration
        /// </summary>
        private GrpcChannel CreateNewChannel()
        {
            return GrpcChannel.ForAddress(_serverEndpoint, new GrpcChannelOptions
            {
                // Direct channel with minimal options, relying on explicit management
                MaxReceiveMessageSize = null, // No size limit
                MaxSendMessageSize = null,    // No size limit
                ThrowOperationCanceledOnCancellation = true
            });
        }

        /// <summary>
        /// Creates a client with a dedicated channel
        /// </summary>
        /// <typeparam name="T">The client type</typeparam>
        /// <param name="clientFactory">Factory function to create the client</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The client and dedicated channel</returns>
        public async Task<(T Client, GrpcChannel Channel)> CreateClientWithDedicatedChannelAsync<T>(
            Func<GrpcChannel, T> clientFactory, 
            CancellationToken cancellationToken = default) where T : class
        {
            if (clientFactory == null) throw new ArgumentNullException(nameof(clientFactory));
            
            var channel = await GetChannelAsync(cancellationToken).ConfigureAwait(false);
            var client = clientFactory(channel);
            return (client, channel);
        }

        /// <summary>
        /// Releases all resources used by the connection manager
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources and optionally releases the managed resources
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Close and dispose all channels
                    foreach (var channel in _activeChannels.Values)
                    {
                        try { channel.Dispose(); } catch { /* Ignore disposal errors */ }
                    }
                    _activeChannels.Clear();
                    
                    // Dispose the main channel
                    _mainChannel?.Dispose();
                    _mainChannel = null;
                    
                    _channelSemaphore.Dispose();
                }

                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Tracks connection statistics for performance analysis
    /// </summary>
    public class ConnectionTracker
    {
        private long _establishmentTimeSum = 0;
        private long _ttfbSum = 0;
        private int _connectionCount = 0;
        private readonly object _lock = new object();

        /// <summary>
        /// Records a connection establishment duration
        /// </summary>
        /// <param name="duration">The time it took to establish the connection</param>
        public void RecordConnectionEstablishment(TimeSpan duration)
        {
            lock (_lock)
            {
                _establishmentTimeSum += duration.Ticks;
                _connectionCount++;
            }
        }

        /// <summary>
        /// Records a time-to-first-byte duration
        /// </summary>
        /// <param name="duration">The time it took to receive the first byte</param>
        public void RecordTimeToFirstByte(TimeSpan duration)
        {
            lock (_lock)
            {
                _ttfbSum += duration.Ticks;
            }
        }

        /// <summary>
        /// Gets the average connection establishment time
        /// </summary>
        public TimeSpan AverageEstablishmentTime
        {
            get
            {
                lock (_lock)
                {
                    if (_connectionCount == 0)
                        return TimeSpan.Zero;
                    return TimeSpan.FromTicks(_establishmentTimeSum / _connectionCount);
                }
            }
        }

        /// <summary>
        /// Gets the average time to first byte
        /// </summary>
        public TimeSpan AverageTimeToFirstByte
        {
            get
            {
                lock (_lock)
                {
                    if (_connectionCount == 0)
                        return TimeSpan.Zero;
                    return TimeSpan.FromTicks(_ttfbSum / _connectionCount);
                }
            }
        }

        /// <summary>
        /// Gets the number of connections established
        /// </summary>
        public int ConnectionCount
        {
            get
            {
                lock (_lock)
                {
                    return _connectionCount;
                }
            }
        }
    }
} 