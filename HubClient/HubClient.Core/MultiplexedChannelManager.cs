using Grpc.Core;
using Grpc.Net.Client;
using HubClient.Core.Resilience;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Core
{
    /// <summary>
    /// Implements connection multiplexing with shared channels (Approach C in the implementation steps)
    /// </summary>
    public class MultiplexedChannelManager : IGrpcConnectionManager, IDisposable
    {
        private readonly string _serverEndpoint;
        private readonly int _channelCount;
        private readonly GrpcChannel[] _channels;
        private readonly SemaphoreSlim[] _channelSemaphores;
        private readonly Random _random;
        private readonly MultiplexingMetrics _metrics;
        private readonly int _maxConcurrentCallsPerChannel;
        private readonly Lazy<IGrpcResiliencePolicy> _defaultResiliencePolicy;
        private long _totalRequests;
        private bool _disposed = false;

        /// <summary>
        /// Creates a new instance of MultiplexedChannelManager with the specified endpoint and channel configuration
        /// </summary>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        /// <param name="channelCount">The number of shared channels to create (default: 4)</param>
        /// <param name="maxConcurrentCallsPerChannel">Maximum number of concurrent calls per channel (default: 100)</param>
        public MultiplexedChannelManager(string serverEndpoint, int channelCount = 4, int maxConcurrentCallsPerChannel = 100)
        {
            _serverEndpoint = serverEndpoint ?? throw new ArgumentNullException(nameof(serverEndpoint));
            _channelCount = channelCount > 0 ? channelCount : throw new ArgumentException("Channel count must be greater than zero", nameof(channelCount));
            _maxConcurrentCallsPerChannel = maxConcurrentCallsPerChannel > 0 ? maxConcurrentCallsPerChannel : throw new ArgumentException("Max concurrent calls must be greater than zero", nameof(maxConcurrentCallsPerChannel));
            
            _channels = new GrpcChannel[channelCount];
            _channelSemaphores = new SemaphoreSlim[channelCount];
            _random = new Random();
            _metrics = new MultiplexingMetrics(channelCount);
            
            // Initialize all channels and their semaphores
            for (int i = 0; i < channelCount; i++)
            {
                _channelSemaphores[i] = new SemaphoreSlim(maxConcurrentCallsPerChannel, maxConcurrentCallsPerChannel);
                _channels[i] = CreateChannel();
            }
            
            // Create specialized resilience policy tuned for multiplexing
            _defaultResiliencePolicy = new Lazy<IGrpcResiliencePolicy>(() => 
                new PollyGrpcResiliencePolicy(new ResilienceOptions 
                {
                    // More gradual retry approach for multiplexed connections
                    MaxRetryAttempts = 4,
                    RetryBackoffBaseMs = 75, 
                    RetryBackoffFactor = 2.0,
                    // Disable circuit breaker to handle server with 5% error rate
                    DisableCircuitBreaker = true,
                    // These settings will be ignored but kept for future reference
                    ExceptionsAllowedBeforeBreaking = 10,
                    CircuitBreakerDuration = TimeSpan.FromSeconds(15)
                }));
        }

        /// <summary>
        /// Gets the primary gRPC channel for use with the IGrpcConnectionManager interface
        /// </summary>
        public GrpcChannel Channel => _channels[0];

        /// <summary>
        /// Gets the metrics for this connection manager
        /// </summary>
        public MultiplexingMetrics Metrics => _metrics;

        /// <summary>
        /// Creates a client of the specified type using one of the shared channels
        /// </summary>
        /// <typeparam name="T">The client type to create</typeparam>
        /// <returns>A new instance of the client</returns>
        public T CreateClient<T>() where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));

            // Get a client using the main channel for the standard interface implementation
            var clientType = typeof(T);
            var constructor = clientType.GetConstructor(new[] { typeof(GrpcChannel) });

            if (constructor == null)
                throw new ArgumentException($"Type {clientType.Name} does not have a constructor that takes a GrpcChannel parameter");

            return (T)constructor.Invoke(new object[] { Channel });
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
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));
                
            // Create the underlying client
            var client = CreateClient<T>();
            
            // Create a resilient wrapper with the specified or default policy
            return new ResilientGrpcClient<T>(
                client, 
                resiliencePolicy ?? _defaultResiliencePolicy.Value);
        }

        /// <summary>
        /// Creates a specialized multiplexing client that uses all available channels
        /// </summary>
        /// <typeparam name="T">The client type</typeparam>
        /// <param name="clientFactory">Factory function to create the client from a channel</param>
        /// <returns>A multiplexing client wrapper</returns>
        public MultiplexingClient<T> CreateMultiplexingClient<T>(Func<GrpcChannel, T> clientFactory) where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));
                
            if (clientFactory == null)
                throw new ArgumentNullException(nameof(clientFactory));

            // Create clients for each channel
            var clients = new T[_channelCount];
            for (int i = 0; i < _channelCount; i++)
            {
                clients[i] = clientFactory(_channels[i]);
            }

            return new MultiplexingClient<T>(this, clients);
        }
        
        /// <summary>
        /// Creates a resilient multiplexing client that uses all available channels with built-in resilience
        /// </summary>
        /// <typeparam name="T">The client type</typeparam>
        /// <param name="clientFactory">Factory function to create the client from a channel</param>
        /// <param name="resiliencePolicy">The resilience policy to use (null to use default)</param>
        /// <returns>A resilient multiplexing client wrapper</returns>
        public ResilientMultiplexingClient<T> CreateResilientMultiplexingClient<T>(
            Func<GrpcChannel, T> clientFactory,
            IGrpcResiliencePolicy? resiliencePolicy = null) where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));
                
            if (clientFactory == null)
                throw new ArgumentNullException(nameof(clientFactory));
                
            // Create the base multiplexing client
            var multiplexingClient = CreateMultiplexingClient(clientFactory);
            
            // Wrap it with resilience
            return new ResilientMultiplexingClient<T>(
                multiplexingClient,
                resiliencePolicy ?? _defaultResiliencePolicy.Value);
        }

        /// <summary>
        /// Gets a channel with a semaphore permit for executing an operation
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A tuple with the channel index, channel and a disposable permit</returns>
        internal async Task<(int ChannelIndex, GrpcChannel Channel, IDisposable Permit)> GetChannelWithPermitAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MultiplexedChannelManager));

            // Use load balancing to select a channel
            var channelIndex = SelectChannelIndexAsync();
            var channel = _channels[channelIndex];
            var semaphore = _channelSemaphores[channelIndex];

            // Get a permit from the semaphore
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            var permit = new SemaphorePermit(semaphore);

            Interlocked.Increment(ref _totalRequests);
            _metrics.RecordChannelSelection(channelIndex);

            return (channelIndex, channel, permit);
        }

        /// <summary>
        /// Select a channel index based on load and availability
        /// </summary>
        private int SelectChannelIndexAsync()
        {
            // Implement a more sophisticated channel selection strategy if needed
            // For now, simply find the channel with the lowest number of active operations
            int selectedIndex = 0;
            int minActiveOperations = _channelSemaphores[0].CurrentCount;

            for (int i = 1; i < _channels.Length; i++)
            {
                int currentCount = _channelSemaphores[i].CurrentCount;
                if (currentCount > minActiveOperations)
                {
                    minActiveOperations = currentCount;
                    selectedIndex = i;
                }
            }

            return selectedIndex;
        }

        /// <summary>
        /// Creates a gRPC channel with optimized settings for multiplexing
        /// </summary>
        private GrpcChannel CreateChannel()
        {
            var channelOptions = new GrpcChannelOptions
            {
                HttpHandler = new SocketsHttpHandler
                {
                    // Set common HTTP/2 settings
                    EnableMultipleHttp2Connections = true,  // Keep only one instance of this
                    KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                    KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
                    PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                    
                    // Set connection pooling settings
                    MaxConnectionsPerServer = 100
                },
                MaxReceiveMessageSize = null,  // No message size limit
            };

            return GrpcChannel.ForAddress(_serverEndpoint, channelOptions);
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
                    // Dispose all channels
                    foreach (var channel in _channels)
                    {
                        try { channel?.Dispose(); } catch { /* Ignore disposal errors */ }
                    }

                    // Dispose all semaphores
                    foreach (var semaphore in _channelSemaphores)
                    {
                        try { semaphore?.Dispose(); } catch { /* Ignore disposal errors */ }
                    }
                }

                _disposed = true;
            }
        }

        /// <summary>
        /// Helper class to dispose semaphore permit when done with a call
        /// </summary>
        private class SemaphorePermit : IDisposable
        {
            private readonly SemaphoreSlim _semaphore;
            private bool _disposed;

            public SemaphorePermit(SemaphoreSlim semaphore)
            {
                _semaphore = semaphore;
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _semaphore.Release();
                    _disposed = true;
                }
            }
        }
    }

    /// <summary>
    /// A client that distributes calls across multiple channels for improved throughput
    /// </summary>
    /// <typeparam name="T">The gRPC client type</typeparam>
    public class MultiplexingClient<T> where T : class
    {
        private readonly MultiplexedChannelManager _manager;
        private readonly T[] _clients;
        private readonly ConcurrentDictionary<int, int> _activeCallsPerChannel = new ConcurrentDictionary<int, int>();

        /// <summary>
        /// Creates a new instance of MultiplexingClient
        /// </summary>
        /// <param name="manager">The channel manager</param>
        /// <param name="clients">The array of client instances, one per channel</param>
        public MultiplexingClient(MultiplexedChannelManager manager, T[] clients)
        {
            _manager = manager ?? throw new ArgumentNullException(nameof(manager));
            _clients = clients ?? throw new ArgumentNullException(nameof(clients));
            
            if (clients.Length == 0)
                throw new ArgumentException("At least one client is required", nameof(clients));
        }

        /// <summary>
        /// Executes a call using the multiplexing strategy
        /// </summary>
        /// <typeparam name="TResponse">The response type</typeparam>
        /// <param name="callFunc">The function to call on the client</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The response from the call</returns>
        public async Task<TResponse> CallAsync<TResponse>(
            Func<T, Task<TResponse>> callFunc,
            CancellationToken cancellationToken = default)
        {
            if (callFunc == null)
                throw new ArgumentNullException(nameof(callFunc));

            var callStartTime = Stopwatch.StartNew();
            
            var (channelIndex, _, permit) = await _manager.GetChannelWithPermitAsync(cancellationToken).ConfigureAwait(false);
            
            // Increment active calls count for the selected channel
            _activeCallsPerChannel.AddOrUpdate(channelIndex, 1, (_, count) => count + 1);
            
            try
            {
                // Get the client for the selected channel
                var client = _clients[channelIndex];
                
                // Execute the call and measure time to first byte
                var callStopwatch = Stopwatch.StartNew();
                var response = await callFunc(client).ConfigureAwait(false);
                callStopwatch.Stop();
                
                // Record metrics
                _manager.Metrics.RecordCallDuration(channelIndex, callStopwatch.Elapsed);
                return response;
            }
            finally
            {
                // Decrement active calls count
                _activeCallsPerChannel.AddOrUpdate(channelIndex, 0, (_, count) => count - 1);
                
                // Release the permit
                permit.Dispose();
                
                callStartTime.Stop();
                _manager.Metrics.RecordTotalCallTime(callStartTime.Elapsed);
            }
        }
    }
    
    /// <summary>
    /// A resilient client that distributes calls across multiple channels with built-in resilience
    /// </summary>
    /// <typeparam name="T">The gRPC client type</typeparam>
    public class ResilientMultiplexingClient<T> where T : class
    {
        private readonly MultiplexingClient<T> _multiplexingClient;
        private readonly IGrpcResiliencePolicy _resiliencePolicy;
        
        /// <summary>
        /// Creates a new instance of ResilientMultiplexingClient
        /// </summary>
        /// <param name="multiplexingClient">The underlying multiplexing client</param>
        /// <param name="resiliencePolicy">The resilience policy to use</param>
        public ResilientMultiplexingClient(MultiplexingClient<T> multiplexingClient, IGrpcResiliencePolicy resiliencePolicy)
        {
            _multiplexingClient = multiplexingClient ?? throw new ArgumentNullException(nameof(multiplexingClient));
            _resiliencePolicy = resiliencePolicy ?? throw new ArgumentNullException(nameof(resiliencePolicy));
        }
        
        /// <summary>
        /// Gets the resilience policy
        /// </summary>
        public IGrpcResiliencePolicy ResiliencePolicy => _resiliencePolicy;
        
        /// <summary>
        /// Executes a call with resilience using the multiplexing strategy
        /// </summary>
        /// <typeparam name="TResponse">The response type</typeparam>
        /// <param name="callFunc">The function to call on the client</param>
        /// <param name="methodName">The name of the method being called (for telemetry)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The response from the call</returns>
        public Task<TResponse> CallAsync<TResponse>(
            Func<T, Task<TResponse>> callFunc,
            string? methodName = null,
            CancellationToken cancellationToken = default)
        {
            if (callFunc == null)
                throw new ArgumentNullException(nameof(callFunc));
                
            return _resiliencePolicy.ExecuteAsync(
                async (ct) => await _multiplexingClient.CallAsync(callFunc, ct),
                methodName,
                cancellationToken);
        }
    }

    /// <summary>
    /// Collects metrics for the multiplexed channel manager
    /// </summary>
    public class MultiplexingMetrics
    {
        private readonly int _channelCount;
        private readonly long[] _channelSelectionCounts;
        private readonly long[] _callDurationSumTicks;
        private readonly long[] _callCounts;
        private long _totalCallTimeTicks;
        private long _totalCalls;
        private readonly object _lock = new object();

        /// <summary>
        /// Creates a new instance of MultiplexingMetrics
        /// </summary>
        /// <param name="channelCount">The number of channels being monitored</param>
        public MultiplexingMetrics(int channelCount)
        {
            _channelCount = channelCount;
            _channelSelectionCounts = new long[channelCount];
            _callDurationSumTicks = new long[channelCount];
            _callCounts = new long[channelCount];
        }

        /// <summary>
        /// Records that a channel was selected for a call
        /// </summary>
        /// <param name="channelIndex">The index of the selected channel</param>
        public void RecordChannelSelection(int channelIndex)
        {
            if (channelIndex >= 0 && channelIndex < _channelCount)
            {
                Interlocked.Increment(ref _channelSelectionCounts[channelIndex]);
            }
        }

        /// <summary>
        /// Records the duration of a call on a specific channel
        /// </summary>
        /// <param name="channelIndex">The channel index</param>
        /// <param name="duration">The call duration</param>
        public void RecordCallDuration(int channelIndex, TimeSpan duration)
        {
            if (channelIndex >= 0 && channelIndex < _channelCount)
            {
                Interlocked.Add(ref _callDurationSumTicks[channelIndex], duration.Ticks);
                Interlocked.Increment(ref _callCounts[channelIndex]);
            }
        }

        /// <summary>
        /// Records the total time from call initiation to completion
        /// </summary>
        /// <param name="totalTime">The total call time including scheduling</param>
        public void RecordTotalCallTime(TimeSpan totalTime)
        {
            Interlocked.Add(ref _totalCallTimeTicks, totalTime.Ticks);
            Interlocked.Increment(ref _totalCalls);
        }

        /// <summary>
        /// Gets the distribution of calls across channels as percentages
        /// </summary>
        public double[] ChannelDistributionPercentages
        {
            get
            {
                var totalSelections = _channelSelectionCounts.Sum();
                if (totalSelections == 0)
                    return Enumerable.Repeat(0.0, _channelCount).ToArray();

                return _channelSelectionCounts
                    .Select(count => (double)count / totalSelections * 100)
                    .ToArray();
            }
        }

        /// <summary>
        /// Gets the average call duration per channel
        /// </summary>
        public TimeSpan[] AverageCallDurationPerChannel
        {
            get
            {
                var result = new TimeSpan[_channelCount];
                for (int i = 0; i < _channelCount; i++)
                {
                    var count = _callCounts[i];
                    if (count > 0)
                        result[i] = TimeSpan.FromTicks(_callDurationSumTicks[i] / count);
                    else
                        result[i] = TimeSpan.Zero;
                }
                return result;
            }
        }

        /// <summary>
        /// Gets the overall average call duration including scheduling overhead
        /// </summary>
        public TimeSpan AverageTotalCallTime
        {
            get
            {
                var calls = _totalCalls;
                if (calls > 0)
                    return TimeSpan.FromTicks(_totalCallTimeTicks / calls);
                return TimeSpan.Zero;
            }
        }

        /// <summary>
        /// Gets the raw channel selection counts
        /// </summary>
        public long[] ChannelSelectionCounts => (long[])_channelSelectionCounts.Clone();

        /// <summary>
        /// Gets the total number of calls made
        /// </summary>
        public long TotalCalls => _totalCalls;
    }
} 