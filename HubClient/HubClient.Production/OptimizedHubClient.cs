using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using HubClient.Production.Concurrency;
using HubClient.Production.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.ObjectPool;
using Microsoft.IO;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using HubClient.Production.Grpc;
using HubClient.Core.Serialization;

namespace HubClient.Production
{
    /// <summary>
    /// High-performance implementation of a Hub client based on extensive benchmarking.
    /// This implementation uses the most efficient patterns identified during benchmarking:
    /// - MultiplexedChannelManager for connection pooling
    /// - UnsafeGrpcMessageSerializer for fast serialization
    /// - ChannelPipeline for concurrency
    /// </summary>
    public class OptimizedHubClient : IDisposable, IAsyncDisposable
    {
        private readonly ILogger<OptimizedHubClient> _logger;
        private readonly OptimizedHubClientOptions _options;
        private readonly MultiplexedChannelManager _connectionManager;
        private readonly RecyclableMemoryStreamManager _streamManager;
        private readonly PipelineCreationOptions _pipelineOptions;
        private readonly Dictionary<string, Func<IConcurrencyPipeline<byte[], byte[]>>> _pipelineFactories;
        private readonly Dictionary<string, IConcurrencyPipeline<byte[], byte[]>> _activePipelines;
        private readonly SemaphoreSlim _initializationLock = new SemaphoreSlim(1, 1);
        private readonly HubClient.Core.Serialization.MessageSerializerFactory.SerializerType _serializerType;
        private bool _initialized;
        private bool _disposed;

        /// <summary>
        /// Creates a new instance of the optimized hub client
        /// </summary>
        /// <param name="options">Client configuration options</param>
        /// <param name="logger">Optional logger</param>
        public OptimizedHubClient(OptimizedHubClientOptions options, ILogger<OptimizedHubClient> logger = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger;
            
            // Initialize stream manager for memory management
            _streamManager = new RecyclableMemoryStreamManager(
                new RecyclableMemoryStreamManager.Options
                {
                    BlockSize = _options.BlockSize,
                    LargeBufferMultiple = _options.LargeBufferMultiple,
                    MaximumBufferSize = _options.MaximumBufferSize
                });

            // Create connection manager with optimal number of channels (8) based on benchmarks
            _connectionManager = new MultiplexedChannelManager(
                _options.ServerEndpoint,
                _options.ChannelCount,
                _options.MaxConcurrentCallsPerChannel);

            // Set up pipeline options based on benchmarks
            _pipelineOptions = new PipelineCreationOptions
            {
                MaxConcurrency = _options.MaxPipelineConcurrency,
                InputQueueCapacity = _options.PipelineInputQueueCapacity,
                OutputQueueCapacity = _options.PipelineOutputQueueCapacity,
                BoundedCapacity = _options.PipelineBoundedCapacity,
                PreserveOrderInBatch = _options.PreserveMessageOrder,
                AllowSynchronousContinuations = _options.AllowSynchronousContinuations
            };
            
            // Set up pipeline factories for different operation types
            _pipelineFactories = new Dictionary<string, Func<IConcurrencyPipeline<byte[], byte[]>>>();
            _activePipelines = new Dictionary<string, IConcurrencyPipeline<byte[], byte[]>>();
            _serializerType = _options.SerializerType;
        }

        /// <summary>
        /// Initializes the client and establishes the connection to the server
        /// </summary>
        public async Task InitializeAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OptimizedHubClient));

            await _initializationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            
            try
            {
                if (_initialized)
                    return;

                _logger?.LogInformation("Initializing OptimizedHubClient with server endpoint: {Endpoint}", _options.ServerEndpoint);

                // Verify connection to the server
                await VerifyConnectionAsync(cancellationToken).ConfigureAwait(false);

                // Set up pipeline factories
                InitializePipelineFactories();

                _initialized = true;
                _logger?.LogInformation("OptimizedHubClient initialized successfully");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to initialize OptimizedHubClient");
                throw;
            }
            finally
            {
                _initializationLock.Release();
            }
        }

        /// <summary>
        /// Sends a request to the server and gets a response
        /// </summary>
        /// <typeparam name="TRequest">Request message type</typeparam>
        /// <typeparam name="TResponse">Response message type</typeparam>
        /// <param name="request">The request message</param>
        /// <param name="operationKey">A unique key identifying this operation type (for policy isolation)</param>
        /// <param name="callFunc">Function that executes the gRPC call</param>
        /// <param name="timeout">Optional timeout</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The response message</returns>
        public async Task<TResponse> SendRequestAsync<TRequest, TResponse>(
            TRequest request,
            string operationKey,
            Func<TRequest, CancellationToken, Task<TResponse>> callFunc,
            TimeSpan? timeout = null,
            CancellationToken cancellationToken = default)
            where TRequest : IMessage<TRequest>, new()
            where TResponse : IMessage<TResponse>, new()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OptimizedHubClient));

            if (!_initialized)
                throw new InvalidOperationException("Client is not initialized. Call InitializeAsync first.");

            if (request == null)
                throw new ArgumentNullException(nameof(request));
            
            if (string.IsNullOrEmpty(operationKey))
                throw new ArgumentNullException(nameof(operationKey));
            
            if (callFunc == null)
                throw new ArgumentNullException(nameof(callFunc));

            var stopwatch = Stopwatch.StartNew();

            try
            {
                _logger?.LogDebug("Sending request of type {RequestType} with operation key {OperationKey}", 
                    typeof(TRequest).Name, operationKey);

                // Execute the call function
                return await callFunc(request, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is RpcException or TaskCanceledException or OperationCanceledException)
            {
                _logger?.LogWarning(ex, "Error sending request of type {RequestType} with operation key {OperationKey}",
                    typeof(TRequest).Name, operationKey);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                _logger?.LogDebug("Request of type {RequestType} completed in {ElapsedMs}ms", 
                    typeof(TRequest).Name, stopwatch.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Processes a stream of messages through a pipeline
        /// </summary>
        /// <typeparam name="TRequest">Request message type</typeparam>
        /// <typeparam name="TResponse">Response message type</typeparam>
        /// <param name="request">The request message</param>
        /// <param name="operationKey">A unique key identifying this operation type (for policy isolation)</param>
        /// <param name="streamingCallFunc">Function that executes the gRPC streaming call</param>
        /// <param name="messageProcessor">Function that processes each message</param>
        /// <param name="timeout">Optional timeout</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The count of processed messages</returns>
        public async Task<int> ProcessStreamAsync<TRequest, TResponse>(
            TRequest request,
            string operationKey,
            Func<TRequest, CancellationToken, AsyncServerStreamingCall<TResponse>> streamingCallFunc,
            Func<TResponse, ValueTask> messageProcessor,
            TimeSpan? timeout = null,
            CancellationToken cancellationToken = default)
            where TRequest : IMessage<TRequest>, new()
            where TResponse : IMessage<TResponse>, new()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OptimizedHubClient));

            if (!_initialized)
                throw new InvalidOperationException("Client is not initialized. Call InitializeAsync first.");

            if (request == null)
                throw new ArgumentNullException(nameof(request));
            
            if (string.IsNullOrEmpty(operationKey))
                throw new ArgumentNullException(nameof(operationKey));
            
            if (streamingCallFunc == null)
                throw new ArgumentNullException(nameof(streamingCallFunc));
            
            if (messageProcessor == null)
                throw new ArgumentNullException(nameof(messageProcessor));

            var stopwatch = Stopwatch.StartNew();
            int processedCount = 0;
            
            // Get or create a pipeline for this operation
            var pipeline = GetOrCreatePipeline(operationKey);
            var serializer = HubClient.Core.Serialization.MessageSerializerFactory.Create<TResponse>(_serializerType);

            // Create a pipeline consumer
            var pipelineConsumer = new Func<byte[], ValueTask>(async responseBytes =>
            {
                try
                {
                    // Deserialize the response
                    if (serializer.TryDeserialize(responseBytes, out var responseMessage))
                    {
                        // Process the message
                        await messageProcessor(responseMessage).ConfigureAwait(false);
                        Interlocked.Increment(ref processedCount);
                    }
                    else
                    {
                        _logger?.LogWarning("Failed to deserialize message in pipeline for operation {OperationKey}", operationKey);
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error processing message in pipeline for operation {OperationKey}", operationKey);
                }
            });

            try
            {
                _logger?.LogDebug("Starting stream processing for operation {OperationKey}", operationKey);

                // Make the gRPC call to get the stream
                using var call = streamingCallFunc(request, cancellationToken);

                // Start consuming from the pipeline
                var consumeTask = pipeline.ConsumeAsync(pipelineConsumer, cancellationToken);

                // Process the stream responses
                await using var enumerator = call.ResponseStream.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
                
                while (await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    var response = enumerator.Current;
                    
                    // Serialize the response to bytes
                    var responseBytes = serializer.Serialize(response);
                    
                    // Enqueue the bytes to the pipeline
                    await pipeline.EnqueueAsync(responseBytes, cancellationToken).ConfigureAwait(false);
                }

                // Complete the pipeline and wait for all messages to be processed
                await pipeline.CompleteAsync(cancellationToken).ConfigureAwait(false);
                await consumeTask.ConfigureAwait(false);

                return processedCount;
            }
            catch (Exception ex) when (ex is RpcException or TaskCanceledException or OperationCanceledException)
            {
                _logger?.LogWarning(ex, "Error in stream processing for operation {OperationKey}", operationKey);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                _logger?.LogDebug("Stream processing for operation {OperationKey} completed in {ElapsedMs}ms with {ProcessedCount} messages", 
                    operationKey, stopwatch.ElapsedMilliseconds, processedCount);
            }
        }

        /// <summary>
        /// Creates a resilient gRPC client of the specified type.
        /// </summary>
        /// <typeparam name="TClient">The gRPC client type to create.</typeparam>
        /// <returns>A resilient wrapper around the gRPC client.</returns>
        public Resilience.ResilientGrpcClient<TClient> CreateClient<TClient>() 
            where TClient : ClientBase<TClient>
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OptimizedHubClient));

            return _connectionManager.CreateProductionResilientClient<TClient>();
        }

        /// <summary>
        /// Gets metrics for a specific pipeline
        /// </summary>
        /// <param name="operationKey">The operation key for the pipeline</param>
        /// <returns>Pipeline metrics or null if the pipeline doesn't exist</returns>
        public PipelineMetrics GetPipelineMetrics(string operationKey)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OptimizedHubClient));

            if (_activePipelines.TryGetValue(operationKey, out var pipeline))
            {
                return pipeline.Metrics;
            }

            return null;
        }

        /// <summary>
        /// Processes batched messages through a pipeline with configurable concurrency
        /// </summary>
        /// <typeparam name="TRequest">The request message type</typeparam>
        /// <typeparam name="TResponse">The response message type</typeparam>
        /// <param name="messages">The batch of messages to process</param>
        /// <param name="messageProcessor">Function that processes a message and returns the response</param>
        /// <param name="operationKey">Optional key to identify the operation for metrics</param>
        /// <returns>A task representing the asynchronous operation</returns>
        public async Task ProcessMessagesAsync<TRequest, TResponse>(
            IEnumerable<TRequest> messages,
            Func<TRequest, Task<TResponse>> messageProcessor,
            string? operationKey = null)
            where TRequest : Google.Protobuf.IMessage<TRequest>, new()
            where TResponse : Google.Protobuf.IMessage<TResponse>, new()
        {
            if (messages == null)
                throw new ArgumentNullException(nameof(messages));
            if (messageProcessor == null)
                throw new ArgumentNullException(nameof(messageProcessor));

            var stopwatch = Stopwatch.StartNew();
            int processedCount = 0;
            
            // Get or create a pipeline for this operation
            var pipeline = GetOrCreatePipeline(operationKey);
            var requestSerializer = HubClient.Core.Serialization.MessageSerializerFactory.Create<TRequest>(_serializerType);
            var responseSerializer = HubClient.Core.Serialization.MessageSerializerFactory.Create<TResponse>(_serializerType);

            // Create a pipeline consumer
            var pipelineConsumer = new Func<byte[], ValueTask>(async responseBytes =>
            {
                try
                {
                    // Deserialize the response
                    if (responseSerializer.TryDeserialize(responseBytes, out var responseMessage))
                    {
                        processedCount++;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing response in pipeline");
                }
            });

            try
            {
                _logger?.LogDebug("Starting batch processing for operation {OperationKey}", operationKey);

                // Process each message in the batch
                foreach (var message in messages)
                {
                    // Serialize the message to bytes
                    var messageBytes = requestSerializer.Serialize(message);
                    
                    // Enqueue the bytes to the pipeline
                    await pipeline.EnqueueAsync(messageBytes, default).ConfigureAwait(false);
                }

                // Complete the pipeline and wait for all messages to be processed
                await pipeline.CompleteAsync(default).ConfigureAwait(false);

                // Process the responses
                var consumeTask = pipeline.ConsumeAsync(pipelineConsumer, default);
                await consumeTask.ConfigureAwait(false);

                return;
            }
            catch (Exception ex) when (ex is RpcException or TaskCanceledException or OperationCanceledException)
            {
                _logger?.LogWarning(ex, "Error in batch processing for operation {OperationKey}", operationKey);
                throw;
            }
            finally
            {
                stopwatch.Stop();
                _logger?.LogDebug("Batch processing for operation {OperationKey} completed in {ElapsedMs}ms with {ProcessedCount} messages", 
                    operationKey, stopwatch.ElapsedMilliseconds, processedCount);
            }
        }

        /// <summary>
        /// Gets user data for the specified FID
        /// </summary>
        /// <param name="request">The request containing the FID</param>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <returns>Response containing user data messages</returns>
        public async Task<MessagesResponse> GetUserDataByFidAsync(FidRequest request, CancellationToken cancellationToken = default)
        {
            EnsureNotDisposed();
            await EnsureInitializedAsync().ConfigureAwait(false);
            
            var client = new HubService.HubServiceClient(_connectionManager.Channel);
            return await client.GetUserDataByFidAsync(request, cancellationToken: cancellationToken);
        }
        
        /// <summary>
        /// Gets current storage limits and tier subscription info for the specified FID
        /// </summary>
        /// <param name="request">The request containing the FID</param>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <returns>Response containing storage limits and tier subscriptions</returns>
        public async Task<StorageLimitsResponse> GetCurrentStorageLimitsByFidAsync(FidRequest request, CancellationToken cancellationToken = default)
        {
            EnsureNotDisposed();
            await EnsureInitializedAsync().ConfigureAwait(false);
            
            var client = new HubService.HubServiceClient(_connectionManager.Channel);
            return await client.GetCurrentStorageLimitsByFidAsync(request, cancellationToken: cancellationToken);
        }
        
        /// <summary>
        /// Gets the ID registry on-chain event for the specified FID
        /// </summary>
        /// <param name="request">The request containing the FID</param>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <returns>On-chain event containing the primary Ethereum address</returns>
        public async Task<OnChainEvent> GetIdRegistryOnChainEventAsync(FidRequest request, CancellationToken cancellationToken = default)
        {
            EnsureNotDisposed();
            await EnsureInitializedAsync().ConfigureAwait(false);
            
            var client = new HubService.HubServiceClient(_connectionManager.Channel);
            return await client.GetIdRegistryOnChainEventAsync(request, cancellationToken: cancellationToken);
        }
        
        /// <summary>
        /// Gets the latest FID from the hub
        /// </summary>
        /// <param name="shardId">The shard ID to query (default 0)</param>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <returns>The latest FID or null if none found</returns>
        public async Task<ulong?> GetLatestFidAsync(uint shardId = 0, CancellationToken cancellationToken = default)
        {
            EnsureNotDisposed();
            await EnsureInitializedAsync().ConfigureAwait(false);
            
            var client = new HubService.HubServiceClient(_connectionManager.Channel);
            
            // Request FIDs in reverse order to get the latest first
            var request = new FidsRequest
            {
                ShardId = shardId,
                PageSize = 1,
                Reverse = true
            };
            
            var response = await client.GetFidsAsync(request, cancellationToken: cancellationToken);
            
            if (response.Fids.Count > 0)
            {
                return response.Fids[0];
            }
            
            return null;
        }
        
        /// <summary>
        /// Gets the latest FID across all shards
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <returns>The latest FID across all shards</returns>
        public async Task<ulong?> GetLatestFidFromAnyShard(CancellationToken cancellationToken = default)
        {
            EnsureNotDisposed();
            await EnsureInitializedAsync().ConfigureAwait(false);
            
            ulong? maxFid = null;
            
            // Try shards 0, 1, and 2 (common shard IDs)
            for (uint shardId = 0; shardId <= 2; shardId++)
            {
                try
                {
                    var latestFid = await GetLatestFidAsync(shardId, cancellationToken);
                    if (latestFid.HasValue && (!maxFid.HasValue || latestFid.Value > maxFid.Value))
                    {
                        maxFid = latestFid.Value;
                    }
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.InvalidArgument)
                {
                    // This shard doesn't exist, continue
                    continue;
                }
            }
            
            return maxFid;
        }

        #region Private Methods

        private async Task VerifyConnectionAsync(CancellationToken cancellationToken)
        {
            try
            {
                // The actual implementation would verify the server connection
                // This could be a simple ping/heartbeat mechanism

                _logger?.LogDebug("Verifying connection to server endpoint: {Endpoint}", _options.ServerEndpoint);
                
                // For now, we'll just delay to simulate a connection verification
                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to verify connection to server endpoint: {Endpoint}", _options.ServerEndpoint);
                throw new InvalidOperationException($"Failed to connect to server at {_options.ServerEndpoint}", ex);
            }
        }

        private void InitializePipelineFactories()
        {
            // Based on benchmarking, ChannelPipeline is the optimal choice
            _pipelineFactories[nameof(PipelineStrategy.Channel)] = () => 
                new ChannelPipeline<byte[], byte[]>(
                    (bytes, _) => new ValueTask<byte[]>(bytes),  // Identity transform for simplicity
                    _pipelineOptions);

            // For completeness, we also support other pipeline strategies
            _pipelineFactories[nameof(PipelineStrategy.TaskParallel)] = () => 
                new ChannelPipeline<byte[], byte[]>(
                    (bytes, _) => new ValueTask<byte[]>(bytes),  // Identity transform for simplicity
                    _pipelineOptions);

            _pipelineFactories[nameof(PipelineStrategy.Dataflow)] = () => 
                new ChannelPipeline<byte[], byte[]>(
                    (bytes, _) => new ValueTask<byte[]>(bytes),  // Identity transform for simplicity
                    _pipelineOptions);
        }

        private IConcurrencyPipeline<byte[], byte[]> GetOrCreatePipeline(string operationKey)
        {
            // Return existing pipeline if available
            if (_activePipelines.TryGetValue(operationKey, out var pipeline))
            {
                return pipeline;
            }

            // Create a new pipeline if none exists
            lock (_activePipelines)
            {
                // Double-check in case another thread created it
                if (_activePipelines.TryGetValue(operationKey, out pipeline))
                {
                    return pipeline;
                }

                // Select the pipeline factory based on the configured strategy
                var strategyName = _options.PipelineStrategy.ToString();
                if (!_pipelineFactories.TryGetValue(strategyName, out var factory))
                {
                    _logger?.LogWarning("Pipeline strategy {Strategy} not found, falling back to Channel strategy", strategyName);
                    factory = _pipelineFactories[nameof(PipelineStrategy.Channel)];
                }

                // Create and register the new pipeline
                pipeline = factory();
                _activePipelines[operationKey] = pipeline;
                return pipeline;
            }
        }

        private void EnsureNotDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(OptimizedHubClient));
        }

        private async Task EnsureInitializedAsync()
        {
            if (!_initialized)
                await InitializeAsync();
        }
        
        private async Task InitializeAsync()
        {
            await _initializationLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_initialized)
                    return;
                    
                // Initialize the connection manager
                await _connectionManager.InitializeAsync().ConfigureAwait(false);
                
                _initialized = true;
                _logger?.LogInformation("OptimizedHubClient initialized successfully");
            }
            finally
            {
                _initializationLock.Release();
            }
        }

        #endregion

        #region IDisposable Implementation

        /// <summary>
        /// Disposes the client and releases all resources
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            DisposeAsync().AsTask().GetAwaiter().GetResult();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Asynchronously disposes the client and releases all resources
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _logger?.LogDebug("Disposing OptimizedHubClient");

            try
            {
                // Dispose all active pipelines
                foreach (var pipeline in _activePipelines.Values)
                {
                    await pipeline.DisposeAsync().ConfigureAwait(false);
                }
                
                _activePipelines.Clear();
                
                // Dispose other resources
                _connectionManager.Dispose();
                _initializationLock.Dispose();
                
                _disposed = true;

                _logger?.LogDebug("OptimizedHubClient disposed successfully");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error disposing OptimizedHubClient");
            }
        }

        #endregion
    }
} 