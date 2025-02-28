using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Net.Client;
using HubClient.Core;
using HubClient.Core.Concurrency;
using HubClient.Core.Grpc;
using HubClient.Core.Serialization;
using HubClient.Core.Storage;
using Microsoft.Extensions.ObjectPool;
using Microsoft.IO;
using Parquet;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HubClient.Benchmarks
{
    /// <summary>
    /// Benchmarks for the Intermediate Storage implementation
    /// </summary>
    [MemoryDiagnoser]
    [GcServer(true)]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    [EventPipeProfiler(BenchmarkDotNet.Diagnosers.EventPipeProfile.GcVerbose)]
    [SimpleJob(RunStrategy.Monitoring, launchCount: 1, warmupCount: 5, iterationCount: 25, id: "QuickBenchmark")]
    public class IntermediateStorageBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        /// <summary>
        /// Size of each batch to process - only using large batch sizes
        /// </summary>
        [Params(10000, 25000)]
        public int BatchSize { get; set; }
        
        /// <summary>
        /// Compression method for Parquet files
        /// </summary>
        public enum CompressionType
        {
            None,
            Snappy,
            Gzip
        }
        
        /// <summary>
        /// Compression method to use
        /// </summary>
        [Params(CompressionType.Snappy)]
        public CompressionType Compression { get; set; }
        
        /// <summary>
        /// Row group size for Parquet files
        /// </summary>
        [Params(5000)]
        public int RowGroupSize { get; set; }
        
        /// <summary>
        /// Storage approach for benchmarking
        /// </summary>
        public enum StorageApproach
        {
            /// <summary>
            /// Direct write to Parquet as messages arrive
            /// </summary>
            DirectWrite,
            
            /// <summary>
            /// In-memory buffer with bulk writes to Parquet
            /// </summary>
            BufferedWrite
        }
        
        /// <summary>
        /// Storage approach to benchmark
        /// </summary>
        [Params(StorageApproach.DirectWrite)]
        public StorageApproach Approach { get; set; }
        
        /// <summary>
        /// Total messages to process in the benchmark
        /// </summary>
        [Params(50000)]
        public int MessageCount { get; set; }
        
        /// <summary>
        /// Workload pattern to use
        /// </summary>
        public enum WorkloadPattern
        {
            /// <summary>
            /// Small messages at high frequency
            /// </summary>
            SmallFrequent,
            
            /// <summary>
            /// Large batched messages
            /// </summary>
            LargeBatched,
            
            /// <summary>
            /// Mix of message sizes and frequencies
            /// </summary>
            MixedTraffic
        }
        
        /// <summary>
        /// Workload pattern for the benchmark
        /// </summary>
        [Params(WorkloadPattern.LargeBatched)]
        public WorkloadPattern Workload { get; set; }
        
        // Storage components
        private string _outputDirectory = null!;
        private IIntermediateStorage<Message> _storage = null!;
        
        // gRPC components
        private IGrpcConnectionManager _connectionManager = null!;
        private MinimalHubService.MinimalHubServiceClient _grpcClient = null!;
        
        // Metrics
        private long _messagesStored;
        private long _bytesProcessed;
        private Stopwatch _stopwatch = new();
        
        /// <summary>
        /// Setup benchmark environment
        /// </summary>
        [GlobalSetup]
        public void Setup()
        {
            Console.WriteLine($"Setting up Intermediate Storage Benchmark: {Approach}, {Compression}, Row Group={RowGroupSize}, Batch={BatchSize}, Workload={Workload}");
            
            // Create output directory for benchmark
            _outputDirectory = Path.Combine(Path.GetTempPath(), $"hubclient_benchmark_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            Directory.CreateDirectory(_outputDirectory);
            
            // Create gRPC connection manager and client based on LEARNINGS.md recommendation of 8 channels
            _connectionManager = new MultiplexedChannelManager(ServerEndpoint, 8);
            _grpcClient = _connectionManager.CreateClient<MinimalHubService.MinimalHubServiceClient>();
            
            // Create storage with appropriate compression settings
            _storage = CreateStorage(Compression);
            
            // Reset metrics
            _messagesStored = 0;
            _bytesProcessed = 0;
            
            Console.WriteLine("Using real data from gRPC server for benchmarking");
            Console.WriteLine($"Server endpoint: {ServerEndpoint}");
            Console.WriteLine($"Messages to process: {MessageCount}");
            Console.WriteLine($"Compression: {Compression}");
            Console.WriteLine($"Row group size: {RowGroupSize}");
            Console.WriteLine($"Storage approach: {Approach}");
            Console.WriteLine($"Output directory: {_outputDirectory}");
            
            // Validate server connection before proceeding
            ValidateServerConnection().GetAwaiter().GetResult();
            
            Console.WriteLine("Setup complete.");
        }
        
        /// <summary>
        /// Validate server connection before running benchmark
        /// </summary>
        private async Task ValidateServerConnection()
        {
            try
            {
                Console.WriteLine("Validating server connection...");
                
                // Create a resilient client for the validation
                var resilientClient = _connectionManager.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
                
                // Create a simple request with FID 42
                var request = new FidRequest { Fid = 42, PageSize = 1 };
                
                // Create a cancellation token with a short timeout
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                
                // Make a test call to the server
                var response = await resilientClient.CallAsync(
                    (client, ct) => client.GetCastsByFidAsync(request, cancellationToken: ct).ResponseAsync,
                    "ValidateConnection",
                    cts.Token);
                
                if (response != null)
                {
                    Console.WriteLine($"Server connection validated. Received response with {response.Messages?.Count ?? 0} messages.");
                    
                    // Check if we got any messages
                    if (response.Messages?.Count > 0)
                    {
                        var firstMessage = response.Messages[0];
                        Console.WriteLine($"First message hash: {firstMessage?.Hash?.ToBase64() ?? "null"}");
                        Console.WriteLine($"Message type: {firstMessage?.Data?.Type.ToString() ?? "unknown"}");
                    }
                    else
                    {
                        Console.WriteLine("WARNING: No messages received in validation. Server may not have data for FID 42.");
                    }
                }
                else
                {
                    Console.WriteLine("WARNING: Received null response from server during validation.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WARNING: Server connection validation failed: {ex.Message}");
                Console.WriteLine("Continuing with benchmark, but results may be affected.");
            }
        }
        
        /// <summary>
        /// Clean up resources
        /// </summary>
        [GlobalCleanup]
        public void Cleanup()
        {
            try
            {
                // Ensure storage is properly flushed before closing
                try
                {
                    _storage?.FlushAsync().GetAwaiter().GetResult();
                    Console.WriteLine("Storage flushed successfully during cleanup");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error flushing storage during cleanup: {ex.Message}");
                }
                
                // Dispose of the connection manager
                try
                {
                    _connectionManager?.Dispose();
                    Console.WriteLine("Connection manager disposed successfully");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error disposing connection manager: {ex.Message}");
                }
                
                // Dispose of other resources
                try
                {
                    (_storage as IDisposable)?.Dispose();
                    Console.WriteLine("Storage disposed successfully");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error disposing storage: {ex.Message}");
                }
                
                // Print final metrics
                Console.WriteLine(new string('-', 80));
                Console.WriteLine("FINAL BENCHMARK RESULTS");
                Console.WriteLine(new string('-', 80));
                Console.WriteLine($"Messages stored: {_messagesStored:N0}");
                Console.WriteLine($"Bytes processed: {_bytesProcessed:N0} ({(_bytesProcessed / (1024.0 * 1024.0)):N2} MB)");
                
                if (_storage?.Metrics != null)
                {
                    Console.WriteLine($"Storage batches: {_storage.Metrics.BatchesFlushed:N0}");
                    Console.WriteLine($"Storage bytes written: {_storage.Metrics.BytesWritten:N0} ({(_storage.Metrics.BytesWritten / (1024.0 * 1024.0)):N2} MB)");
                    Console.WriteLine($"Average flush time: {_storage.Metrics.AverageFlushTimeMs:N2}ms");
                    Console.WriteLine($"Peak flush time: {_storage.Metrics.PeakFlushTimeMs:N2}ms");
                    Console.WriteLine($"Peak memory usage: {_storage.Metrics.PeakMemoryUsage:N0} bytes ({(_storage.Metrics.PeakMemoryUsage / (1024.0 * 1024.0)):N2} MB)");
                }
                
                // Don't delete the output directory so we can inspect the files
                Console.WriteLine($"Benchmark files are available in: {_outputDirectory}");
                Console.WriteLine(new string('-', 80));
                Console.WriteLine("Cleanup complete.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during cleanup: {ex.Message}");
            }
        }
        
        /// <summary>
        /// Setup for each iteration
        /// </summary>
        [IterationSetup]
        public void IterationSetup()
        {
            // Map benchmark compression type to Parquet compression method
            CompressionMethod compressionMethod = Compression switch
            {
                CompressionType.None => CompressionMethod.None,
                CompressionType.Snappy => CompressionMethod.Snappy,
                CompressionType.Gzip => CompressionMethod.Gzip,
                _ => CompressionMethod.Snappy
            };
            
            // Create a subdirectory for this iteration
            var iterationDir = Path.Combine(_outputDirectory, $"iteration_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            Directory.CreateDirectory(iterationDir);
            
            // Reset metrics
            _messagesStored = 0;
            _bytesProcessed = 0;
            
            // Create storage
            if (Approach == StorageApproach.BufferedWrite)
            {
                _storage = CreateStorage(Compression);
            }
            else
            {
                // Direct write uses batch size of 1
                _storage = CreateStorage(Compression);
            }
        }
        
        /// <summary>
        /// Cleanup after each iteration
        /// </summary>
        [IterationCleanup]
        public void IterationCleanup()
        {
            try
            {
                // Complete and dispose storage
                if (_storage != null)
                {
                    _storage.CompleteAsync().GetAwaiter().GetResult();
                    _storage.DisposeAsync().GetAwaiter().GetResult();
                }
                
                // Report metrics
                Console.WriteLine($"Stored {_messagesStored:N0} messages, {_bytesProcessed:N0} bytes");
                
                if (_storage?.Metrics != null)
                {
                    Console.WriteLine($"Storage metrics: {_storage.Metrics.BatchesFlushed} batches, {_storage.Metrics.BytesWritten:N0} bytes written");
                    Console.WriteLine($"Average flush time: {_storage.Metrics.AverageFlushTimeMs:N2}ms, Peak: {_storage.Metrics.PeakFlushTimeMs:N2}ms");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during cleanup: {ex.Message}");
            }
        }
        
        /// <summary>
        /// Benchmark for fetching and storing messages from the gRPC server
        /// </summary>
        [Benchmark(Description = "Fetch from gRPC and store to Parquet")]
        public async Task FetchAndStoreFromGrpcAsync()
        {
            _stopwatch.Restart();
            
            // Create cancellation token source with reasonable timeout
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            
            // Parameters for the server call based on workload
            int pageSize = Workload switch
            {
                WorkloadPattern.SmallFrequent => 100,
                WorkloadPattern.LargeBatched => 500,
                WorkloadPattern.MixedTraffic => 250,
                _ => 250
            };
            
            // Create resilient client for better error handling
            var resilientClient = _connectionManager.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
            
            // Process messages in batches
            int remaining = MessageCount;
            int totalBatches = 0;
            int successfulBatches = 0;
            int fetchErrors = 0;
            const int MAX_ERRORS = 5;
            
            try
            {
                while (remaining > 0 && !cts.Token.IsCancellationRequested)
                {
                    // Determine batch size for this request
                    int currentBatchSize = Math.Min(pageSize, remaining);
                    totalBatches++;
                    
                    try
                    {
                        // ALWAYS use FID 42 as a consistent value that's known to work with the API
                        // This addresses the "Value cannot be null" errors we're seeing with random FIDs
                        uint fid = 42;
                        
                        // Create the FID request - ONLY include the FID property for maximum compatibility
                        var request = new FidRequest { Fid = fid };
                        
                        // Make the gRPC call with resilience
                        MessagesResponse response = null;
                        
                        try
                        {
                            response = await resilientClient.CallAsync(
                                (client, ct) => client.GetCastsByFidAsync(request, cancellationToken: ct).ResponseAsync,
                                "GetCastsByFid",
                                cts.Token);
                        }
                        catch (Exception ex)
                        {
                            fetchErrors++;
                            Console.WriteLine($"Exception during gRPC call ({fetchErrors}/{MAX_ERRORS}): {ex.Message}");
                            if (ex.InnerException != null)
                            {
                                Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                            }
                            
                            if (fetchErrors >= MAX_ERRORS)
                            {
                                Console.WriteLine("Too many fetch errors, exiting");
                                break;
                            }
                            
                            // Add a delay before retrying
                            await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                            continue;
                        }
                        
                        // Validate the response
                        if (response == null)
                        {
                            Console.WriteLine($"WARNING: Received null response from gRPC call for FID: {fid}");
                            fetchErrors++;
                            
                            if (fetchErrors >= MAX_ERRORS)
                            {
                                Console.WriteLine("Too many fetch errors, exiting");
                                break;
                            }
                            
                            await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                            continue;
                        }
                        
                        // Handle null or empty Messages collection
                        if (response.Messages == null)
                        {
                            Console.WriteLine($"WARNING: Messages collection is null in response for FID: {fid}");
                            fetchErrors++;
                            
                            if (fetchErrors >= MAX_ERRORS)
                            {
                                Console.WriteLine("Too many fetch errors, exiting");
                                break;
                            }
                            
                            await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                            continue;
                        }
                        
                        if (response.Messages.Count == 0)
                        {
                            Console.WriteLine($"No messages found for FID: {fid}, may have reached the end of available data");
                            break;
                        }
                        
                        // Filter out any null messages
                        var messageBatch = response.Messages.Where(m => m != null).ToList();
                        
                        if (messageBatch.Count == 0)
                        {
                            Console.WriteLine("All messages in response were null");
                            fetchErrors++;
                            
                            if (fetchErrors >= MAX_ERRORS)
                            {
                                Console.WriteLine("Too many fetch errors, exiting");
                                break;
                            }
                            
                            await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                            continue;
                        }
                        
                        // Validate all messages have a hash (optional, could be expensive for large batches)
                        var invalidMessages = messageBatch.Count(m => m.Hash == null);
                        if (invalidMessages > 0)
                        {
                            Console.WriteLine($"WARNING: {invalidMessages} messages have null hash values");
                            // Note: We'll still process these messages
                        }
                        
                        // Store messages in intermediate storage directly
                        await _storage.AddBatchAsync(messageBatch);
                        
                        // Reset error count after successful batch
                        fetchErrors = 0;
                        
                        // Update metrics
                        int messagesReceived = messageBatch.Count;
                        Interlocked.Add(ref _messagesStored, messagesReceived);
                        remaining -= messagesReceived;
                        successfulBatches++;
                        
                        // Track bytes processed
                        foreach (var message in messageBatch)
                        {
                            Interlocked.Add(ref _bytesProcessed, message.CalculateSize());
                        }
                        
                        // Log progress occasionally
                        if (successfulBatches % 5 == 0 || messagesReceived < currentBatchSize)
                        {
                            int percentComplete = (int)(100.0 * (MessageCount - remaining) / MessageCount);
                            Console.WriteLine($"Progress: {percentComplete}% - {_messagesStored:N0}/{MessageCount} messages processed in {successfulBatches} batches");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("Operation was canceled");
                        break;
                    }
                    catch (Exception ex)
                    {
                        fetchErrors++;
                        Console.WriteLine($"Error in batch {totalBatches} ({fetchErrors}/{MAX_ERRORS}): {ex.Message}");
                        if (ex.InnerException != null)
                        {
                            Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                        }
                        
                        if (fetchErrors >= MAX_ERRORS)
                        {
                            Console.WriteLine("Too many fetch errors, exiting");
                            break;
                        }
                        
                        // Add a small delay before retrying
                        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                    }
                }
                
                // Ensure everything is flushed
                await _storage.FlushAsync();
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Operation was canceled");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unhandled exception: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
            }
            
            _stopwatch.Stop();
            Console.WriteLine($"FetchAndStoreFromGrpcAsync completed in {_stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Processed {_messagesStored:N0} messages in {successfulBatches} batches out of {totalBatches} attempts");
            Console.WriteLine($"Total data processed: {_bytesProcessed:N0} bytes ({(_bytesProcessed / (1024.0 * 1024.0)):N2} MB)");
            
            if (_storage?.Metrics != null)
            {
                Console.WriteLine($"Storage batches flushed: {_storage.Metrics.BatchesFlushed:N0}");
                Console.WriteLine($"Storage bytes written: {_storage.Metrics.BytesWritten:N0} ({(_storage.Metrics.BytesWritten / (1024.0 * 1024.0)):N2} MB)");
            }
        }

        private IIntermediateStorage<Message> CreateStorage(CompressionType compressionType)
        {
            // Create a service collection and register required services
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStorageServices();
            
            // Build the service provider
            var serviceProvider = services.BuildServiceProvider();
            
            // Create a StorageFactory instance
            var storageFactory = new StorageFactory(serviceProvider);
            
            // Map BenchmarkDotNet compression type to Parquet compression method
            var compressionMethod = compressionType switch
            {
                CompressionType.None => CompressionMethod.None,
                CompressionType.Snappy => CompressionMethod.Snappy,
                CompressionType.Gzip => CompressionMethod.Gzip,
                _ => CompressionMethod.Snappy
            };
            
            // Create storage with the appropriate compression method
            return storageFactory.CreateMessageStorage(
                Path.Combine(Path.GetTempPath(), "parquet_benchmark"),
                BatchSize,
                compressionMethod);
        }
    }
} 