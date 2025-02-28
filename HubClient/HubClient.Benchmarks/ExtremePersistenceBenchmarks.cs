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

namespace HubClient.Benchmarks
{
    /// <summary>
    /// Extreme scaling benchmarks for persistence with 50M+ records
    /// This is designed to be run manually, not through BenchmarkDotNet
    /// </summary>
    public class ExtremePersistenceBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        private const int DefaultBatchSize = 10000;
        private const int DefaultRowGroupSize = 5000;
        private const CompressionType DefaultCompression = CompressionType.Snappy;
        
        /// <summary>
        /// Compression method for Parquet files
        /// </summary>
        public enum CompressionType
        {
            None,
            Snappy,
            Gzip
        }
        
        // Storage components
        private string _outputDirectory = null!;
        private IIntermediateStorage<Message> _storage = null!;
        
        // Concurrency pipeline
        private IConcurrencyPipeline<Message, Message> _pipeline = null!;
        
        // gRPC components
        private IGrpcConnectionManager _connectionManager = null!;
        private MinimalHubService.MinimalHubServiceClient _grpcClient = null!;
        
        // Metrics
        private long _messagesStored;
        private long _bytesProcessed;
        private long _fetchCalls;
        private long _fetchErrors;
        private CancellationTokenSource _globalCts = new();
        private Stopwatch _totalStopwatch = new();
        private Stopwatch _fetchStopwatch = new();
        private Stopwatch _storeStopwatch = new();
        
        /// <summary>
        /// Setup benchmark environment
        /// </summary>
        private void Setup(string outputPath, int batchSize)
        {
            Console.WriteLine("Setting up extreme persistence benchmark...");
            
            // Create output directory for benchmark
            _outputDirectory = outputPath;
            Directory.CreateDirectory(_outputDirectory);
            
            // Create gRPC connection manager and client based on LEARNINGS.md recommendation of 8 channels
            _connectionManager = new MultiplexedChannelManager(ServerEndpoint, 8);
            _grpcClient = _connectionManager.CreateClient<MinimalHubService.MinimalHubServiceClient>();
            
            Console.WriteLine("Using real data from gRPC server for benchmarking");
            Console.WriteLine($"Server endpoint: {ServerEndpoint}");
            
            // Setup the storage
            _storage = CreateStorage(DefaultCompression);
                
            // Setup a Channel-based pipeline to match the recommended approach from benchmarks
            _pipeline = new ChannelPipeline<Message, Message>(
                async (message, ct) => message, // Pass-through processor
                new ChannelPipelineOptions
                {
                    MaxConcurrentProcessors = Math.Min(16, Environment.ProcessorCount),
                    InputQueueCapacity = batchSize * 2,
                    OutputQueueCapacity = batchSize * 2
                });
                
            // Reset metrics
            _messagesStored = 0;
            _bytesProcessed = 0;
            _fetchCalls = 0;
            _fetchErrors = 0;
            
            Console.WriteLine($"Benchmark setup complete, messages will be stored in: {_outputDirectory}");
        }
        
        /// <summary>
        /// Cleans up resources used by the benchmark
        /// </summary>
        private async Task CleanupAsync()
        {
            Console.WriteLine("Cleaning up resources...");
            
            try
            {
                // Complete and dispose pipeline
                if (_pipeline != null)
                {
                    await _pipeline.CompleteAsync();
                    await _pipeline.DisposeAsync();
                }
                
                // Complete and dispose storage
                if (_storage != null)
                {
                    await _storage.CompleteAsync();
                    await _storage.DisposeAsync();
                }
                
                // Dispose connection manager
                _connectionManager?.Dispose();
                
                // Dispose cancellation token source
                _globalCts.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during cleanup: {ex.Message}");
            }
            
            // Report final metrics
            Console.WriteLine(new string('-', 80));
            Console.WriteLine("Final Metrics:");
            Console.WriteLine($"Total messages stored: {_messagesStored:N0}");
            Console.WriteLine($"Total bytes processed: {_bytesProcessed:N0} ({((double)_bytesProcessed / (1024 * 1024)):N2} MB)");
            Console.WriteLine($"Fetch calls: {_fetchCalls:N0}, Errors: {_fetchErrors:N0}");
            Console.WriteLine($"Total time: {_totalStopwatch.Elapsed}");
            Console.WriteLine($"Fetch time: {_fetchStopwatch.Elapsed}");
            Console.WriteLine($"Store time: {_storeStopwatch.Elapsed}");
            
            if (_storage?.Metrics != null)
            {
                Console.WriteLine($"Storage batches: {_storage.Metrics.BatchesFlushed:N0}");
                Console.WriteLine($"Storage bytes written: {_storage.Metrics.BytesWritten:N0} ({((double)_storage.Metrics.BytesWritten / (1024 * 1024)):N2} MB)");
                Console.WriteLine($"Average flush time: {_storage.Metrics.AverageFlushTimeMs:N2}ms");
                Console.WriteLine($"Peak flush time: {_storage.Metrics.PeakFlushTimeMs:N2}ms");
                Console.WriteLine($"Peak memory usage: {_storage.Metrics.PeakMemoryUsage:N0} bytes ({((double)_storage.Metrics.PeakMemoryUsage / (1024 * 1024)):N2} MB)");
            }
            
            Console.WriteLine($"Messages per second: {(_messagesStored / _totalStopwatch.Elapsed.TotalSeconds):N2}");
            Console.WriteLine($"MB per second: {(_bytesProcessed / (1024 * 1024) / _totalStopwatch.Elapsed.TotalSeconds):N2}");
            Console.WriteLine(new string('-', 80));
            
            Console.WriteLine("Benchmark files are in: " + _outputDirectory);
        }
        
        /// <summary>
        /// Runs the producer task that fetches messages from gRPC
        /// </summary>
        private async Task RunProducerAsync(int targetMessages, int pageSize, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Starting producer, target: {targetMessages:N0} messages, page size: {pageSize:N0}");
            
            int remaining = targetMessages;
            
            // Create resilient client for better error handling
            var resilientClient = _connectionManager.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
            
            while (remaining > 0 && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Update progress every 10 fetch calls
                    if (_fetchCalls % 10 == 0)
                    {
                        int percentComplete = (int)(100.0 * (targetMessages - remaining) / targetMessages);
                        Console.WriteLine($"Progress: {percentComplete}% - Fetched: {_messagesStored:N0} messages, Fetch calls: {_fetchCalls:N0}");
                    }
                    
                    // Start timing the fetch
                    _fetchStopwatch.Start();
                    
                    // Make the gRPC call
                    _fetchCalls++;
                    int currentBatchSize = Math.Min(pageSize, remaining);
                    
                    var request = new FidRequest
                    {
                        Fid = 42, // Hardcoded FID of 42
                        PageSize = (uint)currentBatchSize
                    };
                    
                    // Make the gRPC call with resilience
                    var response = await resilientClient.CallAsync(
                        (client, ct) => client.GetCastsByFidAsync(request, cancellationToken: ct).ResponseAsync,
                        "GetCastsByFid",
                        cancellationToken);
                    
                    // Stop timing the fetch
                    _fetchStopwatch.Stop();
                    
                    if (response?.Messages == null || response.Messages.Count == 0)
                    {
                        Console.WriteLine("No more messages from server, exiting");
                        break;
                    }
                    
                    // Start timing the store
                    _storeStopwatch.Start();
                    
                    // Enqueue messages to the pipeline
                    foreach (var message in response.Messages)
                    {
                        await _pipeline.EnqueueAsync(message, cancellationToken);
                        
                        // Track bytes
                        _bytesProcessed += message.CalculateSize();
                    }
                    
                    // Stop timing the store enqueue
                    _storeStopwatch.Stop();
                    
                    // Update counts
                    int messagesReceived = response.Messages.Count;
                    _messagesStored += messagesReceived;
                    remaining -= messagesReceived;
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    _fetchErrors++;
                    Console.WriteLine($"Error fetching from gRPC ({_fetchErrors}): {ex.Message}");
                    
                    if (_fetchErrors >= 5)
                    {
                        Console.WriteLine("Too many fetch errors, exiting");
                        break;
                    }
                    
                    // Retry after delay
                    await Task.Delay(1000, cancellationToken);
                }
            }
            
            Console.WriteLine("Producer task complete");
        }
        
        /// <summary>
        /// Runs the consumer task that stores messages to disk
        /// </summary>
        private async Task RunConsumerAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Starting consumer task");
            
            // Consume messages from the pipeline and store them
            await _pipeline.ConsumeAsync(async (message) =>
            {
                try
                {
                    // Store to intermediate storage
                    await _storage.AddAsync(message, cancellationToken);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine($"Error storing message: {ex.Message}");
                }
            }, cancellationToken);
            
            Console.WriteLine("Consumer task complete");
        }
        
        /// <summary>
        /// Runs the extreme persistence benchmark for a specified number of messages
        /// </summary>
        /// <param name="targetMessages">Number of messages to process</param>
        /// <param name="outputPath">Directory to store output files</param>
        /// <param name="batchSize">Size of batches for storage</param>
        /// <param name="pageSize">Size of pages to request from gRPC</param>
        /// <param name="timeoutMinutes">Timeout in minutes</param>
        public async Task RunBenchmarkAsync(
            int targetMessages = 50_000_000,
            string outputPath = "",
            int batchSize = DefaultBatchSize,
            int pageSize = 1000,
            int timeoutMinutes = 180)
        {
            // Use current directory if none specified
            if (string.IsNullOrWhiteSpace(outputPath))
            {
                outputPath = Path.Combine(
                    Path.GetTempPath(),
                    $"extreme_benchmark_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            }
            
            // Setup everything
            Setup(outputPath, batchSize);
            
            // Reset and start the timer
            _totalStopwatch.Reset();
            _totalStopwatch.Start();
            
            // Create a cancellation token source with timeout
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(
                _globalCts.Token,
                new CancellationTokenSource(TimeSpan.FromMinutes(timeoutMinutes)).Token);
                
            Console.WriteLine($"Starting extreme persistence benchmark:");
            Console.WriteLine($"- Target: {targetMessages:N0} messages");
            Console.WriteLine($"- Batch size: {batchSize:N0}");
            Console.WriteLine($"- Page size: {pageSize:N0}");
            Console.WriteLine($"- Timeout: {timeoutMinutes} minutes");
            Console.WriteLine($"- Output: {outputPath}");
            
            try
            {
                // Start the consumer task
                var consumerTask = RunConsumerAsync(cts.Token);
                
                // Run the producer and wait for it to complete
                await RunProducerAsync(targetMessages, pageSize, cts.Token);
                
                // Complete the pipeline
                await _pipeline.CompleteAsync(cts.Token);
                
                // Wait for the consumer to finish
                await consumerTask;
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                Console.WriteLine("Benchmark was cancelled due to timeout");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Benchmark failed with error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
            finally
            {
                // Stop the timer
                _totalStopwatch.Stop();
                
                // Clean up resources
                await CleanupAsync();
            }
        }
        
        /// <summary>
        /// Main entry point for running the benchmark manually
        /// </summary>
        public static async Task RunAsync(string[] args)
        {
            Console.WriteLine("=== Extreme Persistence Benchmark ===");
            
            // Parse command line arguments
            int targetMessages = 50_000_000; // Default 50M
            string outputPath = Path.Combine(Path.GetTempPath(), $"extreme_benchmark_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            int batchSize = DefaultBatchSize;
            int pageSize = 1000;
            int timeoutMinutes = 180;
            
            // Override with command line args if provided
            if (args.Length >= 1) int.TryParse(args[0], out targetMessages);
            if (args.Length >= 2) outputPath = args[1];
            if (args.Length >= 3) int.TryParse(args[2], out batchSize);
            if (args.Length >= 4) int.TryParse(args[3], out pageSize);
            if (args.Length >= 5) int.TryParse(args[4], out timeoutMinutes);
            
            // Run the benchmark
            var benchmark = new ExtremePersistenceBenchmarks();
            
            // Log memory at start
            Console.WriteLine($"Initial memory: {GC.GetTotalMemory(true) / (1024 * 1024):N2} MB");
            
            // Run the benchmark
            await benchmark.RunBenchmarkAsync(
                targetMessages,
                outputPath,
                batchSize,
                pageSize,
                timeoutMinutes);
                
            // Log memory at end
            Console.WriteLine($"Final memory: {GC.GetTotalMemory(true) / (1024 * 1024):N2} MB");
            
            Console.WriteLine("=== Benchmark Complete ===");
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
                _outputDirectory,
                DefaultBatchSize,
                compressionMethod,
                DefaultRowGroupSize);
        }
    }
}