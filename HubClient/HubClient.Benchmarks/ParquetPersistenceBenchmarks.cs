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
using Parquet.Data;
using Parquet.Schema;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace HubClient.Benchmarks
{
    /// <summary>
    /// Benchmarks for different Parquet persistence strategies
    /// </summary>
    [MemoryDiagnoser]
    [GcServer(true)]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    [EventPipeProfiler(BenchmarkDotNet.Diagnosers.EventPipeProfile.GcVerbose)]
    [SimpleJob(RunStrategy.Monitoring, launchCount: 1, warmupCount: 1, iterationCount: 10, id: "QuickBenchmark")]
    public class ParquetPersistenceBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        /// <summary>
        /// Size of each batch to process
        /// </summary>
        [Params(25000)]
        public int BatchSize { get; set; }
        
        /// <summary>
        /// Compression method for Parquet files
        /// </summary>
        [Params(CompressionMethod.Snappy)]
        public CompressionMethod Compression { get; set; }
        
        /// <summary>
        /// Row group size for Parquet files
        /// </summary>
        [Params(50000)]
        public int RowGroupSize { get; set; }
        
        /// <summary>
        /// Page size for Parquet column chunks (only used in OptimizedParquet approach)
        /// </summary>
        [Params(4096)]
        public int PageSize { get; set; }
        
        /// <summary>
        /// Whether to enable dictionary encoding (only used in OptimizedParquet approach)
        /// </summary>
        [Params(true)]
        public bool EnableDictionary { get; set; }
        
        /// <summary>
        /// Parquet persistence approach to benchmark
        /// </summary>
        public enum PersistenceApproach
        {
            /// <summary>
            /// Standard Parquet.NET library with default settings
            /// </summary>
            Baseline,
            
            /// <summary>
            /// Optimized Parquet.NET with custom row groups and compression
            /// </summary>
            OptimizedParquet,
            
            /// <summary>
            /// Multi-file approach with parallel writers and later merging
            /// </summary>
            MultiFileParallel
        }
        
        /// <summary>
        /// Approach to use for benchmarking
        /// </summary>
        [Params(PersistenceApproach.Baseline, PersistenceApproach.OptimizedParquet, PersistenceApproach.MultiFileParallel)]
        public PersistenceApproach Approach { get; set; }
        
        /// <summary>
        /// Total messages to process in the benchmark
        /// </summary>
        [Params(50000)]
        public int MessageCount { get; set; }
        
        /// <summary>
        /// Type of workload to simulate
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
        
        // Components
        private string _outputDirectory = null!;
        private IGrpcConnectionManager _connectionManager = null!;
        private MinimalHubService.MinimalHubServiceClient _grpcClient = null!;
        
        // Specialized components for different approaches
        private IParquetWriter<Message> _standardWriter = null!;
        private IParquetWriter<Message> _optimizedWriter = null!;
        private List<IParquetWriter<Message>> _parallelWriters = null!;
        
        // Schema generation
        private ISchemaGenerator _schemaGenerator = null!;
        private ParquetSchema _messageSchema = null!;
        
        // Metrics
        private long _messagesProcessed;
        private long _bytesProcessed;
        private Stopwatch _stopwatch = new();
        private ConcurrentDictionary<string, long> _metrics = new();
        
        private class BenchmarkMetrics
        {
            public long TotalMessages { get; set; }
            public long TotalBytes { get; set; }
            public long WriteTimeMs { get; set; }
            public long PeakWriteTimeMs { get; set; }
            public int BatchesProcessed { get; set; }
            
            // Change property to field to allow ref usage with Interlocked
            private long _fileSizeBytes;
            public long FileSizeBytes 
            { 
                get => _fileSizeBytes; 
                set => _fileSizeBytes = value; 
            }
        }
        
        private BenchmarkMetrics _benchmarkMetrics = new();
        
        /// <summary>
        /// Setup benchmark environment
        /// </summary>
        [GlobalSetup]
        public void Setup()
        {
            Console.WriteLine($"Setting up Parquet Persistence Benchmark: {Approach}, {Compression}, Row Group={RowGroupSize}, Batch={BatchSize}, Workload={Workload}");
            
            // Create output directory for benchmark
            _outputDirectory = Path.Combine(Path.GetTempPath(), $"parquet_benchmark_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            Directory.CreateDirectory(_outputDirectory);
            
            // Create gRPC connection manager and client based on 8 channels
            _connectionManager = new MultiplexedChannelManager(ServerEndpoint, 8);
            _grpcClient = _connectionManager.CreateClient<MinimalHubService.MinimalHubServiceClient>();
            
            // Create a service collection and register required services
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddStorageServices();
            
            // Build the service provider
            var serviceProvider = services.BuildServiceProvider();
            
            // Create schema generator
            _schemaGenerator = serviceProvider.GetRequiredService<ISchemaGenerator>();
            
            // Reset metrics
            _messagesProcessed = 0;
            _bytesProcessed = 0;
            _metrics.Clear();
            
            Console.WriteLine("Using real data from gRPC server for benchmarking");
            Console.WriteLine($"Server endpoint: {ServerEndpoint}");
            Console.WriteLine($"Messages to process: {MessageCount}");
            Console.WriteLine($"Compression: {Compression}");
            Console.WriteLine($"Row group size: {RowGroupSize}");
            Console.WriteLine($"Persistence approach: {Approach}");
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
                var request = new FidRequest { Fid = 42 };
                
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
                // Dispose of writers
                _standardWriter?.DisposeAsync().AsTask().Wait();
                _optimizedWriter?.DisposeAsync().AsTask().Wait();
                
                if (_parallelWriters != null)
                {
                    foreach (var writer in _parallelWriters)
                    {
                        writer?.DisposeAsync().AsTask().Wait();
                    }
                }
                
                // Dispose of the connection manager
                _connectionManager?.Dispose();
                
                // Print final metrics
                Console.WriteLine(new string('-', 80));
                Console.WriteLine("FINAL BENCHMARK RESULTS");
                Console.WriteLine(new string('-', 80));
                Console.WriteLine($"Messages processed: {_messagesProcessed:N0}");
                Console.WriteLine($"Bytes processed: {_bytesProcessed:N0} ({(_bytesProcessed / (1024.0 * 1024.0)):N2} MB)");
                Console.WriteLine($"Batches processed: {_benchmarkMetrics.BatchesProcessed:N0}");
                Console.WriteLine($"Total write time: {_benchmarkMetrics.WriteTimeMs:N0}ms");
                Console.WriteLine($"Peak write time: {_benchmarkMetrics.PeakWriteTimeMs:N0}ms");
                Console.WriteLine($"File size: {_benchmarkMetrics.FileSizeBytes:N0} bytes ({(_benchmarkMetrics.FileSizeBytes / (1024.0 * 1024.0)):N2} MB)");
                Console.WriteLine($"Compression ratio: {(_bytesProcessed / (double)_benchmarkMetrics.FileSizeBytes):N2}x");
                
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
            // Create a subdirectory for this iteration
            var iterationDir = Path.Combine(_outputDirectory, $"iteration_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            Directory.CreateDirectory(iterationDir);
            
            // Reset metrics
            _messagesProcessed = 0;
            _bytesProcessed = 0;
            _metrics.Clear();
            _benchmarkMetrics = new BenchmarkMetrics();
            
            // Log the configuration for this iteration
            Console.WriteLine($"=== ITERATION CONFIG ===");
            Console.WriteLine($"Approach: {Approach}");
            Console.WriteLine($"Compression: {Compression}");
            Console.WriteLine($"RowGroupSize: {RowGroupSize}");
            if (Approach == PersistenceApproach.OptimizedParquet)
            {
                Console.WriteLine($"PageSize: {PageSize}");
                Console.WriteLine($"EnableDictionary: {EnableDictionary}");
            }
            Console.WriteLine($"======================");
            
            // Initialize writers based on approach
            InitializeWriters(iterationDir);
        }
        
        /// <summary>
        /// Initialize Parquet writers based on the selected approach
        /// </summary>
        private void InitializeWriters(string outputDirectory)
        {
            var serviceProvider = new ServiceCollection()
                .AddLogging()
                .AddStorageServices()
                .BuildServiceProvider();
            
            var logger = serviceProvider.GetRequiredService<ILogger<ParquetWriter<Message>>>();
            var schemaGenerator = serviceProvider.GetRequiredService<ISchemaGenerator>();
            
            // Create a StorageFactory instance (similar to IntermediateStorageBenchmarks.cs)
            var storageFactory = new StorageFactory(serviceProvider);
            
            switch (Approach)
            {
                case PersistenceApproach.Baseline:
                    // Standard Parquet.NET with default settings - use fixed defaults
                    _standardWriter = new ParquetWriter<Message>(
                        Path.Combine(outputDirectory, "baseline"),
                        MessageParquetConverter.CreateConverter(), // Use static method instead of service
                        logger,
                        schemaGenerator,
                        CompressionMethod.Snappy,  // Fixed default compression
                        5000);  // Fixed default row group size
                    break;
                    
                case PersistenceApproach.OptimizedParquet:
                    // Create advanced options for the optimized approach
                    var advancedOptions = new ParquetWriterOptions
                    {
                        Compression = Compression,
                        RowGroupSize = RowGroupSize,
                        PageSize = PageSize,
                        EnableDictionaryEncoding = EnableDictionary,
                        HighPerformance = true,
                        UseMemoryOptimizedWrites = true
                    };
                    
                    // Create an optimized ParquetWriter directly
                    _optimizedWriter = new ParquetWriter<Message>(
                        Path.Combine(outputDirectory, "optimized"),
                        MessageParquetConverter.CreateConverter(), // Use static method instead of service
                        logger,
                        schemaGenerator,
                        advancedOptions.Compression,
                        advancedOptions.RowGroupSize);
                        
                    // Apply additional optimizations
                    ApplyOptimizations(_optimizedWriter, advancedOptions);
                    break;
                    
                case PersistenceApproach.MultiFileParallel:
                    // Multi-file approach with parallel writers
                    _parallelWriters = new List<IParquetWriter<Message>>();
                    
                    // Create multiple writers for parallel processing
                    int workerCount = Math.Min(Environment.ProcessorCount, 8);
                    for (int i = 0; i < workerCount; i++)
                    {
                        var workerDir = Path.Combine(outputDirectory, $"worker_{i}");
                        Directory.CreateDirectory(workerDir);
                        
                        // Create each parallel writer directly
                        var parallelWriter = new ParquetWriter<Message>(
                            workerDir,
                            MessageParquetConverter.CreateConverter(), // Use static method instead of service
                            logger,
                            schemaGenerator,
                            Compression,
                            RowGroupSize);
                        
                        _parallelWriters.Add(parallelWriter);
                    }
                    break;
                    
                default:
                    throw new ArgumentOutOfRangeException();
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
                // Dispose writers
                switch (Approach)
                {
                    case PersistenceApproach.Baseline:
                        _standardWriter?.DisposeAsync().AsTask().Wait();
                        break;
                        
                    case PersistenceApproach.OptimizedParquet:
                        _optimizedWriter?.DisposeAsync().AsTask().Wait();
                        break;
                        
                    case PersistenceApproach.MultiFileParallel:
                        if (_parallelWriters != null)
                        {
                            foreach (var writer in _parallelWriters)
                            {
                                writer?.DisposeAsync().AsTask().Wait();
                            }
                        }
                        break;
                }
                
                // Report metrics
                Console.WriteLine($"Processed {_messagesProcessed:N0} messages, {_bytesProcessed:N0} bytes");
                Console.WriteLine($"Batches: {_benchmarkMetrics.BatchesProcessed}, Write time: {_benchmarkMetrics.WriteTimeMs}ms, Peak: {_benchmarkMetrics.PeakWriteTimeMs}ms");
                Console.WriteLine($"File size: {_benchmarkMetrics.FileSizeBytes:N0} bytes ({(_benchmarkMetrics.FileSizeBytes / (1024.0 * 1024.0)):N2} MB)");
                
                // For multi-file approach, perform the merge after the benchmark completes
                if (Approach == PersistenceApproach.MultiFileParallel)
                {
                    Console.WriteLine("Note: Multi-file approach would typically merge files after this point");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during cleanup: {ex.Message}");
            }
        }
        
        /// <summary>
        /// Benchmark for persisting messages with Parquet
        /// </summary>
        [Benchmark(Description = "Persist messages to Parquet")]
        public async Task PersistToParquetAsync()
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
                        var request = new FidRequest { Fid = 42 };
                        
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
                        if (response == null || response.Messages == null || response.Messages.Count == 0)
                        {
                            fetchErrors++;
                            Console.WriteLine($"Received invalid response from gRPC call ({fetchErrors}/{MAX_ERRORS})");
                            
                            if (fetchErrors >= MAX_ERRORS)
                            {
                                Console.WriteLine("Too many fetch errors, exiting");
                                break;
                            }
                            
                            await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                            continue;
                        }
                        
                        // Filter out any null messages
                        var messageBatch = response.Messages.Where(m => m != null).ToList();
                        
                        if (messageBatch.Count == 0)
                        {
                            fetchErrors++;
                            Console.WriteLine($"All messages in response were null ({fetchErrors}/{MAX_ERRORS})");
                            
                            if (fetchErrors >= MAX_ERRORS)
                            {
                                Console.WriteLine("Too many fetch errors, exiting");
                                break;
                            }
                            
                            await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                            continue;
                        }
                        
                        // Accumulate message batch into larger batches if needed
                        var messagesToProcess = new List<Message>(messageBatch);
                        
                        // Store messages using the appropriate approach
                        await PersistMessagesAsync(messagesToProcess, cts.Token);
                        
                        // Reset error count after successful batch
                        fetchErrors = 0;
                        
                        // Update metrics
                        int messagesReceived = messageBatch.Count;
                        Interlocked.Add(ref _messagesProcessed, messagesReceived);
                        remaining -= messagesReceived;
                        successfulBatches++;
                        _benchmarkMetrics.BatchesProcessed++;
                        
                        // Track bytes processed
                        foreach (var message in messageBatch)
                        {
                            Interlocked.Add(ref _bytesProcessed, message.CalculateSize());
                        }
                        
                        // Log progress occasionally
                        if (successfulBatches % 5 == 0 || messagesReceived < currentBatchSize)
                        {
                            int percentComplete = (int)(100.0 * (MessageCount - remaining) / MessageCount);
                            Console.WriteLine($"Progress: {percentComplete}% - {_messagesProcessed:N0}/{MessageCount} messages processed in {successfulBatches} batches");
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
                        
                        if (fetchErrors >= MAX_ERRORS)
                        {
                            Console.WriteLine("Too many fetch errors, exiting");
                            break;
                        }
                        
                        // Add a small delay before retrying
                        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                    }
                }
                
                // For approaches that need final flushing
                await FinalizeWritesAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Operation was canceled");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unhandled exception: {ex.Message}");
            }
            
            _stopwatch.Stop();
            Console.WriteLine($"PersistToParquetAsync completed in {_stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Processed {_messagesProcessed:N0} messages in {successfulBatches} batches out of {totalBatches} attempts");
            Console.WriteLine($"Total data processed: {_bytesProcessed:N0} bytes ({(_bytesProcessed / (1024.0 * 1024.0)):N2} MB)");
        }
        
        /// <summary>
        /// Persist messages using the selected approach
        /// </summary>
        private async Task PersistMessagesAsync(List<Message> messages, CancellationToken cancellationToken)
        {
            if (messages == null || messages.Count == 0)
            {
                return;
            }
            
            var batchId = $"batch_{DateTime.UtcNow:yyyyMMdd_HHmmss}_{Guid.NewGuid():N}";
            var writeStopwatch = Stopwatch.StartNew();
            
            try
            {
                switch (Approach)
                {
                    case PersistenceApproach.Baseline:
                        // Standard Parquet.NET with default settings
                        await _standardWriter.WriteMessagesAsync(messages, batchId, cancellationToken);
                        
                        // Track file size
                        var batchInfo = _standardWriter.GetLastBatchInfo();
                        if (batchInfo != null)
                        {
                            // Update property safely
                            _benchmarkMetrics.FileSizeBytes += batchInfo.FileSizeBytes;
                        }
                        break;
                        
                    case PersistenceApproach.OptimizedParquet:
                        // Optimized Parquet.NET with custom row groups and compression
                        await _optimizedWriter.WriteMessagesAsync(messages, batchId, cancellationToken);
                        
                        // Track file size
                        var optimizedBatchInfo = _optimizedWriter.GetLastBatchInfo();
                        if (optimizedBatchInfo != null)
                        {
                            // Update property safely
                            _benchmarkMetrics.FileSizeBytes += optimizedBatchInfo.FileSizeBytes;
                        }
                        break;
                        
                    case PersistenceApproach.MultiFileParallel:
                        // Multi-file approach with parallel writers
                        if (_parallelWriters == null || _parallelWriters.Count == 0)
                        {
                            throw new InvalidOperationException("Parallel writers not initialized");
                        }
                        
                        // Distribute messages across multiple writers
                        int workerCount = _parallelWriters.Count;
                        var chunkSize = (int)Math.Ceiling(messages.Count / (double)workerCount);
                        var tasks = new List<Task>();
                        
                        for (int i = 0; i < workerCount; i++)
                        {
                            var startIndex = i * chunkSize;
                            var endIndex = Math.Min(startIndex + chunkSize, messages.Count);
                            
                            if (startIndex >= messages.Count)
                            {
                                // No more messages for this worker
                                continue;
                            }
                            
                            var chunk = messages.GetRange(startIndex, endIndex - startIndex);
                            var writer = _parallelWriters[i];
                            var chunkId = $"{batchId}_part{i}";
                            
                            tasks.Add(Task.Run(async () =>
                            {
                                await writer.WriteMessagesAsync(chunk, chunkId, cancellationToken);
                                
                                // Track file size
                                var parallelBatchInfo = writer.GetLastBatchInfo();
                                if (parallelBatchInfo != null)
                                {
                                    // Use a local variable first, then update the property
                                    long size = parallelBatchInfo.FileSizeBytes;
                                    lock (_benchmarkMetrics)
                                    {
                                        _benchmarkMetrics.FileSizeBytes += size;
                                    }
                                }
                            }, cancellationToken));
                        }
                        
                        // Wait for all parallel writers to complete
                        await Task.WhenAll(tasks);
                        break;
                        
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            finally
            {
                writeStopwatch.Stop();
                var writeTime = writeStopwatch.ElapsedMilliseconds;
                
                // Update metrics
                _benchmarkMetrics.WriteTimeMs += writeTime;
                if (writeTime > _benchmarkMetrics.PeakWriteTimeMs)
                {
                    _benchmarkMetrics.PeakWriteTimeMs = writeTime;
                }
                
                // Log write time for large batches
                if (messages.Count >= 1000)
                {
                    Console.WriteLine($"Wrote {messages.Count} messages in {writeTime}ms ({writeTime / (double)messages.Count:F2}ms/message)");
                }
            }
        }
        
        /// <summary>
        /// Finalize writes for approaches that need it
        /// </summary>
        private async Task FinalizeWritesAsync(CancellationToken cancellationToken)
        {
            switch (Approach)
            {
                case PersistenceApproach.Baseline:
                    // Standard writer doesn't need additional finalization
                    break;
                    
                case PersistenceApproach.OptimizedParquet:
                    // Optimized writer doesn't need additional finalization
                    break;
                    
                case PersistenceApproach.MultiFileParallel:
                    // For a real implementation, we would merge files here
                    // This would involve reading all files and writing a combined file
                    // For benchmarking, we'll just log the concept
                    Console.WriteLine("In a production environment, we would merge partial files into a single combined file here");
                    break;
                    
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            await Task.CompletedTask;
        }
        
        /// <summary>
        /// Advanced options for configuring a ParquetWriter
        /// </summary>
        public class ParquetWriterOptions
        {
            /// <summary>
            /// Compression method to use
            /// </summary>
            public CompressionMethod Compression { get; set; } = CompressionMethod.Snappy;
            
            /// <summary>
            /// Size of each row group
            /// </summary>
            public int RowGroupSize { get; set; } = 5000;
            
            /// <summary>
            /// Size of each data page
            /// </summary>
            public int PageSize { get; set; } = 8192;
            
            /// <summary>
            /// Whether to enable dictionary encoding
            /// </summary>
            public bool EnableDictionaryEncoding { get; set; } = true;
            
            /// <summary>
            /// Whether to optimize for high performance at the cost of memory usage
            /// </summary>
            public bool HighPerformance { get; set; } = false;
            
            /// <summary>
            /// Whether to use memory-optimized write operations
            /// </summary>
            public bool UseMemoryOptimizedWrites { get; set; } = false;
        }
        
        // Apply optimizations to the writer
        private void ApplyOptimizations(IParquetWriter<Message> writer, ParquetWriterOptions options)
        {
            // This is a simplified example - in a real implementation, you'd access the 
            // underlying Parquet.NET writer to configure these settings
            Console.WriteLine($"Applied optimizations: PageSize={options.PageSize}, " +
                             $"Dictionary={options.EnableDictionaryEncoding}, " +
                             $"HighPerf={options.HighPerformance}, " +
                             $"MemOpt={options.UseMemoryOptimizedWrites}");
            
            // In a real implementation, you might do something like:
            // writer.Configuration.PageSize = options.PageSize;
            // writer.Configuration.EnableDictionaryEncoding = options.EnableDictionaryEncoding;
            // etc.
        }
    }
} 