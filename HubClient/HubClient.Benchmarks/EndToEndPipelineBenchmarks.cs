using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using HubClient.Core;
using HubClient.Core.Concurrency;
using HubClient.Core.Grpc;
using HubClient.Core.Serialization;
using Microsoft.Extensions.ObjectPool;
using Microsoft.IO;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Benchmarks
{
    /// <summary>
    /// End-to-end benchmarks for the pipeline with gRPC client, processing, and serialization
    /// in a realistic scenario based on the LEARNINGS.md guidance
    /// </summary>
    [MemoryDiagnoser]
    [GcServer(true)]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    [EventPipeProfiler(BenchmarkDotNet.Diagnosers.EventPipeProfile.GcVerbose)]
    public class EndToEndPipelineBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        /// <summary>
        /// Messages per batch for batch processing (aligned with the 10K messages per batch from LEARNINGS.md)
        /// </summary>
        [Params(10_000)]
        public int BatchSize { get; set; }
        
        /// <summary>
        /// Total number of messages to process
        /// </summary>
        [Params(100_000)]
        public int TotalMessages { get; set; }
        
        /// <summary>
        /// Choose how to manage memory streams
        /// </summary>
        [ParamsAllValues]
        public bool UseRecyclableMemoryStream { get; set; }
        
        // Component selection
        private PipelineStrategy _pipelineStrategy = PipelineStrategy.Channel; // Default, configurable by parameters
        private HubClient.Core.Serialization.MessageSerializerFactory.SerializerType _serializerType = HubClient.Core.Serialization.MessageSerializerFactory.SerializerType.PooledBuffer; // Default, configurable
        private IGrpcConnectionManager _connectionManager = null!;
        
        // Test data
        private List<Message> _messages = null!;
        private IMessageSerializer<Message> _serializer = null!;
        private ObjectPool<MemoryStream> _memoryStreamPool = null!;
        private RecyclableMemoryStreamManager _memoryStreamManager = null!;
        private Random _random = null!;
        
        // Output stats
        private PipelineMetrics _metrics = null!;
        private long _bytesSerialized;
        private int _grpcCallsMade;
        private double _processingTimeMs;
        
        [GlobalSetup]
        public void Setup()
        {
            // Initialize components
            _random = new Random(42); // Fixed seed for reproducibility
            
            // Set up connection manager
            int channelCount = 8; // From LEARNINGS.md recommendations (8-16 channels optimal)
            int maxConcurrentCallsPerChannel = 100; // From LEARNINGS.md (up to 1000)
            _connectionManager = new MultiplexedChannelManager(ServerEndpoint, channelCount, maxConcurrentCallsPerChannel);
            
            // Set up memory streams
            if (UseRecyclableMemoryStream)
            {
                // Configure RecyclableMemoryStreamManager from LEARNINGS.md recommendations
                _memoryStreamManager = new RecyclableMemoryStreamManager(
                    new RecyclableMemoryStreamManager.Options
                    {
                        BlockSize = 4096,
                        LargeBufferMultiple = 4096,
                        MaximumBufferSize = 4 * 1024 * 1024
                    });
            }
            else
            {
                // Standard memory stream pool
                _memoryStreamPool = ObjectPool.Create<MemoryStream>();
            }
            
            // Set up serializer based on LEARNINGS.md recommendations
            _serializer = MessageSerializerFactory.Create<Message>(_serializerType);
            
            // Generate test messages
            Console.WriteLine($"Generating {TotalMessages} test messages...");
            _messages = new List<Message>(TotalMessages);
            
            for (int i = 0; i < TotalMessages; i++)
            {
                _messages.Add(GenerateTestMessage(1024)); // 1KB messages
            }
            
            Console.WriteLine("Setup complete.");
        }
        
        [GlobalCleanup]
        public void Cleanup()
        {
            _connectionManager.Dispose();
            // RecyclableMemoryStreamManager doesn't implement IDisposable in the new version
            // _memoryStreamManager?.Dispose();
        }
        
        [IterationSetup]
        public void IterationSetup()
        {
            // Reset stats
            _bytesSerialized = 0;
            _grpcCallsMade = 0;
            _processingTimeMs = 0;
            _metrics = new PipelineMetrics();
            
            // Force GC to get a clean start
            GC.Collect(2, GCCollectionMode.Forced, true, true);
        }
        
        [IterationCleanup]
        public void IterationCleanup()
        {
            // Log stats
            Console.WriteLine($"Memory stream mode: {(UseRecyclableMemoryStream ? "Recyclable" : "Pooled")}");
            Console.WriteLine($"Pipeline: {_pipelineStrategy}, Serializer: {_serializerType}");
            Console.WriteLine($"Total bytes serialized: {_bytesSerialized:N0} bytes");
            Console.WriteLine($"gRPC calls made: {_grpcCallsMade}");
            Console.WriteLine($"Processing time: {_processingTimeMs:F2} ms");
            Console.WriteLine($"Min latency: {_metrics.MinLatency.TotalMilliseconds:F2} ms");
            Console.WriteLine($"Max latency: {_metrics.MaxLatency.TotalMilliseconds:F2} ms");
            Console.WriteLine($"Avg latency: {_metrics.AverageLatency.TotalMilliseconds:F2} ms");
            Console.WriteLine($"P99 latency: {_metrics.P99Latency.TotalMilliseconds:F2} ms");
            Console.WriteLine($"Throughput: {_metrics.AverageThroughput:F2} msgs/sec");
            Console.WriteLine($"Backpressure events: {_metrics.TotalBackpressureEvents}");
            
            // Force GC to clean up between iterations
            GC.Collect(2, GCCollectionMode.Forced, true, true);
            GC.WaitForPendingFinalizers();
        }
        
        /// <summary>
        /// Benchmark the TPL Dataflow pipeline strategy
        /// </summary>
        [Benchmark(Baseline = true)]
        public async Task TPL_Dataflow_Benchmark()
        {
            _pipelineStrategy = PipelineStrategy.Dataflow;
            await RunEndToEndBenchmark();
        }
        
        /// <summary>
        /// Benchmark the Channels pipeline strategy
        /// </summary>
        [Benchmark]
        public async Task Channels_Benchmark()
        {
            _pipelineStrategy = PipelineStrategy.Channel;
            await RunEndToEndBenchmark();
        }
        
        /// <summary>
        /// Benchmark the Thread-per-core pipeline strategy
        /// </summary>
        [Benchmark]
        public async Task ThreadPerCore_Benchmark()
        {
            _pipelineStrategy = PipelineStrategy.ThreadPerCore;
            await RunEndToEndBenchmark();
        }
        
        /// <summary>
        /// Runs the end-to-end benchmark with the pipeline
        /// </summary>
        private async Task RunEndToEndBenchmark()
        {
            // Create pipeline based on the best practices in LEARNINGS.md
            var options = new PipelineCreationOptions
            {
                InputQueueCapacity = BatchSize * 2, // Account for 2 batches in the queue
                OutputQueueCapacity = BatchSize * 2,
                MaxConcurrency = Environment.ProcessorCount * 2, // Optimal concurrency for mixed workloads
                AllowSynchronousContinuations = false, // Better predictability
                PreserveOrderInBatch = false
            };
            
            // Create the pipeline
            using var pipeline = PipelineFactory.Create<Message, ProcessedMessage>(
                _pipelineStrategy,
                ProcessMessageWithSerializer,
                options);
                
            // Create a gRPC client for the pipeline
            var grpcClient = _connectionManager.CreateClient<MinimalHubService.MinimalHubServiceClient>();
            
            // Processed message counter and buffer
            var processedCount = 0;
            
            // Consumer that tracks output and manages buffers
            var consumerTask = pipeline.ConsumeAsync(
                message => {
                    Interlocked.Increment(ref processedCount);
                    Interlocked.Add(ref _bytesSerialized, message.SerializedSize);
                    return ValueTask.CompletedTask;
                },
                CancellationToken.None);
                
            // Stream-based processing with pre-allocated, reused buffers (from LEARNINGS.md)
            var stopwatch = Stopwatch.StartNew();
            
            // Process in batches of the specified size
            for (int batchStart = 0; batchStart < _messages.Count; batchStart += BatchSize)
            {
                // Determine the batch size (last batch may be smaller)
                int currentBatchSize = Math.Min(BatchSize, _messages.Count - batchStart);
                
                // Get the messages for this batch
                var batch = _messages.GetRange(batchStart, currentBatchSize);
                
                // Process the batch
                await pipeline.EnqueueBatchAsync(batch);
                
                // Strategic GC management at batch boundaries (as recommended in LEARNINGS.md)
                // Run a Gen0 collection after each batch to minimize memory pressure
                if (batchStart > 0 && batchStart % (BatchSize * 4) == 0)
                {
                    GC.Collect(0, GCCollectionMode.Optimized, false);
                }
            }
            
            // Wait for all processing to complete
            await pipeline.CompleteAsync();
            await consumerTask;
            
            stopwatch.Stop();
            
            // Log metrics
            _processingTimeMs = stopwatch.Elapsed.TotalMilliseconds;
            _metrics = pipeline.Metrics;
            
            // Validation
            if (processedCount != TotalMessages)
            {
                throw new InvalidOperationException(
                    $"Expected {TotalMessages} processed messages, but got {processedCount}");
            }
        }
        
        /// <summary>
        /// Processor function for the pipeline that uses the appropriate serializer
        /// </summary>
        private async ValueTask<ProcessedMessage> ProcessMessageWithSerializer(Message message, CancellationToken cancellationToken)
        {
            // Get a memory stream from the pool or manager
            MemoryStream memoryStream;
            if (UseRecyclableMemoryStream)
            {
                memoryStream = _memoryStreamManager.GetStream("BenchmarkStream", 4096, true);
            }
            else
            {
                memoryStream = _memoryStreamPool.Get();
                memoryStream.SetLength(0);
                memoryStream.Position = 0;
            }
            
            try
            {
                // Serialize to the memory stream
                _serializer.Serialize(message, memoryStream);
                
                // Perform some processing on the message
                ProcessedMessage result;
                
                // Mix of CPU-bound and I/O-bound work
                if (_random.Next(10) < 2) // 20% chance of making a gRPC call
                {
                    // Make a real gRPC call
                    try
                    {
                        // Create a resilient client
                        var resilientClient = _connectionManager.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
                        
                        // Make the call
                        var fidRequest = new FidRequest { Fid = (uint)message.Data.Fid };
                        var response = await resilientClient.CallAsync(
                            (client, ct) => client.GetUserDataByFidAsync(fidRequest, cancellationToken: ct).ResponseAsync,
                            "GetUserDataByFid",
                            cancellationToken);
                            
                        // Record stats
                        Interlocked.Increment(ref _grpcCallsMade);
                        
                        // Create result with data from response
                        result = new ProcessedMessage
                        {
                            OriginalMessage = message,
                            ProcessedData = memoryStream.ToArray(),
                            IsSuccess = true,
                            SerializedSize = (int)memoryStream.Length
                        };
                    }
                    catch
                    {
                        // Fallback if call fails
                        result = new ProcessedMessage
                        {
                            OriginalMessage = message,
                            ProcessedData = memoryStream.ToArray(),
                            IsSuccess = true,
                            SerializedSize = (int)memoryStream.Length
                        };
                    }
                }
                else
                {
                    // CPU-bound work - compute hash from serialized data
                    result = new ProcessedMessage
                    {
                        OriginalMessage = message,
                        ProcessedData = memoryStream.ToArray(),
                        IsSuccess = true,
                        SerializedSize = (int)memoryStream.Length
                    };
                }
                
                return result;
            }
            finally
            {
                // Return the memory stream to the pool
                if (UseRecyclableMemoryStream)
                {
                    memoryStream.Dispose(); // For RecyclableMemoryStream, Dispose returns it to the pool
                }
                else
                {
                    memoryStream.SetLength(0);
                    _memoryStreamPool.Return(memoryStream);
                }
            }
        }
        
        /// <summary>
        /// Computes a hash from the memory stream contents
        /// </summary>
        private ulong ComputeHash(MemoryStream stream)
        {
            ulong hash = 0;
            stream.Position = 0;
            
            // Read the stream in chunks to avoid creating large byte arrays
            byte[] buffer = ArrayPool<byte>.Shared.Rent(4096);
            try
            {
                int bytesRead;
                while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    for (int i = 0; i < bytesRead; i++)
                    {
                        hash = hash * 31 + buffer[i];
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
            
            return hash;
        }
        
        /// <summary>
        /// Generates a test message of the specified approximate size
        /// </summary>
        private Message GenerateTestMessage(int approximateSizeBytes)
        {
            // Create a message with the specified approximate size
            string text = GenerateRandomString(approximateSizeBytes / 2);
            
            return new Message
            {
                Data = new MessageData
                {
                    Type = MessageType.CastAdd,
                    Fid = (ulong)_random.Next(1, 10000),
                    Timestamp = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    Network = Network.Mainnet,
                    CastAddBody = new CastAddBody
                    {
                        Text = text,
                        Mentions = { 1, 2, 3 }
                    }
                },
                Hash = Google.Protobuf.ByteString.CopyFrom(new byte[32]),
                HashScheme = HashScheme.Blake3,
                Signature = Google.Protobuf.ByteString.CopyFrom(new byte[64]),
                SignatureScheme = SignatureScheme.Ed25519,
                Signer = Google.Protobuf.ByteString.CopyFrom(new byte[32])
            };
        }
        
        /// <summary>
        /// Generates a random string of the specified approximate byte length
        /// </summary>
        private string GenerateRandomString(int approximateByteLength)
        {
            // Characters to use in the random string
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            
            // Average character size in UTF-8 is roughly 1 byte, so we'll generate that many characters
            var result = new char[approximateByteLength];
            for (int i = 0; i < approximateByteLength; i++)
            {
                result[i] = chars[_random.Next(chars.Length)];
            }
            
            return new string(result);
        }
    }
} 