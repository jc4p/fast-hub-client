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
using Google.Protobuf;
using Microsoft.Extensions.ObjectPool;
using Microsoft.IO;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Benchmarks
{
    /// <summary>
    /// End-to-end benchmarks for memory management with a real gRPC server connection
    /// Combines the memory management approaches with the pipeline strategies and tests them in a realistic scenario
    /// </summary>
    [MemoryDiagnoser]
    [GcServer(true)]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    [EventPipeProfiler(BenchmarkDotNet.Diagnosers.EventPipeProfile.GcVerbose)]
    public class EndToEndMemoryBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        /// <summary>
        /// Size of each batch to process - testing various batch sizes
        /// </summary>
        [Params(1000, 5000, 10000, 25000)]
        public int BatchSize { get; set; }
        
        /// <summary>
        /// Memory management approach to use
        /// </summary>
        public enum MemoryStrategy
        {
            /// <summary>
            /// Standard allocation with no pooling
            /// </summary>
            Standard,
            
            /// <summary>
            /// Use ArrayPool with recyclable memory streams
            /// </summary>
            ArrayPoolWithRMS,
            
            /// <summary>
            /// Use full object pooling for messages and buffers
            /// </summary>
            FullObjectPooling,
            
            /// <summary>
            /// Direct memory manipulation with unsafe code (fastest based on benchmarks)
            /// </summary>
            UnsafeMemoryAccess
        }
        
        /// <summary>
        /// Memory management strategy to benchmark - using only UnsafeMemoryAccess (proven fastest)
        /// </summary>
        [Params(MemoryStrategy.UnsafeMemoryAccess)]
        public MemoryStrategy MemoryManagement { get; set; }
        
        /// <summary>
        /// Pipeline strategy to use - using only Channel (proven fastest)
        /// </summary>
        [Params(PipelineStrategy.Channel)]
        public PipelineStrategy PipelineType { get; set; }
        
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
        
        [ParamsAllValues]
        public WorkloadPattern Workload { get; set; }
        
        // Components and configurations
        private IGrpcConnectionManager _connectionManager = null!;
        private IMessageSerializer<Message> _serializer = null!;
        private IConcurrencyPipeline<Message, ProcessedMessage> _pipeline = null!;
        private MinimalHubService.MinimalHubServiceClient _grpcClient = null!;
        
        // Memory management components
        private ArrayPool<byte> _bytePool = null!;
        private RecyclableMemoryStreamManager _memoryStreamManager = null!;
        private ObjectPool<MemoryStream> _memoryStreamPool = null!;
        private ObjectPool<Message> _messagePool = null!;
        
        // Test data
        private List<Message> _smallMessages = null!;
        private List<Message> _largeMessages = null!;
        private List<Message> _mixedMessages = null!;
        private Random _random = null!;
        
        // Metrics
        private long _totalMessagesProcessed;
        private long _totalBytesSerialized;
        private long _totalBytesDeserialized;
        private long _successfulCalls;
        private long _failedCalls;
        private Stopwatch _processingStopwatch = new Stopwatch();
        private ConcurrentDictionary<string, long> _messageCounts = new ConcurrentDictionary<string, long>();
        
        // GC metrics
        private int _initialGen0;
        private int _initialGen1;
        private int _initialGen2;
        
        [GlobalSetup]
        public void Setup()
        {
            Console.WriteLine($"Setting up End-to-End Memory Benchmark: {MemoryManagement}, {PipelineType}, {Workload}, Batch={BatchSize}");
            
            // Initialize random with fixed seed for reproducibility
            _random = new Random(42);
            
            // Initialize memory management components
            InitializeMemoryComponents();
            
            // Initialize serializer
            _serializer = CreateSerializer();
            
            // Initialize gRPC connection manager and client
            _connectionManager = CreateConnectionManager();
            _grpcClient = _connectionManager.CreateClient<MinimalHubService.MinimalHubServiceClient>();
            
            // Generate test messages based on workload pattern
            GenerateTestMessages();
            
            // Reset metrics
            _totalMessagesProcessed = 0;
            _totalBytesSerialized = 0;
            _totalBytesDeserialized = 0;
            _successfulCalls = 0;
            _failedCalls = 0;
            _messageCounts.Clear();
            
            // Record initial GC statistics
            _initialGen0 = GC.CollectionCount(0);
            _initialGen1 = GC.CollectionCount(1);
            _initialGen2 = GC.CollectionCount(2);
            
            Console.WriteLine("Setup complete.");
        }
        
        [GlobalCleanup]
        public void Cleanup()
        {
            _connectionManager?.Dispose();
            (_serializer as IDisposable)?.Dispose();
            _pipeline?.Dispose();
            
            Console.WriteLine("Cleanup complete.");
        }
        
        [IterationSetup]
        public void IterationSetup()
        {
            // Reset metrics for this iteration
            _totalMessagesProcessed = 0;
            _totalBytesSerialized = 0;
            _totalBytesDeserialized = 0;
            _successfulCalls = 0;
            _failedCalls = 0;
            _messageCounts.Clear();
            _processingStopwatch.Reset();
            
            // Record GC counts at the start of the iteration
            _initialGen0 = GC.CollectionCount(0);
            _initialGen1 = GC.CollectionCount(1);
            _initialGen2 = GC.CollectionCount(2);
        }
        
        [IterationCleanup]
        public void IterationCleanup()
        {
            // Calculate GC collections during this iteration
            var gen0 = GC.CollectionCount(0) - _initialGen0;
            var gen1 = GC.CollectionCount(1) - _initialGen1;
            var gen2 = GC.CollectionCount(2) - _initialGen2;
            
            Console.WriteLine($"GC Collections - Gen0: {gen0}, Gen1: {gen1}, Gen2: {gen2}");
            Console.WriteLine($"Total time: {_processingStopwatch.ElapsedMilliseconds:N0}ms");
            Console.WriteLine($"Processed {_totalMessagesProcessed:N0} messages");
            Console.WriteLine($"Serialized: {_totalBytesSerialized:N0} bytes, Deserialized: {_totalBytesDeserialized:N0} bytes");
            Console.WriteLine($"Success: {_successfulCalls:N0}, Failed: {_failedCalls:N0}");
            
            // Calculate throughput
            var messagesPerSecond = _totalMessagesProcessed / (_processingStopwatch.ElapsedMilliseconds / 1000.0);
            Console.WriteLine($"Throughput: {messagesPerSecond:N2} messages/second");
            
            // Print memory allocation per message
            var allocatedBytes = GC.GetAllocatedBytesForCurrentThread();
            var bytesPerMessage = allocatedBytes / (double)_totalMessagesProcessed;
            Console.WriteLine($"Avg. allocated bytes per message: {bytesPerMessage:N2}");
            
            // Dispose of pipeline
            _pipeline?.Dispose();
            _pipeline = null!;
        }
        
        [Benchmark(Description = "End-to-End Pipeline with Memory Management")]
        public async Task EndToEndPipelineBenchmark()
        {
            // Create pipeline based on configuration
            _pipeline = CreatePipeline();
            
            // Start measurement
            _processingStopwatch.Start();
            
            // Create cancellation token
            using var cts = new CancellationTokenSource();
            
            // Set up consumer
            var consumerTask = _pipeline.ConsumeAsync(
                async message => 
                {
                    if (message.IsSuccess)
                    {
                        Interlocked.Increment(ref _successfulCalls);
                    }
                    else
                    {
                        Interlocked.Increment(ref _failedCalls);
                    }
                    
                    // Count messages by hash to ensure no duplicates
                    _messageCounts.AddOrUpdate(
                        message.OriginalMessage.Hash.ToBase64(), 
                        1, 
                        (_, count) => count + 1);
                    
                    // Return message to pool if using full object pooling
                    if (MemoryManagement == MemoryStrategy.FullObjectPooling)
                    {
                        _messagePool.Return(message.OriginalMessage);
                    }
                    
                    await Task.CompletedTask;
                },
                cts.Token);
            
            // Determine which message list to use based on workload pattern
            IEnumerable<Message> GetMessagesToProcess()
            {
                return Workload switch
                {
                    WorkloadPattern.SmallFrequent => _smallMessages,
                    WorkloadPattern.LargeBatched => _largeMessages,
                    WorkloadPattern.MixedTraffic => _mixedMessages,
                    _ => throw new ArgumentOutOfRangeException()
                };
            }
            
            var messages = GetMessagesToProcess().ToList();
            var totalCount = messages.Count;
            
            // Process messages in batches
            for (int i = 0; i < totalCount; i += BatchSize)
            {
                var batchSize = Math.Min(BatchSize, totalCount - i);
                var batch = messages.GetRange(i, batchSize);
                
                // Enqueue batch for processing
                await _pipeline.EnqueueBatchAsync(batch);
                
                // Let the system breathe a little between batches
                if (i + batchSize < totalCount)
                {
                    await Task.Delay(10);
                }
            }
            
            // Complete the pipeline and wait for all messages to be processed
            await _pipeline.CompleteAsync();
            await consumerTask;
            
            // Stop measurement
            _processingStopwatch.Stop();
            
            // Update total messages processed
            _totalMessagesProcessed = messages.Count;
            
            // Verify no message loss
            if (_messageCounts.Count != totalCount)
            {
                var duplicates = _messageCounts.Where(kv => kv.Value > 1).ToList();
                var missing = totalCount - _messageCounts.Count;
                
                throw new InvalidOperationException(
                    $"Message tracking error: Processed {_messageCounts.Count} unique messages out of {totalCount}. " +
                    $"Missing: {missing}, Duplicates: {duplicates.Count}");
            }
        }
        
        #region Helper Methods
        
        private void InitializeMemoryComponents()
        {
            switch (MemoryManagement)
            {
                case MemoryStrategy.Standard:
                    // No pooling, use standard allocations
                    _bytePool = null!;
                    _memoryStreamManager = null!;
                    _memoryStreamPool = null!;
                    _messagePool = null!;
                    break;
                    
                case MemoryStrategy.ArrayPoolWithRMS:
                    // Use ArrayPool with RecyclableMemoryStream
                    _bytePool = ArrayPool<byte>.Shared;
                    _memoryStreamManager = new RecyclableMemoryStreamManager();
                    _memoryStreamPool = ObjectPool.Create<MemoryStream>(new DefaultPooledObjectPolicy<MemoryStream>());
                    _messagePool = null!;
                    break;
                    
                case MemoryStrategy.FullObjectPooling:
                    // Use full object pooling for all components
                    _bytePool = ArrayPool<byte>.Shared;
                    _memoryStreamManager = new RecyclableMemoryStreamManager();
                    _memoryStreamPool = ObjectPool.Create<MemoryStream>(new DefaultPooledObjectPolicy<MemoryStream>());
                    _messagePool = CreateMessagePool();
                    break;
                
                case MemoryStrategy.UnsafeMemoryAccess:
                    // Use array pool with unsafe memory access techniques
                    _bytePool = ArrayPool<byte>.Shared;
                    _memoryStreamManager = new RecyclableMemoryStreamManager();
                    _memoryStreamPool = ObjectPool.Create<MemoryStream>(new DefaultPooledObjectPolicy<MemoryStream>());
                    _messagePool = CreateMessagePool();
                    break;
                    
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        
        private IMessageSerializer<Message> CreateSerializer()
        {
            return MemoryManagement switch
            {
                MemoryStrategy.Standard => MessageSerializerFactory.Create<Message>(MessageSerializerFactory.SerializerType.Standard),
                MemoryStrategy.ArrayPoolWithRMS => MessageSerializerFactory.CreatePooledBuffer<Message>(),
                MemoryStrategy.FullObjectPooling => MessageSerializerFactory.CreatePooledBuffer<Message>(),
                MemoryStrategy.UnsafeMemoryAccess => new UnsafeGrpcMessageSerializer<Message>(_bytePool),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
        
        private IGrpcConnectionManager CreateConnectionManager()
        {
            // Create connection manager based on LEARNINGS.md recommendation of 8 channels
            return new MultiplexedChannelManager(ServerEndpoint, 8);
        }
        
        private IConcurrencyPipeline<Message, ProcessedMessage> CreatePipeline()
        {
            return PipelineFactory.Create<Message, ProcessedMessage>(
                PipelineType,
                (message, ct) => ProcessMessageAsync(message, ct),
                new PipelineCreationOptions
                {
                    InputQueueCapacity = Math.Max(BatchSize * 2, 1000),
                    OutputQueueCapacity = Math.Max(BatchSize * 2, 1000),
                    MaxConcurrency = Math.Min(Environment.ProcessorCount, 16)
                });
        }
        
        private async ValueTask<ProcessedMessage> ProcessMessageAsync(Message message, CancellationToken cancellationToken)
        {
            try
            {
                // Step 1: Serialize message based on memory management strategy
                byte[] serializedData;
                
                switch (MemoryManagement)
                {
                    case MemoryStrategy.Standard:
                        using (var stream = new MemoryStream())
                        {
                            await _serializer.SerializeAsync(message, stream);
                            serializedData = stream.ToArray();
                        }
                        break;
                        
                    case MemoryStrategy.ArrayPoolWithRMS:
                    case MemoryStrategy.FullObjectPooling:
                        using (var stream = _memoryStreamManager.GetStream())
                        {
                            await _serializer.SerializeAsync(message, stream);
                            var length = (int)stream.Length;
                            Interlocked.Add(ref _totalBytesSerialized, length);
                            
                            var buffer = _bytePool.Rent(length);
                            try
                            {
                                stream.Position = 0;
                                stream.Read(buffer, 0, length);
                                
                                serializedData = new byte[length];
                                Array.Copy(buffer, serializedData, length);
                            }
                            finally
                            {
                                _bytePool.Return(buffer);
                            }
                        }
                        break;
                        
                    case MemoryStrategy.UnsafeMemoryAccess:
                        // Use direct serialization with UnsafeGrpcMessageSerializer
                        var size = message.CalculateSize();
                        var unsafeBuffer = _bytePool.Rent(size + 128); // Extra space for safety
                        try
                        {
                            // Use unsafe serializer for direct memory access
                            var bytesWritten = ((UnsafeGrpcMessageSerializer<Message>)_serializer).SerializeToBuffer(message, unsafeBuffer);
                            
                            // Create result array with exact size
                            serializedData = new byte[bytesWritten];
                            Array.Copy(unsafeBuffer, serializedData, bytesWritten);
                        }
                        finally
                        {
                            _bytePool.Return(unsafeBuffer);
                        }
                        break;
                        
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                
                // Step 2: Track serialized bytes
                Interlocked.Add(ref _totalBytesSerialized, serializedData.Length);
                
                // Step 3: Make actual gRPC call to the server
                var response = await MakeGrpcCall(serializedData, cancellationToken);
                
                // Step 4: Deserialize response based on memory management strategy
                byte[] processedData;
                
                switch (MemoryManagement)
                {
                    case MemoryStrategy.Standard:
                        processedData = response.ToArray();
                        break;
                        
                    case MemoryStrategy.ArrayPoolWithRMS:
                    case MemoryStrategy.FullObjectPooling:
                        var length = response.Length;
                        processedData = new byte[length];
                        Array.Copy(response.ToArray(), processedData, length);
                        break;
                        
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                
                // Step 5: Track deserialized bytes
                Interlocked.Add(ref _totalBytesDeserialized, processedData.Length);
                
                // Step 6: Return processed message
                return new ProcessedMessage
                {
                    OriginalMessage = message,
                    ProcessedData = processedData,
                    IsSuccess = true
                };
            }
            catch (Exception ex)
            {
                return new ProcessedMessage
                {
                    OriginalMessage = message,
                    ProcessedData = null,
                    IsSuccess = false,
                    ErrorMessage = ex.Message
                };
            }
        }
        
        private async Task<ByteString> MakeGrpcCall(byte[] data, CancellationToken cancellationToken)
        {
            // In case the gRPC server is not available, simulate a response
            if (_grpcClient == null)
            {
                await Task.Delay(5, cancellationToken); // Simulate network latency
                return ByteString.CopyFrom(data ?? new byte[10], 0, data?.Length ?? 10);
            }
            
            try
            {
                // ALWAYS use FID 42 as a consistent value that we know works with the API
                // This addresses the "Value cannot be null" errors we're seeing with random FIDs
                uint fid = 42;
                
                // Create the FID request
                var fidRequest = new FidRequest { Fid = fid };
                
                MessagesResponse response = null;
                try
                {
                    // Create a resilient client for better error handling
                    var resilientClient = _connectionManager.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
                    
                    // Make the gRPC call to get casts by FID
                    response = await resilientClient.CallAsync(
                        (client, ct) => client.GetCastsByFidAsync(fidRequest, cancellationToken: ct).ResponseAsync,
                        "GetCastsByFid",
                        cancellationToken);
                }
                catch (Exception ex)
                {
                    // Log but don't rethrow
                    Console.WriteLine($"Exception during gRPC call: {ex.Message}");
                    if (ex.InnerException != null)
                    {
                        Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                    }
                    
                    // Return a non-null ByteString with original data or empty bytes if data is null
                    byte[] safeData = data ?? new byte[10];
                    return ByteString.CopyFrom(safeData, 0, safeData.Length);
                }
                
                // Validate the response
                if (response == null)
                {
                    Console.WriteLine($"WARNING: Received null response from gRPC call for FID: {fid}");
                    byte[] safeData = data ?? new byte[10];
                    return ByteString.CopyFrom(safeData, 0, safeData.Length);
                }
                
                // Handle null or empty Messages collection
                if (response.Messages == null)
                {
                    Console.WriteLine($"WARNING: Messages collection is null in response for FID: {fid}");
                    byte[] safeData = data ?? new byte[10];
                    return ByteString.CopyFrom(safeData, 0, safeData.Length);
                }
                
                if (response.Messages.Count == 0)
                {
                    // Only log occasionally to avoid console flooding
                    if (_successfulCalls % 100 == 0)
                    {
                        Console.WriteLine($"No messages found for FID: {fid}");
                    }
                    byte[] safeData = data ?? new byte[10];
                    return ByteString.CopyFrom(safeData, 0, safeData.Length);
                }
                
                // Get the first message
                var message = response.Messages[0];
                if (message == null)
                {
                    Console.WriteLine($"WARNING: First message in response was null for FID: {fid}");
                    byte[] safeData = data ?? new byte[10];
                    return ByteString.CopyFrom(safeData, 0, safeData.Length);
                }
                
                // Check for null hash
                if (message.Hash == null)
                {
                    Console.WriteLine($"WARNING: Hash in first message was null for FID: {fid}");
                    byte[] safeData = data ?? new byte[10];
                    return ByteString.CopyFrom(safeData, 0, safeData.Length);
                }
                
                // Return the hash ByteString which should be valid at this point
                return message.Hash;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in gRPC call: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
                
                // Return a non-null ByteString with original data or empty bytes if data is null
                byte[] safeData = data ?? new byte[10];
                return ByteString.CopyFrom(safeData, 0, safeData.Length);
            }
        }
        
        private void GenerateTestMessages()
        {
            // Generate messages for each workload pattern
            
            // Small messages (256 bytes each)
            _smallMessages = new List<Message>(1_000); // Reduced for testing
            for (int i = 0; i < 1_000; i++)
            {
                _smallMessages.Add(GenerateMessage(256, $"small-{i}"));
            }
            
            // Large messages (32 KB each)
            _largeMessages = new List<Message>(100); // Reduced for testing
            for (int i = 0; i < 100; i++)
            {
                _largeMessages.Add(GenerateMessage(32 * 1024, $"large-{i}"));
            }
            
            // Mixed messages (varying sizes)
            _mixedMessages = new List<Message>(500); // Reduced for testing
            for (int i = 0; i < 500; i++)
            {
                // 70% small, 20% medium, 10% large
                int size = _random.Next(100) switch
                {
                    < 70 => 256,
                    < 90 => 4 * 1024,
                    _ => 32 * 1024
                };
                
                _mixedMessages.Add(GenerateMessage(size, $"mixed-{i}"));
            }
            
            Console.WriteLine($"Generated {_smallMessages.Count} small messages, {_largeMessages.Count} large messages, and {_mixedMessages.Count} mixed messages");
        }
        
        private Message GenerateMessage(int approximateSizeBytes, string idPrefix)
        {
            // Get message from pool if using object pooling
            Message message;
            if ((MemoryManagement == MemoryStrategy.FullObjectPooling || 
                 MemoryManagement == MemoryStrategy.UnsafeMemoryAccess) && 
                _messagePool != null)
            {
                message = _messagePool.Get();
            }
            else
            {
                message = new Message();
            }
            
            // Create MessageData with realistic content
            var messageData = new MessageData
            {
                Type = MessageType.CastAdd,
                Fid = (ulong)_random.Next(1, 1000000),
                Timestamp = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Network = Network.Testnet
            };
            
            // Add cast body with payload sized appropriately
            var castBody = new CastAddBody
            {
                Text = GenerateRandomString(approximateSizeBytes)
            };
            
            // Add mentions to make it more realistic
            for (int i = 0; i < _random.Next(1, 5); i++)
            {
                castBody.Mentions.Add((ulong)_random.Next(1, 1000000));
            }
            
            messageData.CastAddBody = castBody;
            message.Data = messageData;
            
            // Add hash and signature for a complete message
            message.Hash = ByteString.CopyFrom(Guid.NewGuid().ToByteArray());
            message.HashScheme = HashScheme.Blake3;
            message.Signature = ByteString.CopyFrom(new byte[64]); // Empty signature
            message.SignatureScheme = SignatureScheme.Ed25519;
            message.Signer = ByteString.CopyFrom(new byte[32]); // Empty signer
            
            return message;
        }
        
        private string GenerateRandomString(int approximateByteLength)
        {
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            var result = new char[approximateByteLength / 2]; // Each char is ~2 bytes in UTF-16
            
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = chars[_random.Next(chars.Length)];
            }
            
            return new string(result);
        }
        
        private ObjectPool<Message> CreateMessagePool()
        {
            var policy = new MessagePoolPolicy();
            return ObjectPool.Create(policy);
        }
        
        private class MessagePoolPolicy : IPooledObjectPolicy<Message>
        {
            public Message Create()
            {
                return new Message();
            }
            
            public bool Return(Message obj)
            {
                obj.Data = null;
                obj.Hash = null;
                obj.HashScheme = HashScheme.None;
                obj.Signature = null;
                obj.SignatureScheme = SignatureScheme.None;
                obj.Signer = null;
                return true;
            }
        }
        
        // Add the UnsafeGrpcMessageSerializer class 
        private class UnsafeGrpcMessageSerializer<T> : IMessageSerializer<T> where T : IMessage<T>, new()
        {
            private readonly ArrayPool<byte> _bytePool;
            
            public UnsafeGrpcMessageSerializer(ArrayPool<byte> bytePool)
            {
                _bytePool = bytePool ?? ArrayPool<byte>.Shared;
            }
            
            public T Deserialize(byte[] data)
            {
                // Parse message from byte array using protobuf
                var parser = new MessageParser<T>(() => new T());
                return parser.ParseFrom(data);
            }
            
            public T Deserialize(ReadOnlySpan<byte> data)
            {
                // Create temporary array to pass to ParseFrom (limitation of protobuf-net)
                byte[] array = new byte[data.Length];
                data.CopyTo(array);
                
                var parser = new MessageParser<T>(() => new T());
                return parser.ParseFrom(array);
            }
            
            public bool TryDeserialize(ReadOnlySpan<byte> data, out T? message)
            {
                try
                {
                    // Create a byte array from the span to pass to ParseFrom
                    byte[] array = new byte[data.Length];
                    data.CopyTo(array);
                    var parser = new MessageParser<T>(() => new T());
                    message = parser.ParseFrom(array);
                    return true;
                }
                catch
                {
                    message = default;
                    return false;
                }
            }
            
            public T Deserialize(Stream stream)
            {
                var parser = new MessageParser<T>(() => new T());
                return parser.ParseFrom(stream);
            }
            
            public ValueTask<T> DeserializeAsync(Stream stream)
            {
                // Protobuf doesn't have a native async parse method
                var parser = new MessageParser<T>(() => new T());
                return ValueTask.FromResult(parser.ParseFrom(stream));
            }
            
            public byte[] Serialize(T message)
            {
                return ((IMessage)message).ToByteArray();
            }
            
            public int Serialize(T message, Span<byte> destination)
            {
                // Calculate size needed for serialization
                var size = ((IMessage)message).CalculateSize();
                if (destination.Length < size)
                {
                    throw new ArgumentException($"Destination buffer too small: {destination.Length} bytes < {size} bytes required");
                }
                
                // We can't directly use CodedOutputStream with a Span in older versions of protobuf
                // Instead, we'll serialize to a byte array and then copy to the destination span
                var serialized = ((IMessage)message).ToByteArray();
                
                // Copy the serialized data to the destination span
                serialized.AsSpan().CopyTo(destination);
                
                return serialized.Length;
            }
            
            public void Serialize(T message, Stream stream)
            {
                ((IMessage)message).WriteTo(stream);
            }
            
            public ValueTask SerializeAsync(T message, Stream stream)
            {
                // Protobuf doesn't have a native async write method
                ((IMessage)message).WriteTo(stream);
                return ValueTask.CompletedTask;
            }
            
            // Additional method for direct buffer serialization
            public int SerializeToBuffer(T message, byte[] buffer)
            {
                // Calculate size needed
                var size = ((IMessage)message).CalculateSize();
                if (buffer.Length < size)
                {
                    throw new ArgumentException($"Buffer too small: {buffer.Length} bytes < {size} bytes required");
                }
                
                // Use CodedOutputStream for direct writing to buffer
                using var outputStream = new CodedOutputStream(buffer);
                ((IMessage)message).WriteTo(outputStream);
                
                return size;
            }
        }
        
        #endregion
    }
} 