using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Google.Protobuf;
using HubClient.Core;
using HubClient.Core.Grpc;
using HubClient.Core.Serialization;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace HubClient.Benchmarks
{
    /// <summary>
    /// Benchmark for measuring performance of different serialization approaches.
    /// Optimized for testing with high-volume historical data (10M+ messages per month).
    /// 
    /// EXECUTING BENCHMARKS:
    /// 
    /// 1. For ALL serializers with STANDARD message counts:
    ///    dotnet run --project HubClient.Benchmarks --configuration Release -- --filter * --category Standard --job short
    /// 
    /// 2. For EXTREME message counts (50k-5M):
    ///    dotnet run --project HubClient.Benchmarks --configuration Release -- --filter * --category Extreme --job short
    /// 
    /// 3. For ADVANCED throughput benchmarks (optimized for 500M+ records):
    ///    dotnet run --project HubClient.Benchmarks --configuration Release -- --filter * --category AdvancedThroughput --job short
    ///
    /// 4. For comparing specific approaches at EXTREME scale:
    ///    - Stream Pipeline:
    ///      dotnet run --project HubClient.Benchmarks --configuration Release -- --filter *StreamPipeline_WithBufferReuse* --job short
    ///    - Memory-Mapped File:
    ///      dotnet run --project HubClient.Benchmarks --configuration Release -- --filter *MemoryMappedFile_Throughput* --job short
    ///    - Concurrent Channels:
    ///      dotnet run --project HubClient.Benchmarks --configuration Release -- --filter *ChannelPipeline_Concurrent* --job short
    ///
    /// 5. For memory-constrained environments, limit the message count:
    ///    dotnet run --project HubClient.Benchmarks --configuration Release -- --filter *Throughput* --job short -- --messageCount 100000
    ///
    /// </summary>
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [GcServer(true)]
    [RankColumn]
    [EventPipeProfiler(BenchmarkDotNet.Diagnosers.EventPipeProfile.GcVerbose)]
    public class SerializationBenchmarks
    {
        // Server endpoint for the gRPC connection
        private const string ServerEndpoint = "http://localhost:5293";
        
        // Message sizes to benchmark - focusing on large-scale historical data processing
        // For a server with 10M messages/month, potentially years of data
        // [Params(10000, 50000, 100000, 1000000, 5000000)]
        [Params(500000)]
        public int MessageCount { get; set; }
        
        // Variables for holding test data
        private List<Message> _realMessages = null!;
        private Message _testMessage = null!;
        private byte[] _serializedMessage = null!;
        private byte[] _tempBuffer = null!;
        private MemoryStream _memoryStream = null!;
        private Random _random = null!;
        
        // MultiplexedChannelManager for the gRPC connection
        private MultiplexedChannelManager _channelManager = null!;
        private MinimalHubService.MinimalHubServiceClient _grpcClient = null!;
        
        // Serializer instances
        private IMessageSerializer<Message> _standardSerializer = null!;
        private IMessageSerializer<Message> _pooledBufferSerializer = null!;
        private IMessageSerializer<Message> _spanBasedSerializer = null!;
        private IMessageSerializer<Message> _pooledMessageSerializer = null!;
        
        [GlobalSetup]
        public async Task Setup()
        {
            // Setup the gRPC connection
            _random = new Random(42); // Use fixed seed for reproducibility
            
            // Create MultiplexedChannelManager with dynamic configuration based on message count
            int channelCount = Math.Max(8, MessageCount / 500000); // More channels for very large tests
            int maxConcurrentCallsPerChannel = Math.Min(100, MessageCount / 50000); // More concurrent calls for larger tests
            
            // int channelCount = 8;
            // int maxConcurrentCallsPerChannel = 100;

            Console.WriteLine($"Using {channelCount} channels with {maxConcurrentCallsPerChannel} concurrent calls per channel");
            
            _channelManager = new MultiplexedChannelManager(ServerEndpoint, channelCount, maxConcurrentCallsPerChannel);
            
            // Create gRPC client
            _grpcClient = _channelManager.CreateClient<MinimalHubService.MinimalHubServiceClient>();
            
            // For extreme message counts, limit actual data size for benchmarking purposes
            // This prevents excessive memory usage while still testing the serialization performance
            int actualDataSize = Math.Min(MessageCount, 100000); // Cap at 100K actual messages for memory reasons
            
            Console.WriteLine($"MessageCount parameter: {MessageCount}");
            Console.WriteLine($"Fetching {actualDataSize} unique messages for serialization testing...");
            
            // Initialize message lists with capacity to avoid reallocations
            _realMessages = new List<Message>(actualDataSize);
            
            try
            {
                // Create a cache of FIDs that have messages to avoid repeated failures
                HashSet<ulong> fidsWithMessages = new HashSet<ulong>();
                HashSet<ulong> fidsWithoutMessages = new HashSet<ulong>();
                int fetchAttempts = 0;
                
                // First pass: Try to find FIDs with lots of messages
                Console.WriteLine("Sampling FIDs for high message volume...");
                
                // Use parallel tasks to search for productive FIDs
                var fidSamplingTasks = new List<Task>();
                for (int i = 0; i < 20; i++) // Try 20 parallel FID samples
                {
                    fidSamplingTasks.Add(Task.Run(async () =>
                    {
                        for (int j = 0; j < 50 && fidsWithMessages.Count < 20; j++)
                        {
                            ulong randomFid = (ulong)_random.Next(1, 100001); // Expanded FID range
                            
                            // Skip if we already know about this FID
                            lock (fidsWithoutMessages)
                            {
                                if (fidsWithoutMessages.Contains(randomFid)) continue;
                            }
                            
                            lock (fidsWithMessages)
                            {
                                if (fidsWithMessages.Contains(randomFid)) continue;
                            }
                            
                            var request = new FidRequest
                            {
                                Fid = randomFid,
                                PageSize = 200 // Larger page size for sampling
                            };
                            
                            try
                            {
                                var response = await _grpcClient.GetCastsByFidAsync(request);
                                Interlocked.Increment(ref fetchAttempts);
                                
                                if (response.Messages.Count > 50) // Only keep FIDs with lots of messages
                                {
                                    lock (fidsWithMessages)
                                    {
                                        fidsWithMessages.Add(randomFid);
                                        Console.WriteLine($"Found productive FID {randomFid} with {response.Messages.Count} messages");
                                    }
                                    
                                    lock (_realMessages)
                                    {
                                        if (_realMessages.Count < actualDataSize)
                                        {
                                            _realMessages.AddRange(response.Messages.Take(
                                                Math.Min(response.Messages.Count, actualDataSize - _realMessages.Count)));
                                        }
                                    }
                                }
                                else
                                {
                                    lock (fidsWithoutMessages)
                                    {
                                        fidsWithoutMessages.Add(randomFid);
                                    }
                                }
                            }
                            catch
                            {
                                lock (fidsWithoutMessages)
                                {
                                    fidsWithoutMessages.Add(randomFid);
                                }
                            }
                        }
                    }));
                }
                
                // Wait for all FID sampling to complete
                await Task.WhenAll(fidSamplingTasks);
                
                Console.WriteLine($"Found {fidsWithMessages.Count} productive FIDs after {fetchAttempts} fetch attempts");
                Console.WriteLine($"Current message count: {_realMessages.Count}/{actualDataSize}");
                
                // If we found some productive FIDs, use them to gather all the messages we need
                if (fidsWithMessages.Count > 0)
                {
                    Console.WriteLine("Fetching additional messages from productive FIDs...");
                    
                    // Calculate how many more messages we need
                    int remainingMessages = actualDataSize - _realMessages.Count;
                    
                    if (remainingMessages > 0)
                    {
                        // Tasks to run in parallel
                        List<Task> parallelFetchTasks = new List<Task>();
                        
                        // Distribute the work across productive FIDs
                        foreach (ulong fid in fidsWithMessages)
                        {
                            int messagesPerFid = (remainingMessages / fidsWithMessages.Count) + 1;
                            
                            parallelFetchTasks.Add(Task.Run(async () =>
                            {
                                string? pageToken = null;
                                bool hasMoreMessages = true;
                                int messagesFetched = 0;
                                
                                while (hasMoreMessages && messagesFetched < messagesPerFid)
                                {
                                    // Check if we've reached our total target
                                    lock (_realMessages)
                                    {
                                        if (_realMessages.Count >= actualDataSize)
                                            break;
                                    }
                                    
                                    try
                                    {
                                        var request = new FidRequest
                                        {
                                            Fid = fid,
                                            PageSize = 1000, // Fetch large pages
                                            PageToken = pageToken != null ? ByteString.CopyFromUtf8(pageToken) : null
                                        };
                                        
                                        var response = await _grpcClient.GetCastsByFidAsync(request);
                                        
                                        if (response.Messages.Count == 0)
                                        {
                                            hasMoreMessages = false;
                                            break;
                                        }
                                        
                                        lock (_realMessages)
                                        {
                                            // Only add what we need to reach our target
                                            int toAdd = Math.Min(response.Messages.Count, actualDataSize - _realMessages.Count);
                                            if (toAdd > 0)
                                            {
                                                _realMessages.AddRange(response.Messages.Take(toAdd));
                                                messagesFetched += toAdd;
                                            }
                                            
                                            // Break if we've reached our goal
                                            if (_realMessages.Count >= actualDataSize)
                                                break;
                                        }
                                        
                                        pageToken = response.NextPageToken?.ToStringUtf8();
                                        if (string.IsNullOrEmpty(pageToken))
                                        {
                                            hasMoreMessages = false;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Error fetching messages from FID {fid}: {ex.Message}");
                                        break;
                                    }
                                }
                            }));
                        }
                        
                        // Process in batches to avoid overwhelming the server
                        const int maxConcurrentTasks = 10;
                        for (int i = 0; i < parallelFetchTasks.Count; i += maxConcurrentTasks)
                        {
                            int tasksToRun = Math.Min(maxConcurrentTasks, parallelFetchTasks.Count - i);
                            await Task.WhenAll(parallelFetchTasks.Skip(i).Take(tasksToRun));
                            
                            Console.WriteLine($"Progress: {_realMessages.Count}/{actualDataSize} messages fetched");
                            
                            // Break early if we have enough messages
                            if (_realMessages.Count >= actualDataSize)
                                break;
                        }
                    }
                }
                
                Console.WriteLine($"Successfully fetched {_realMessages.Count} real messages from the server");
                
                // If we couldn't get enough real messages, duplicate the ones we have to reach required count
                if (_realMessages.Count > 0 && _realMessages.Count < actualDataSize)
                {
                    Console.WriteLine($"Duplicating messages to reach target count of {actualDataSize}...");
                    
                    int originalCount = _realMessages.Count;
                    List<Message> templateMessages = new List<Message>(_realMessages); // Create a copy as template
                    
                    while (_realMessages.Count < actualDataSize)
                    {
                        int remaining = actualDataSize - _realMessages.Count;
                        int batchSize = Math.Min(originalCount, remaining);
                        
                        // Add copies of messages
                        for (int i = 0; i < batchSize; i++)
                        {
                            // Create a new copy of the message to avoid reference issues
                            Message originalMsg = templateMessages[i % templateMessages.Count];
                            Message newMsg = new Message
                            {
                                Data = originalMsg.Data.Clone(),
                                Hash = originalMsg.Hash,
                                HashScheme = originalMsg.HashScheme,
                                Signature = originalMsg.Signature,
                                SignatureScheme = originalMsg.SignatureScheme,
                                Signer = originalMsg.Signer
                            };
                            
                            _realMessages.Add(newMsg);
                        }
                    }
                    
                    Console.WriteLine($"Expanded to {_realMessages.Count} messages total");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to connect to gRPC server: {ex.Message}");
                Console.WriteLine("Falling back to synthetic test data");
                
                // Generate synthetic test data
                GenerateSyntheticTestData(actualDataSize);
            }
            
            // If we still have no messages, create synthetic ones
            if (_realMessages.Count == 0)
            {
                Console.WriteLine("No messages fetched. Generating synthetic test data...");
                GenerateSyntheticTestData(actualDataSize);
            }
            
            Console.WriteLine($"Final dataset: {_realMessages.Count} messages");
            
            // For benchmark purposes, we'll simulate the full MessageCount by recycling our fetched messages
            Console.WriteLine($"Actual benchmark will process {MessageCount} messages by reusing the fetched data");
            
            // Select one test message for the single-message benchmarks
            _testMessage = _realMessages.FirstOrDefault() ?? GenerateTestMessage(1000);
            
            // Initialize serializers
            _standardSerializer = MessageSerializerFactory.CreateStandard<Message>();
            _pooledBufferSerializer = MessageSerializerFactory.CreatePooledBuffer<Message>();
            _spanBasedSerializer = MessageSerializerFactory.CreateSpanBased<Message>();
            _pooledMessageSerializer = MessageSerializerFactory.CreatePooledMessage<Message>();
            
            // Pre-serialize the message for deserialization benchmarks
            _serializedMessage = _standardSerializer.Serialize(_testMessage);
            
            // Allocate a large enough buffer for all operations
            _tempBuffer = new byte[Math.Max(_serializedMessage.Length * 2, 1024 * 1024)];
            
            // Create memory stream for stream-based operations
            _memoryStream = new MemoryStream(_tempBuffer);
            
            // Force GC to clean up before benchmarks start
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
        
        [GlobalCleanup]
        public void Cleanup()
        {
            _memoryStream.Dispose();
            _channelManager.Dispose();
        }
        
        [IterationSetup]
        public void IterationSetup()
        {
            // Reset the memory stream position
            _memoryStream.Position = 0;
            _memoryStream.SetLength(0);
        }
        
        #region Serialization Benchmarks
        
        [Benchmark(Baseline = true)]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public byte[] Serialize_Standard()
        {
            return _standardSerializer.Serialize(_testMessage);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public byte[] Serialize_PooledBuffer()
        {
            return _pooledBufferSerializer.Serialize(_testMessage);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public byte[] Serialize_SpanBased()
        {
            return _spanBasedSerializer.Serialize(_testMessage);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public byte[] Serialize_PooledMessage()
        {
            return _pooledMessageSerializer.Serialize(_testMessage);
        }
        
        #endregion
        
        #region Deserialization Benchmarks
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public Message Deserialize_Standard()
        {
            return _standardSerializer.Deserialize(_serializedMessage);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public Message Deserialize_PooledBuffer()
        {
            return _pooledBufferSerializer.Deserialize(_serializedMessage);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public Message Deserialize_SpanBased()
        {
            return _spanBasedSerializer.Deserialize(_serializedMessage);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public Message Deserialize_PooledMessage()
        {
            return _pooledMessageSerializer.Deserialize(_serializedMessage);
        }
        
        #endregion
        
        #region Buffer-based Benchmarks
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public int SerializeToBuffer_Standard()
        {
            return _standardSerializer.Serialize(_testMessage, _tempBuffer);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public int SerializeToBuffer_PooledBuffer()
        {
            return _pooledBufferSerializer.Serialize(_testMessage, _tempBuffer);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public int SerializeToBuffer_SpanBased()
        {
            return _spanBasedSerializer.Serialize(_testMessage, _tempBuffer);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public int SerializeToBuffer_PooledMessage()
        {
            return _pooledMessageSerializer.Serialize(_testMessage, _tempBuffer);
        }
        
        #endregion
        
        #region Stream-based Benchmarks
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public void SerializeToStream_Standard()
        {
            _memoryStream.Position = 0;
            _standardSerializer.Serialize(_testMessage, _memoryStream);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public void SerializeToStream_PooledBuffer()
        {
            _memoryStream.Position = 0;
            _pooledBufferSerializer.Serialize(_testMessage, _memoryStream);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public void SerializeToStream_SpanBased()
        {
            _memoryStream.Position = 0;
            _spanBasedSerializer.Serialize(_testMessage, _memoryStream);
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "SingleOperation")]
        public void SerializeToStream_PooledMessage()
        {
            _memoryStream.Position = 0;
            _pooledMessageSerializer.Serialize(_testMessage, _memoryStream);
        }
        
        #endregion
        
        #region High Volume Throughput
        
        [Benchmark]
        [BenchmarkCategory("Standard", "Throughput")]
        public void Throughput_Standard()
        {
            // Use the actual requested message count for throughput testing
            var messagesToProcess = _realMessages.Take(MessageCount).ToList();
            
            // For large message counts, process in batches to avoid excessive memory pressure
            if (MessageCount > 10000)
            {
                const int batchSize = 1000;
                for (int i = 0; i < messagesToProcess.Count; i += batchSize)
                {
                    int currentBatchSize = Math.Min(batchSize, messagesToProcess.Count - i);
                    
                    for (int j = 0; j < currentBatchSize; j++)
                    {
                        var message = messagesToProcess[i + j];
                        byte[] data = _standardSerializer.Serialize(message);
                        Message deserializedMessage = _standardSerializer.Deserialize(data);
                    }
                    
                    // Force garbage collection after each batch for extremely large tests
                    if (MessageCount >= 50000 && i % 10000 == 0 && i > 0)
                    {
                        GC.Collect();
                    }
                }
            }
            else
            {
                // Original implementation for smaller counts
                foreach (var message in messagesToProcess)
                {
                    byte[] data = _standardSerializer.Serialize(message);
                    Message deserializedMessage = _standardSerializer.Deserialize(data);
                }
            }
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "Throughput")]
        public void Throughput_PooledBuffer()
        {
            var messagesToProcess = _realMessages.Take(MessageCount).ToList();
            
            // For large message counts, process in batches to avoid excessive memory pressure
            if (MessageCount > 10000)
            {
                const int batchSize = 1000;
                for (int i = 0; i < messagesToProcess.Count; i += batchSize)
                {
                    int currentBatchSize = Math.Min(batchSize, messagesToProcess.Count - i);
                    
                    for (int j = 0; j < currentBatchSize; j++)
                    {
                        var message = messagesToProcess[i + j];
                        byte[] data = _pooledBufferSerializer.Serialize(message);
                        Message deserializedMessage = _pooledBufferSerializer.Deserialize(data);
                    }
                    
                    // Force garbage collection after each batch for extremely large tests
                    if (MessageCount >= 50000 && i % 10000 == 0 && i > 0)
                    {
                        GC.Collect();
                    }
                }
            }
            else
            {
                // Original implementation for smaller counts
                foreach (var message in messagesToProcess)
                {
                    byte[] data = _pooledBufferSerializer.Serialize(message);
                    Message deserializedMessage = _pooledBufferSerializer.Deserialize(data);
                }
            }
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "Throughput")]
        public void Throughput_SpanBased()
        {
            var messagesToProcess = _realMessages.Take(MessageCount).ToList();
            
            // For large message counts, process in batches to avoid excessive memory pressure
            if (MessageCount > 10000)
            {
                const int batchSize = 1000;
                for (int i = 0; i < messagesToProcess.Count; i += batchSize)
                {
                    int currentBatchSize = Math.Min(batchSize, messagesToProcess.Count - i);
                    
                    for (int j = 0; j < currentBatchSize; j++)
                    {
                        var message = messagesToProcess[i + j];
                        byte[] data = _spanBasedSerializer.Serialize(message);
                        Message deserializedMessage = _spanBasedSerializer.Deserialize(data);
                    }
                    
                    // Force garbage collection after each batch for extremely large tests
                    if (MessageCount >= 50000 && i % 10000 == 0 && i > 0)
                    {
                        GC.Collect();
                    }
                }
            }
            else
            {
                // Original implementation for smaller counts
                foreach (var message in messagesToProcess)
                {
                    byte[] data = _spanBasedSerializer.Serialize(message);
                    Message deserializedMessage = _spanBasedSerializer.Deserialize(data);
                }
            }
        }
        
        [Benchmark]
        [BenchmarkCategory("Standard", "Throughput")]
        public void Throughput_PooledMessage()
        {
            var messagesToProcess = _realMessages.Take(MessageCount).ToList();
            
            // For large message counts, process in batches to avoid excessive memory pressure
            if (MessageCount > 10000)
            {
                const int batchSize = 1000;
                for (int i = 0; i < messagesToProcess.Count; i += batchSize)
                {
                    int currentBatchSize = Math.Min(batchSize, messagesToProcess.Count - i);
                    
                    for (int j = 0; j < currentBatchSize; j++)
                    {
                        var message = messagesToProcess[i + j];
                        byte[] data = _pooledMessageSerializer.Serialize(message);
                        Message deserializedMessage = _pooledMessageSerializer.Deserialize(data);
                    }
                    
                    // Force garbage collection after each batch for extremely large tests
                    if (MessageCount >= 50000 && i % 10000 == 0 && i > 0)
                    {
                        GC.Collect();
                    }
                }
            }
            else
            {
                // Original implementation for smaller counts
                foreach (var message in messagesToProcess)
                {
                    byte[] data = _pooledMessageSerializer.Serialize(message);
                    Message deserializedMessage = _pooledMessageSerializer.Deserialize(data);
                }
            }
        }
        
        #endregion
        
        #region Optimized Throughput For Extreme Message Counts
        
        [Benchmark]
        [BenchmarkCategory("Extreme", "OptimizedThroughput")]
        public void OptimizedThroughput_SpanBased()
        {
            // For very large message counts, use even more optimized approach
            if (MessageCount >= 1000000)
            {
                // Process messages directly with streams to minimize memory usage
                ProcessMessagesWithStream(_spanBasedSerializer, MessageCount);
                return;
            }
            
            // Only run this benchmark for large message counts
            if (MessageCount < 1000)
            {
                // For small counts, just delegate to the normal benchmark
                Throughput_SpanBased();
                return;
            }
            
            var messagesToProcess = _realMessages;
            int messagesToProcessCount = messagesToProcess.Count;
            
            // Pre-allocate a single buffer for all serialization operations
            int maxMessageSize = messagesToProcess.Max(m => m.CalculateSize());
            byte[] reuseBuffer = new byte[maxMessageSize * 2]; // Extra space to be safe
            
            // Process in batches to manage memory pressure
            const int batchSize = 1000;
            
            // Determine how many messages to process
            int totalMessagesToProcess = MessageCount;
            int messagesProcessed = 0;
            
            while (messagesProcessed < totalMessagesToProcess)
            {
                int remainingToProcess = totalMessagesToProcess - messagesProcessed;
                int currentBatchSize = Math.Min(batchSize, remainingToProcess);
                
                for (int j = 0; j < currentBatchSize; j++)
                {
                    var message = messagesToProcess[j % messagesToProcessCount]; // Cycle through available messages
                    
                    // Use buffer-based serialization to avoid allocations
                    int bytesWritten = _spanBasedSerializer.Serialize(message, reuseBuffer);
                    
                    // Use span-based deserialization to avoid allocations
                    ReadOnlySpan<byte> messageSpan = new ReadOnlySpan<byte>(reuseBuffer, 0, bytesWritten);
                    Message deserializedMessage = _spanBasedSerializer.Deserialize(messageSpan);
                }
                
                messagesProcessed += currentBatchSize;
                
                // Force garbage collection periodically for extremely large tests
                if (MessageCount >= 1000000 && messagesProcessed % 100000 == 0)
                {
                    GC.Collect();
                }
            }
        }
        
        [Benchmark]
        [BenchmarkCategory("Extreme", "OptimizedThroughput")]
        public void OptimizedThroughput_PooledMessage()
        {
            // For very large message counts, use even more optimized approach
            if (MessageCount >= 1000000)
            {
                // Process messages directly with streams to minimize memory usage
                ProcessMessagesWithStream(_pooledMessageSerializer, MessageCount);
                return;
            }
            
            // Only run this benchmark for large message counts
            if (MessageCount < 1000)
            {
                // For small counts, just delegate to the normal benchmark
                Throughput_PooledMessage();
                return;
            }
            
            var messagesToProcess = _realMessages;
            int messagesToProcessCount = messagesToProcess.Count;
            
            // Pre-allocate a single buffer for all serialization operations
            int maxMessageSize = messagesToProcess.Max(m => m.CalculateSize());
            byte[] reuseBuffer = new byte[maxMessageSize * 2]; // Extra space to be safe
            
            // Process in batches to manage memory pressure
            const int batchSize = 1000;
            
            // Determine how many messages to process
            int totalMessagesToProcess = MessageCount;
            int messagesProcessed = 0;
            
            while (messagesProcessed < totalMessagesToProcess)
            {
                int remainingToProcess = totalMessagesToProcess - messagesProcessed;
                int currentBatchSize = Math.Min(batchSize, remainingToProcess);
                
                for (int j = 0; j < currentBatchSize; j++)
                {
                    var message = messagesToProcess[j % messagesToProcessCount]; // Cycle through available messages
                    
                    // Use buffer-based serialization to avoid allocations
                    int bytesWritten = _pooledMessageSerializer.Serialize(message, reuseBuffer);
                    
                    // Use span-based deserialization to avoid allocations
                    ReadOnlySpan<byte> messageSpan = new ReadOnlySpan<byte>(reuseBuffer, 0, bytesWritten);
                    Message deserializedMessage = _pooledMessageSerializer.Deserialize(messageSpan);
                }
                
                messagesProcessed += currentBatchSize;
                
                // Force garbage collection periodically for extremely large tests
                if (MessageCount >= 1000000 && messagesProcessed % 100000 == 0)
                {
                    GC.Collect();
                }
            }
        }
        
        /// <summary>
        /// Helper method to process messages with streams for ultra-large message counts
        /// </summary>
        private void ProcessMessagesWithStream<T>(IMessageSerializer<T> serializer, int messageCount) where T : class, IMessage<T>
        {
            // Get message sample
            var messagesToProcess = _realMessages;
            int messagesToProcessCount = messagesToProcess.Count;
            
            // Create a memory stream that we'll reuse
            using var memStream = new MemoryStream(1024 * 1024); // 1MB buffer
            
            // Process in larger batches for stream operations
            const int batchSize = 10000;
            
            // Keep track of progress
            int messagesProcessed = 0;
            
            while (messagesProcessed < messageCount)
            {
                int remainingToProcess = messageCount - messagesProcessed;
                int currentBatchSize = Math.Min(batchSize, remainingToProcess);
                
                for (int j = 0; j < currentBatchSize; j++)
                {
                    memStream.Position = 0;
                    memStream.SetLength(0);
                    
                    // Get a message (cycling through available ones)
                    var message = messagesToProcess[j % messagesToProcessCount] as T;
                    if (message == null) continue;
                    
                    // Serialize to stream
                    serializer.Serialize(message, memStream);
                    
                    // Rewind and deserialize
                    memStream.Position = 0;
                    T deserializedMessage = serializer.Deserialize(memStream);
                }
                
                messagesProcessed += currentBatchSize;
                
                // Force cleanup every few million messages
                if (messageCount >= 5000000 && messagesProcessed % 1000000 == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                }
            }
        }
        
        #endregion
        
        #region Extreme Scale Comparison
        
        /// <summary>
        /// A special benchmark designed to compare the serializers at extreme scale
        /// This is useful for the 50,000+ message count tests to see which approach is most efficient
        /// </summary>
        [Benchmark]
        [BenchmarkCategory("Extreme", "ScaleComparison")]
        public long ExtremeScale_Comparison()
        {
            // Only run this for large message counts
            if (MessageCount < 10000)
            {
                return 0; // Return quickly for small counts
            }
            
            // Get a subset of messages for testing to avoid excessive runtime
            int actualCount = Math.Min(MessageCount, 10000);
            var messagesToProcess = _realMessages.Take(actualCount).ToList();
            
            // Prepare buffers
            long totalBytes = 0;
            byte[] standardBuffer = new byte[8192];
            byte[] pooledBuffer = new byte[8192];
            byte[] spanBuffer = new byte[8192];
            byte[] pooledMsgBuffer = new byte[8192];
            
            // Sample message for size estimation
            int maxMessageSize = messagesToProcess.Max(m => m.CalculateSize());
            if (maxMessageSize > 8000)
            {
                // Resize buffers if needed
                standardBuffer = new byte[maxMessageSize * 2];
                pooledBuffer = new byte[maxMessageSize * 2];
                spanBuffer = new byte[maxMessageSize * 2];
                pooledMsgBuffer = new byte[maxMessageSize * 2];
            }
            
            // Run each serializer on a portion of the messages to compare efficiency
            
            // Process messages in batches of 100
            const int batchSize = 100;
            for (int i = 0; i < messagesToProcess.Count; i += batchSize)
            {
                int currentBatchSize = Math.Min(batchSize, messagesToProcess.Count - i);
                
                for (int j = 0; j < currentBatchSize; j++)
                {
                    var message = messagesToProcess[i + j];
                    
                    // Standard serializer - 25% of messages
                    if (j % 4 == 0)
                    {
                        int written = _standardSerializer.Serialize(message, standardBuffer);
                        _standardSerializer.Deserialize(new ReadOnlySpan<byte>(standardBuffer, 0, written));
                        totalBytes += written;
                    }
                    // Pooled buffer serializer - 25% of messages
                    else if (j % 4 == 1)
                    {
                        int written = _pooledBufferSerializer.Serialize(message, pooledBuffer);
                        _pooledBufferSerializer.Deserialize(new ReadOnlySpan<byte>(pooledBuffer, 0, written));
                        totalBytes += written;
                    }
                    // Span-based serializer - 25% of messages
                    else if (j % 4 == 2)
                    {
                        int written = _spanBasedSerializer.Serialize(message, spanBuffer);
                        _spanBasedSerializer.Deserialize(new ReadOnlySpan<byte>(spanBuffer, 0, written));
                        totalBytes += written;
                    }
                    // Pooled message serializer - 25% of messages
                    else
                    {
                        int written = _pooledMessageSerializer.Serialize(message, pooledMsgBuffer);
                        _pooledMessageSerializer.Deserialize(new ReadOnlySpan<byte>(pooledMsgBuffer, 0, written));
                        totalBytes += written;
                    }
                }
                
                // Force GC every 1000 messages for extreme message counts
                if (MessageCount >= 50000 && i % 1000 == 0 && i > 0)
                {
                    GC.Collect();
                }
            }
            
            return totalBytes;
        }
        
        #endregion
        
        #region Advanced Throughput For Extreme Scale (500M+ Records)
        
        /// <summary>
        /// Stream-based pipeline with buffer reuse optimized for extreme throughput
        /// Ideal for processing hundreds of millions of records with minimal allocations
        /// </summary>
        [Benchmark]
        [BenchmarkCategory("ExtremeScale", "AdvancedThroughput")]
        public void StreamPipeline_WithBufferReuse()
        {
            // Skip for smaller message counts
            if (MessageCount < 5000)
            {
                return;
            }
            
            // Use the actual messages we have, but simulate processing many more
            var messagesToProcess = _realMessages;
            int messagesToProcessCount = messagesToProcess.Count;
            
            // Pre-allocate a single buffer for all serialization operations
            int maxMessageSize = messagesToProcess.Max(m => m.CalculateSize());
            byte[] sharedBuffer = new byte[maxMessageSize * 2]; // Extra space to be safe
            
            // Track the overall bytes processed
            long totalBytesProcessed = 0;
            
            // Process the requested number of messages using buffer reuse pattern
            const int batchSize = 10000; // Large batch size for efficiency
            
            // For simulating extreme scale
            int totalToProcess = MessageCount;
            int processed = 0;
            
            while (processed < totalToProcess)
            {
                int remainingToProcess = totalToProcess - processed;
                int currentBatchSize = Math.Min(batchSize, remainingToProcess);
                
                for (int j = 0; j < currentBatchSize; j++)
                {
                    // Cycle through our available messages to simulate large dataset
                    var message = messagesToProcess[j % messagesToProcessCount];
                    
                    // Use buffer-based serialization to avoid allocations
                    int bytesWritten = _spanBasedSerializer.Serialize(message, sharedBuffer);
                    totalBytesProcessed += bytesWritten;
                    
                    // Process serialized data directly from the buffer
                    // In a real scenario, we might write to a file, network, etc.
                    // Here we'll just deserialize to complete the processing pipeline
                    ReadOnlySpan<byte> serializedSpan = new ReadOnlySpan<byte>(sharedBuffer, 0, bytesWritten);
                    _spanBasedSerializer.Deserialize(serializedSpan);
                }
                
                processed += currentBatchSize;
                
                // Force garbage collection periodically
                if (totalToProcess > 50000 && processed % 100000 == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
        }
        
        /// <summary>
        /// Memory-mapped file approach for processing extreme data volumes with minimal memory usage
        /// This approach bypasses most GC pressure by using unmanaged memory
        /// </summary>
        [Benchmark] 
        [BenchmarkCategory("ExtremeScale", "AdvancedThroughput")]
        public void MemoryMappedFile_Throughput()
        {
            // Skip for smaller message counts
            if (MessageCount < 5000)
            {
                return;
            }
            
            try
            {
                // Use the messages we have, but simulate processing many more
                var messagesToProcess = _realMessages;
                int messagesToProcessCount = messagesToProcess.Count;
                
                // Determine max message size for buffer allocation
                int maxMessageSize = messagesToProcess.Max(m => m.CalculateSize());
                
                // Create a more reasonable buffer size (reduce from 64MB to 16MB max)
                int bufferSize = Math.Min(16 * 1024 * 1024, Math.Max(maxMessageSize * 100, 1024 * 1024));
                
                // Use a unique name to avoid conflicts with other benchmark runs
                string uniqueMmfName = $"ProcessingBuffer_{Guid.NewGuid()}";
                
                // Create memory-mapped file with try/catch for better error handling
                using var mmf = System.IO.MemoryMappedFiles.MemoryMappedFile.CreateNew(uniqueMmfName, bufferSize);
                using var accessor = mmf.CreateViewAccessor(0, bufferSize);
                
                // Buffer for serializing individual messages
                byte[] serializeBuffer = new byte[maxMessageSize * 2];
                
                // Process in smaller batches for better stability
                const int batchSize = 5000;
                long position = 0;
                
                // For simulating extreme scale
                int totalToProcess = MessageCount;
                int processed = 0;
                
                while (processed < totalToProcess)
                {
                    int remainingToProcess = totalToProcess - processed;
                    int currentBatchSize = Math.Min(batchSize, remainingToProcess);
                    
                    // Reset position for each batch
                    position = 0;
                    
                    // Serialize batch to memory-mapped file
                    for (int j = 0; j < currentBatchSize; j++)
                    {
                        // Cycle through our available messages to simulate large dataset
                        var message = messagesToProcess[j % messagesToProcessCount];
                        
                        // Serialize to our buffer with size checks
                        int bytesWritten = _spanBasedSerializer.Serialize(message, serializeBuffer);
                        
                        // Safety check to prevent buffer overruns
                        if (bytesWritten > serializeBuffer.Length)
                        {
                            throw new InvalidOperationException($"Message serialized to {bytesWritten} bytes, which exceeds buffer size of {serializeBuffer.Length}");
                        }
                        
                        // Make sure we don't exceed the memory-mapped file size
                        if (position + bytesWritten + sizeof(int) > bufferSize)
                        {
                            // We're out of space, process what we have and reset
                            break;
                        }
                        
                        // Copy to memory-mapped file using safer accessor methods
                        accessor.Write(position, bytesWritten); // Write size first
                        position += sizeof(int);
                        
                        // Then write the actual data
                        accessor.WriteArray(position, serializeBuffer, 0, bytesWritten);
                        position += bytesWritten;
                    }
                    
                    // Process the batch from memory-mapped file
                    long readPosition = 0;
                    
                    while (readPosition < position)
                    {
                        // Read size with bounds checking
                        if (readPosition + sizeof(int) > position) break;
                        
                        int messageSize = accessor.ReadInt32(readPosition);
                        readPosition += sizeof(int);
                        
                        // Safety check for message size
                        if (messageSize <= 0 || messageSize > serializeBuffer.Length || readPosition + messageSize > position)
                        {
                            break; // Invalid size, stop processing
                        }
                        
                        // Read message
                        accessor.ReadArray(readPosition, serializeBuffer, 0, messageSize);
                        
                        // Deserialize
                        ReadOnlySpan<byte> messageSpan = new ReadOnlySpan<byte>(serializeBuffer, 0, messageSize);
                        _spanBasedSerializer.Deserialize(messageSpan);
                        
                        readPosition += messageSize;
                    }
                    
                    processed += currentBatchSize;
                    
                    // Force garbage collection periodically
                    if (totalToProcess > 50000 && processed % 100000 == 0)
                    {
                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                    }
                }
            }
            catch (Exception ex)
            {
                // Log the error for debugging
                Console.WriteLine($"MemoryMappedFile_Throughput failed: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                
                // Rethrow to fail the benchmark properly rather than giving misleading results
                throw;
            }
        }
        
        /// <summary>
        /// Multi-threaded pipeline using System.Threading.Channels for optimal throughput
        /// This approach maximizes CPU utilization while controlling memory pressure
        /// </summary>
        [Benchmark]
        [BenchmarkCategory("ExtremeScale", "AdvancedThroughput")]
        public void ChannelPipeline_Concurrent()
        {
            // Skip for smaller message counts
            if (MessageCount < 5000)
            {
                return;
            }
            
            // Use the actual messages we have but simulate processing many more
            var messagesToProcess = _realMessages;
            int messagesToProcessCount = messagesToProcess.Count;
            
            // Determine optimal batch and channel sizes
            int processorCount = Environment.ProcessorCount;
            int channelCapacity = Math.Min(50000, MessageCount / 10);
            
            // Create a bounded channel to control memory pressure
            var channelOptions = new BoundedChannelOptions(channelCapacity)
            {
                SingleReader = false,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            };
            
            var messageChannel = Channel.CreateBounded<byte[]>(channelOptions);
            var resultChannel = Channel.CreateBounded<long>(channelOptions);
            
            // Create cancellation token source to manage worker lifetime
            using var cts = new CancellationTokenSource();
            var token = cts.Token;
            
            // Launch consumer tasks to process serialized messages
            var consumerTasks = new Task[processorCount - 1];
            for (int i = 0; i < processorCount - 1; i++)
            {
                consumerTasks[i] = Task.Run(async () =>
                {
                    long bytesProcessed = 0;
                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            if (await messageChannel.Reader.WaitToReadAsync(token).ConfigureAwait(false))
                            {
                                if (messageChannel.Reader.TryRead(out var serializedMessage))
                                {
                                    // Deserialize and process the message
                                    ReadOnlySpan<byte> messageSpan = new ReadOnlySpan<byte>(serializedMessage);
                                    _spanBasedSerializer.Deserialize(messageSpan);
                                    bytesProcessed += serializedMessage.Length;
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                    
                    // Report bytes processed by this consumer
                    await resultChannel.Writer.WriteAsync(bytesProcessed, token).ConfigureAwait(false);
                }, token);
            }
            
            // Producer task to generate serialized messages
            Task producerTask = Task.Run(async () =>
            {
                // For simulating extreme scale
                int totalToProcess = MessageCount;
                int processed = 0;
                
                // Process in batches
                const int batchSize = 1000; // Smaller batch for better distribution
                
                while (processed < totalToProcess && !token.IsCancellationRequested)
                {
                    int remainingToProcess = totalToProcess - processed;
                    int currentBatchSize = Math.Min(batchSize, remainingToProcess);
                    
                    for (int j = 0; j < currentBatchSize; j++)
                    {
                        // Cycle through our available messages to simulate large dataset
                        var message = messagesToProcess[j % messagesToProcessCount];
                        
                        // Allocate a buffer for this specific message
                        // In production, you'd likely use a buffer pool here
                        int messageSize = message.CalculateSize();
                        byte[] buffer = new byte[messageSize];
                        
                        // Serialize the message
                        int bytesWritten = _spanBasedSerializer.Serialize(message, buffer);
                        
                        // Write to channel but respect cancellation
                        await messageChannel.Writer.WriteAsync(buffer, token).ConfigureAwait(false);
                    }
                    
                    processed += currentBatchSize;
                }
                
                // Signal completion
                messageChannel.Writer.Complete();
            }, token);
            
            // Wait for the producer to finish
            producerTask.GetAwaiter().GetResult();
            
            // Signal consumers to complete processing
            cts.CancelAfter(TimeSpan.FromSeconds(5)); // Give time to process remaining items
            
            try
            {
                // Wait for all consumers to complete
                Task.WaitAll(consumerTasks, TimeSpan.FromSeconds(10));
            }
            catch (AggregateException)
            {
                // Expected when tasks are canceled
            }
            
            // Complete the result channel
            resultChannel.Writer.Complete();
            
            // Calculate total bytes processed
            long totalBytesProcessed = 0;
            while (resultChannel.Reader.TryRead(out long bytesProcessed))
            {
                totalBytesProcessed += bytesProcessed;
            }
            
            // Force final cleanup
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        
        #endregion
        
        #region Helper Methods
        
        /// <summary>
        /// Generates a test message with the specified approximate size in bytes
        /// </summary>
        private Message GenerateTestMessage(int approximateSizeBytes)
        {
            var message = new Message
            {
                Data = new MessageData
                {
                    Type = MessageType.CastAdd,
                    Fid = 12345,
                    Timestamp = (uint)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    Network = Network.Mainnet
                },
                Hash = ByteString.CopyFrom(Guid.NewGuid().ToByteArray()),
                HashScheme = HashScheme.Blake3,
                Signature = ByteString.CopyFrom(new byte[64]), // Ed25519 signature is 64 bytes
                SignatureScheme = SignatureScheme.Ed25519,
                Signer = ByteString.CopyFrom(new byte[32])  // Ed25519 public key is 32 bytes
            };
            
            // Add a cast body to increase message size
            int remainingSize = approximateSizeBytes - message.CalculateSize();
            if (remainingSize > 0)
            {
                message.Data.CastAddBody = new CastAddBody
                {
                    Text = GenerateRandomString(remainingSize)
                };
                
                // Add some mentions to make it more realistic
                for (int i = 1; i <= 5; i++)
                {
                    message.Data.CastAddBody.Mentions.Add((ulong)i);
                }
            }
            
            return message;
        }
        
        /// <summary>
        /// Generates a random string of the specified approximate byte length
        /// </summary>
        private string GenerateRandomString(int approximateByteLength)
        {
            // Each char in a string is approximately 2 bytes in UTF-16
            int charCount = approximateByteLength / 2;
            
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
            var result = new char[charCount];
            
            for (int i = 0; i < charCount; i++)
            {
                result[i] = chars[_random.Next(chars.Length)];
            }
            
            return new string(result);
        }
        
        /// <summary>
        /// Helper method to generate synthetic test data when real data isn't available
        /// </summary>
        private void GenerateSyntheticTestData(int messageCount)
        {
            // Clear any existing messages
            _realMessages.Clear();
            
            // Generate with different sizes to simulate realistic data
            int[] messageSizes = new[] { 500, 1000, 2000, 5000 };
            
            for (int i = 0; i < messageCount; i++)
            {
                // Cycle through different message sizes
                int messageSize = messageSizes[i % messageSizes.Length];
                _realMessages.Add(GenerateTestMessage(messageSize));
                
                // Report progress for large datasets
                if (messageCount > 10000 && i % 10000 == 0 && i > 0)
                {
                    Console.WriteLine($"Generated {i} synthetic messages...");
                }
            }
            
            Console.WriteLine($"Generated {_realMessages.Count} synthetic messages with varied sizes");
        }
        
        #endregion
    }
} 