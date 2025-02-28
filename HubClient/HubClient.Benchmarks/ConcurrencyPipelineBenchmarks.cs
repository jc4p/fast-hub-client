using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using HubClient.Core;
using HubClient.Core.Concurrency;
using HubClient.Core.Grpc;
using HubClient.Core.Serialization;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Benchmarks
{
    /// <summary>
    /// Benchmarks for comparing different concurrency pipeline implementations
    /// </summary>
    [MemoryDiagnoser]
    [GcServer(true)]
    [ThreadingDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    [EventPipeProfiler(BenchmarkDotNet.Diagnosers.EventPipeProfile.GcVerbose)]
    public class ConcurrencyPipelineBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        /// <summary>
        /// Number of messages to process
        /// </summary>
        [Params(10_000, 100_000)]
        public int MessageCount { get; set; }
        
        /// <summary>
        /// Number of messages to process per second (for bursty workload tests)
        /// </summary>
        [Params(100, 1000)]
        public int MessagesPerSecond { get; set; }
        
        /// <summary>
        /// Whether to use a bursty workload pattern
        /// </summary>
        [Params(false, true)]
        public bool BurstyWorkload { get; set; }
        
        // Test data and objects
        private List<Message> _messages = null!;
        private Random _random = null!;
        
        // Serializer
        private IMessageSerializer<Message> _serializer = null!;
        
        // Connection manager for gRPC calls (for testing)
        private MultiplexedChannelManager _channelManager = null!;
        private MinimalHubService.MinimalHubServiceClient _grpcClient = null!;
        
        // Stats
        private TimeSpan _minLatency;
        private TimeSpan _maxLatency;
        private TimeSpan _avgLatency;
        private TimeSpan _p99Latency;
        private double _throughput;
        private int _backpressureEvents;
        
        [GlobalSetup]
        public void Setup()
        {
            Console.WriteLine("DEBUG: Starting benchmark setup");
            
            try
            {
                // Initialize random
                _random = new Random(42); // Fixed seed for reproducibility
                Console.WriteLine("DEBUG: Random initialized");
                
                // Initialize serializer
                _serializer = MessageSerializerFactory.Create<Message>(MessageSerializerFactory.SerializerType.PooledBuffer);
                Console.WriteLine("DEBUG: Serializer initialized");
                
                // Create MultiplexedChannelManager
                Console.WriteLine($"DEBUG: Creating channel manager with endpoint {ServerEndpoint}");
                int channelCount = Math.Max(4, Environment.ProcessorCount / 2);
                int maxConcurrentCallsPerChannel = 100;
                
                try
                {
                    _channelManager = new MultiplexedChannelManager(ServerEndpoint, channelCount, maxConcurrentCallsPerChannel);
                    _grpcClient = _channelManager.CreateClient<MinimalHubService.MinimalHubServiceClient>();
                    Console.WriteLine("DEBUG: Channel manager and gRPC client initialized");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR: Failed to create channel manager: {ex.Message}");
                    Console.WriteLine($"ERROR: Stack trace: {ex.StackTrace}");
                    
                    // Continue with null client - we'll handle this in the ProcessMessageAsync method
                }
                
                // Generate test messages - we'll generate 25% more than needed for batches
                int messagesToGenerate = (int)(MessageCount * 1.25);
                Console.WriteLine($"DEBUG: Generating {messagesToGenerate} test messages...");
                _messages = new List<Message>(messagesToGenerate);
                
                for (int i = 0; i < messagesToGenerate; i++)
                {
                    try
                    {
                        var msg = GenerateTestMessage(1024); // 1KB messages
                        _messages.Add(msg);
                        
                        if (i % 10000 == 0 && i > 0)
                        {
                            Console.WriteLine($"DEBUG: Generated {i} test messages so far");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"ERROR: Failed to generate message {i}: {ex.Message}");
                        // Add a placeholder message instead
                        _messages.Add(new Message());
                    }
                }
                
                Console.WriteLine($"DEBUG: Successfully generated {_messages.Count} messages");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"CRITICAL ERROR in Setup: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                
                // Initialize with empty list to prevent null reference exceptions
                _messages = new List<Message>();
                _random = new Random(42);
            }
            
            Console.WriteLine("Setup complete.");
        }
        
        [GlobalCleanup]
        public void Cleanup()
        {
            _channelManager.Dispose();
        }
        
        [IterationSetup]
        public void IterationSetup()
        {
            // Reset stats
            _minLatency = TimeSpan.MaxValue;
            _maxLatency = TimeSpan.Zero;
            _avgLatency = TimeSpan.Zero;
            _p99Latency = TimeSpan.Zero;
            _throughput = 0;
            _backpressureEvents = 0;
        }
        
        [IterationCleanup]
        public void IterationCleanup()
        {
            // Log stats
            Console.WriteLine($"Min latency: {_minLatency.TotalMilliseconds:F2} ms");
            Console.WriteLine($"Max latency: {_maxLatency.TotalMilliseconds:F2} ms");
            Console.WriteLine($"Avg latency: {_avgLatency.TotalMilliseconds:F2} ms");
            Console.WriteLine($"P99 latency: {_p99Latency.TotalMilliseconds:F2} ms");
            Console.WriteLine($"Throughput: {_throughput:F2} msgs/sec");
            Console.WriteLine($"Backpressure events: {_backpressureEvents}");
            
            // Force GC to clean up between iterations
            GC.Collect(2, GCCollectionMode.Forced, true, true);
            GC.WaitForPendingFinalizers();
        }
        
        #region Benchmark Methods
        
        /// <summary>
        /// Baseline benchmark using TPL Dataflow
        /// </summary>
        [Benchmark(Baseline = true)]
        public async Task DataflowPipeline()
        {
            await RunBenchmark(PipelineStrategy.Dataflow);
        }
        
        /// <summary>
        /// Benchmark using System.Threading.Channels
        /// </summary>
        [Benchmark]
        public async Task ChannelPipeline()
        {
            await RunBenchmark(PipelineStrategy.Channel);
        }
        
        /// <summary>
        /// Benchmark using thread-per-core model
        /// </summary>
        [Benchmark]
        public async Task ThreadPerCorePipeline()
        {
            await RunBenchmark(PipelineStrategy.ThreadPerCore);
        }
        
        /// <summary>
        /// Specific test to verify that the ChannelPipeline implementation properly tracks all messages 
        /// and completes correctly without message loss
        /// </summary>
        [Benchmark(Description = "Verify message tracking in Channel implementation")]
        public async Task VerifyChannelPipelineMessageTracking()
        {
            // Higher message count to increase chances of revealing race conditions
            const int verificationMessageCount = 12500;
            int outputCount = 0;
            
            // If _messages is empty (likely because we're running directly without BenchmarkDotNet),
            // generate test messages directly in this method
            if (_messages == null || _messages.Count == 0)
            {
                Console.WriteLine("DEBUG: No messages found from Setup(), generating messages directly");
                _messages = new List<Message>(verificationMessageCount);
                for (int i = 0; i < verificationMessageCount; i++)
                {
                    _messages.Add(GenerateTestMessage(512)); // Smaller size for faster test
                    if (i % 1000 == 0 && i > 0)
                    {
                        Console.WriteLine($"DEBUG: Generated {i} test messages directly");
                    }
                }
                Console.WriteLine($"DEBUG: Generated {_messages.Count} test messages directly");
            }
            
            Console.WriteLine("DEBUG: Creating pipeline options");
            // Create the pipeline with default options
            var options = new PipelineCreationOptions
            {
                InputQueueCapacity = 50_000,
                OutputQueueCapacity = 50_000,
                MaxConcurrency = Environment.ProcessorCount * 2
            };
            
            Console.WriteLine("DEBUG: Creating pipeline");
            // Create the pipeline specifically using the Channel strategy
            using var pipeline = PipelineFactory.Create<Message, ProcessedMessage>(
                PipelineStrategy.Channel,
                ProcessMessageAsync,
                options);
                
            Console.WriteLine("Starting message tracking verification test...");
            
            Console.WriteLine("DEBUG: Setting up consumer task");
            // Consumer Task with cancellation support
            using var consumerCts = new CancellationTokenSource();
            
            // Consumer Task
            var consumerTask = pipeline.ConsumeAsync(
                message => {
                    Interlocked.Increment(ref outputCount);
                    return ValueTask.CompletedTask;
                },
                consumerCts.Token);
            
            // Enqueue messages in batches to increase concurrency
            // Ensure we only use messages that exist
            int actualMessageCount = Math.Min(verificationMessageCount, _messages.Count);
            var messageSubset = _messages.Take(actualMessageCount).ToList();
            
            Console.WriteLine($"DEBUG: Preparing to enqueue {messageSubset.Count} messages");
            const int batchSize = 1000;
            
            for (int i = 0; i < messageSubset.Count; i += batchSize)
            {
                var batch = messageSubset.Skip(i).Take(Math.Min(batchSize, messageSubset.Count - i)).ToList();
                await pipeline.EnqueueBatchAsync(batch);
                
                // Small delay to allow processing to start
                if (i % 10000 == 0 && i > 0)
                {
                    Console.WriteLine($"  Enqueued {i} messages...");
                    await Task.Delay(10); // Brief delay to allow processing to catch up
                }
            }
            
            Console.WriteLine($"DEBUG: All {messageSubset.Count} messages enqueued, waiting for completion...");
            
            // Complete the pipeline and wait for it to finish
            var sw = Stopwatch.StartNew();
            
            Console.WriteLine("DEBUG: Before pipeline.CompleteAsync()");
            await pipeline.CompleteAsync();
            Console.WriteLine("DEBUG: After pipeline.CompleteAsync()");
            
            Console.WriteLine("DEBUG: Before await consumerTask");
            
            // Wait for consumer task with timeout
            try 
            {
                // Wait for consumer with a 30-second timeout
                var consumerTimeoutTask = Task.Delay(TimeSpan.FromSeconds(30));
                var completedTask = await Task.WhenAny(consumerTask, consumerTimeoutTask);
                
                if (completedTask == consumerTimeoutTask)
                {
                    Console.WriteLine("DEBUG: Consumer task timed out after 30 seconds");
                    Console.WriteLine($"DEBUG: Consumer timeout - Current state: Processed={pipeline.ProcessedItemCount}, Consumed={outputCount}, Expected={messageSubset.Count}");
                    
                    // Force cancel the consumer task
                    Console.WriteLine("DEBUG: Cancelling consumer task");
                    consumerCts.Cancel();
                    
                    // Try to wait a bit more with cancellation
                    try 
                    {
                        await Task.WhenAny(consumerTask, Task.Delay(5000));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"DEBUG: Error while waiting after cancellation: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine("DEBUG: Consumer task completed normally");
                    // Try to complete the task just to get any exceptions
                    await consumerTask;
                }
            }
            catch (Exception ex) 
            {
                Console.WriteLine($"DEBUG: Error waiting for consumer task: {ex.Message}");
            }
            
            Console.WriteLine("DEBUG: After await consumerTask");
            
            sw.Stop();
            
            Console.WriteLine($"Pipeline complete. Processed {outputCount} messages in {sw.ElapsedMilliseconds}ms");
            Console.WriteLine($"Expected {messageSubset.Count} messages");
            
            // Verify that all messages were processed
            if (outputCount != messageSubset.Count)
            {
                Console.WriteLine($"VERIFICATION FAILED: Expected {messageSubset.Count} processed messages, but got {outputCount}");
                throw new InvalidOperationException(
                    $"VERIFICATION FAILED: Expected {messageSubset.Count} processed messages, but got {outputCount}");
            }
            
            Console.WriteLine("VERIFICATION PASSED: All messages were processed correctly");
            Console.WriteLine("DEBUG: Verification complete, preparing to exit");
            
            // Force exit after successful completion to prevent hanging
            Console.WriteLine("DEBUG: About to exit process after successful verification...");
            try 
            {
                Console.WriteLine("DEBUG: Calling Environment.Exit(0)");
                // Flush any pending console output before exiting
                Console.Out.Flush();
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DEBUG: Failed to exit using Environment.Exit(0): {ex.Message}");
                Console.WriteLine("DEBUG: Attempting to kill the process...");
                try 
                {
                    Console.Out.Flush();
                    System.Diagnostics.Process.GetCurrentProcess().Kill();
                }
                catch (Exception killEx)
                {
                    Console.WriteLine($"DEBUG: Failed to kill process: {killEx.Message}");
                }
            }
            
            Console.WriteLine("DEBUG: Process should have exited by now!");
            
            // As a last resort, force garbage collection and exit the method
            Console.WriteLine("DEBUG: Forcing GC and returning from method as last resort");
            GC.Collect(2, GCCollectionMode.Forced, true, true);
            GC.WaitForPendingFinalizers();
            return;
        }
        
        /// <summary>
        /// Specific test to verify that the DataflowPipeline implementation properly tracks all messages 
        /// and completes correctly without message loss
        /// </summary>
        [Benchmark(Description = "Verify message tracking in Dataflow implementation")]
        public async Task VerifyDataflowPipelineMessageTracking()
        {
            // Higher message count to increase chances of revealing race conditions
            const int verificationMessageCount = 12500;
            int outputCount = 0;
            
            // If _messages is empty (likely because we're running directly without BenchmarkDotNet),
            // generate test messages directly in this method
            if (_messages == null || _messages.Count == 0)
            {
                Console.WriteLine("DEBUG: No messages found from Setup(), generating messages directly");
                _messages = new List<Message>(verificationMessageCount);
                for (int i = 0; i < verificationMessageCount; i++)
                {
                    _messages.Add(GenerateTestMessage(512)); // Smaller size for faster test
                    if (i % 1000 == 0 && i > 0)
                    {
                        Console.WriteLine($"DEBUG: Generated {i} test messages directly");
                    }
                }
                Console.WriteLine($"DEBUG: Generated {_messages.Count} test messages directly");
            }
            
            // Create the pipeline with default options
            var options = new PipelineCreationOptions
            {
                InputQueueCapacity = 50_000,
                OutputQueueCapacity = 50_000,
                MaxConcurrency = Environment.ProcessorCount * 2
            };
            
            // Create the pipeline specifically using the Dataflow strategy
            using var pipeline = PipelineFactory.Create<Message, ProcessedMessage>(
                PipelineStrategy.Dataflow,
                ProcessMessageAsync,
                options);
                
            Console.WriteLine("Starting Dataflow message tracking verification test...");
            
            // Consumer Task
            var consumerTask = pipeline.ConsumeAsync(
                message => {
                    Interlocked.Increment(ref outputCount);
                    return ValueTask.CompletedTask;
                },
                CancellationToken.None);
            
            // Enqueue messages in batches to increase concurrency
            // Ensure we only use messages that exist
            int actualMessageCount = Math.Min(verificationMessageCount, _messages.Count);
            var messageSubset = _messages.Take(actualMessageCount).ToList();
            
            Console.WriteLine($"DEBUG: Preparing to enqueue {messageSubset.Count} messages");
            const int batchSize = 1000;
            
            for (int i = 0; i < messageSubset.Count; i += batchSize)
            {
                var batch = messageSubset.Skip(i).Take(Math.Min(batchSize, messageSubset.Count - i)).ToList();
                await pipeline.EnqueueBatchAsync(batch);
                
                // Small delay to allow processing to start
                if (i % 10000 == 0 && i > 0)
                {
                    Console.WriteLine($"  Enqueued {i} messages...");
                    await Task.Delay(10); // Brief delay to allow processing to catch up
                }
            }
            
            Console.WriteLine($"DEBUG: All {messageSubset.Count} messages enqueued, waiting for completion...");
            
            // Complete the pipeline and wait for it to finish
            var sw = Stopwatch.StartNew();
            await pipeline.CompleteAsync();
            await consumerTask;
            sw.Stop();
            
            Console.WriteLine($"Pipeline complete. Processed {outputCount} messages in {sw.ElapsedMilliseconds}ms");
            Console.WriteLine($"Expected {messageSubset.Count} messages");
            
            // Verify that all messages were processed
            if (outputCount != messageSubset.Count)
            {
                throw new InvalidOperationException(
                    $"VERIFICATION FAILED: Expected {messageSubset.Count} processed messages, but got {outputCount}");
            }
            
            Console.WriteLine("VERIFICATION PASSED: All messages were processed correctly");
            
            // Force exit after successful completion to prevent hanging
            Console.WriteLine("DEBUG: About to exit process after successful verification...");
            try 
            {
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DEBUG: Failed to exit using Environment.Exit(0): {ex.Message}");
                // More forceful approach
                Console.WriteLine("DEBUG: Attempting to kill the process...");
                System.Diagnostics.Process.GetCurrentProcess().Kill();
            }
            
            Console.WriteLine("DEBUG: Process should have exited by now!");
        }
        
        /// <summary>
        /// Specific test to verify that the ThreadPerCorePipeline implementation properly tracks all messages 
        /// and completes correctly without message loss
        /// </summary>
        [Benchmark(Description = "Verify message tracking in ThreadPerCore implementation")]
        public async Task VerifyThreadPerCorePipelineMessageTracking()
        {
            // Higher message count to increase chances of revealing race conditions
            const int verificationMessageCount = 12500;
            int outputCount = 0;
            
            // If _messages is empty (likely because we're running directly without BenchmarkDotNet),
            // generate test messages directly in this method
            if (_messages == null || _messages.Count == 0)
            {
                Console.WriteLine("DEBUG: No messages found from Setup(), generating messages directly");
                _messages = new List<Message>(verificationMessageCount);
                for (int i = 0; i < verificationMessageCount; i++)
                {
                    _messages.Add(GenerateTestMessage(512)); // Smaller size for faster test
                    if (i % 1000 == 0 && i > 0)
                    {
                        Console.WriteLine($"DEBUG: Generated {i} test messages directly");
                    }
                }
                Console.WriteLine($"DEBUG: Generated {_messages.Count} test messages directly");
            }
            
            // Create the pipeline with default options
            var options = new PipelineCreationOptions
            {
                InputQueueCapacity = 50_000,
                OutputQueueCapacity = 50_000,
                MaxConcurrency = Environment.ProcessorCount * 2
            };
            
            // Create the pipeline specifically using the ThreadPerCore strategy
            using var pipeline = PipelineFactory.Create<Message, ProcessedMessage>(
                PipelineStrategy.ThreadPerCore,
                ProcessMessageAsync,
                options);
                
            Console.WriteLine("Starting ThreadPerCore message tracking verification test...");
            
            // Consumer Task
            var consumerTask = pipeline.ConsumeAsync(
                message => {
                    Interlocked.Increment(ref outputCount);
                    return ValueTask.CompletedTask;
                },
                CancellationToken.None);
            
            // Enqueue messages in batches to increase concurrency
            // Ensure we only use messages that exist
            int actualMessageCount = Math.Min(verificationMessageCount, _messages.Count);
            var messageSubset = _messages.Take(actualMessageCount).ToList();
            
            Console.WriteLine($"DEBUG: Preparing to enqueue {messageSubset.Count} messages");
            const int batchSize = 1000;
            
            for (int i = 0; i < messageSubset.Count; i += batchSize)
            {
                var batch = messageSubset.Skip(i).Take(Math.Min(batchSize, messageSubset.Count - i)).ToList();
                await pipeline.EnqueueBatchAsync(batch);
                
                // Small delay to allow processing to start
                if (i % 10000 == 0 && i > 0)
                {
                    Console.WriteLine($"  Enqueued {i} messages...");
                    await Task.Delay(10); // Brief delay to allow processing to catch up
                }
            }
            
            Console.WriteLine($"DEBUG: All {messageSubset.Count} messages enqueued, waiting for completion...");
            
            // Complete the pipeline and wait for it to finish
            var sw = Stopwatch.StartNew();
            await pipeline.CompleteAsync();
            await consumerTask;
            sw.Stop();
            
            Console.WriteLine($"Pipeline complete. Processed {outputCount} messages in {sw.ElapsedMilliseconds}ms");
            Console.WriteLine($"Expected {messageSubset.Count} messages");
            
            // Verify that all messages were processed
            if (outputCount != messageSubset.Count)
            {
                throw new InvalidOperationException(
                    $"VERIFICATION FAILED: Expected {messageSubset.Count} processed messages, but got {outputCount}");
            }
            
            Console.WriteLine("VERIFICATION PASSED: All messages were processed correctly");
            
            // Force exit after successful completion to prevent hanging
            Console.WriteLine("DEBUG: About to exit process after successful verification...");
            try 
            {
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DEBUG: Failed to exit using Environment.Exit(0): {ex.Message}");
                // More forceful approach
                Console.WriteLine("DEBUG: Attempting to kill the process...");
                System.Diagnostics.Process.GetCurrentProcess().Kill();
            }
            
            Console.WriteLine("DEBUG: Process should have exited by now!");
        }
        
        #endregion
        
        /// <summary>
        /// Runs the benchmark with the specified pipeline strategy
        /// </summary>
        /// <param name="strategy">The pipeline strategy to use</param>
        private async Task RunBenchmark(PipelineStrategy strategy)
        {
            // Create pipeline options
            var options = new PipelineCreationOptions
            {
                InputQueueCapacity = Math.Max(10000, MessageCount / 10),
                OutputQueueCapacity = Math.Max(10000, MessageCount / 10),
                MaxConcurrency = Environment.ProcessorCount * 2,
                AllowSynchronousContinuations = false
            };
            
            // Create the pipeline
            using var pipeline = PipelineFactory.Create<Message, ProcessedMessage>(
                strategy,
                ProcessMessageAsync,
                options);
                
            // Start a consumer task for the output
            var outputProcessedCount = 0;
            var consumerTask = pipeline.ConsumeAsync(
                message => {
                    Interlocked.Increment(ref outputProcessedCount);
                    return ValueTask.CompletedTask;
                },
                CancellationToken.None);
                
            // Stopwatch for timing
            var stopwatch = Stopwatch.StartNew();
            
            if (BurstyWorkload)
            {
                // Simulate bursty workload with throttling
                await RunBurstyWorkload(pipeline);
            }
            else
            {
                // Send all messages at once as a batch
                await pipeline.EnqueueBatchAsync(_messages.Take(MessageCount).ToList());
            }
            
            // Wait for all messages to be processed
            await pipeline.CompleteAsync();
            await consumerTask;
            
            stopwatch.Stop();
            
            // Gather metrics
            _minLatency = pipeline.Metrics.MinLatency;
            _maxLatency = pipeline.Metrics.MaxLatency;
            _avgLatency = pipeline.Metrics.AverageLatency;
            _p99Latency = pipeline.Metrics.P99Latency;
            _throughput = MessageCount / stopwatch.Elapsed.TotalSeconds;
            _backpressureEvents = (int)pipeline.Metrics.TotalBackpressureEvents;
            
            // Verification
            if (outputProcessedCount != MessageCount)
            {
                throw new InvalidOperationException(
                    $"Expected {MessageCount} processed messages, but got {outputProcessedCount}");
            }
        }
        
        /// <summary>
        /// Runs a bursty workload pattern
        /// </summary>
        private async Task RunBurstyWorkload(IConcurrencyPipeline<Message, ProcessedMessage> pipeline)
        {
            var messageList = _messages.Take(MessageCount).ToList();
            int messagesRemaining = messageList.Count;
            int messageIndex = 0;
            
            // Calculate the delay between batches to maintain the desired messages/sec rate
            int batchSize = Math.Min(MessagesPerSecond / 10, 100); // Split into smaller batches
            int batchesPerSecond = (MessagesPerSecond + batchSize - 1) / batchSize; // Ceiling division
            int delayBetweenBatchesMs = 1000 / batchesPerSecond;
            
            Console.WriteLine($"Bursty workload: {batchSize} msgs/batch, {batchesPerSecond} batches/sec, {delayBetweenBatchesMs}ms delay");
            
            // Send the messages in bursts
            while (messagesRemaining > 0)
            {
                // Determine batch size (last batch may be smaller)
                int currentBatchSize = Math.Min(batchSize, messagesRemaining);
                var batch = messageList.GetRange(messageIndex, currentBatchSize);
                
                // Send the batch
                await pipeline.EnqueueBatchAsync(batch);
                
                // Update counters
                messageIndex += currentBatchSize;
                messagesRemaining -= currentBatchSize;
                
                // Simulate random bursts by occasionally sending a larger batch
                if (_random.Next(10) == 0 && messagesRemaining >= batchSize * 2)
                {
                    // Send a "burst" of 2-5x the normal batch size
                    int burstMultiplier = _random.Next(2, 6);
                    int burstSize = Math.Min(batchSize * burstMultiplier, messagesRemaining);
                    var burstBatch = messageList.GetRange(messageIndex, burstSize);
                    
                    // Send the burst batch
                    await pipeline.EnqueueBatchAsync(burstBatch);
                    
                    // Update counters
                    messageIndex += burstSize;
                    messagesRemaining -= burstSize;
                    
                    Console.WriteLine($"Sent burst of {burstSize} messages");
                }
                
                // Wait before sending the next batch
                if (messagesRemaining > 0)
                {
                    await Task.Delay(delayBetweenBatchesMs);
                }
            }
        }
        
        /// <summary>
        /// Processes a message asynchronously
        /// </summary>
        /// <param name="message">The message to process</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A processed message</returns>
        private async ValueTask<ProcessedMessage> ProcessMessageAsync(Message message, CancellationToken cancellationToken)
        {
            // Serialize the message
            byte[] serialized = _serializer.Serialize(message);
            
            ProcessedMessage result;
            
            // Simulate different processing patterns
            int processingType = _random.Next(4);
            switch (processingType)
            {
                case 0: // Simple CPU-bound processing
                    // Simulate some CPU-bound work - using encryption or hashing
                    result = SimulateCpuBoundProcessing(message, serialized);
                    break;
                    
                case 1: // I/O-bound processing (simulated with delay)
                    // Simulate network I/O with delay
                    await Task.Delay(_random.Next(1, 5), cancellationToken);
                    result = new ProcessedMessage(message.Data.Fid, message.Data.Timestamp, serialized.Length);
                    break;
                    
                case 2: // Mixed processing with gRPC call
                    // Make a real gRPC call
                    try
                    {
                        var fidRequest = new FidRequest { Fid = 24 }; // Fixed FID for testing
                        var response = await _grpcClient.GetUserDataByFidAsync(fidRequest, cancellationToken: cancellationToken);
                        result = new ProcessedMessage(
                            message.Data.Fid, 
                            message.Data.Timestamp, 
                            serialized.Length + (response.Messages.Count > 0 ? response.Messages[0].Data.ToString().Length : 0));
                    }
                    catch
                    {
                        // Fallback if the call fails
                        result = new ProcessedMessage(message.Data.Fid, message.Data.Timestamp, serialized.Length);
                    }
                    break;
                    
                default: // Combined processing
                    // Both CPU-bound work and a small delay
                    result = SimulateCpuBoundProcessing(message, serialized);
                    await Task.Delay(_random.Next(0, 2), cancellationToken);
                    break;
            }
            
            return result;
        }
        
        /// <summary>
        /// Simulates CPU-bound processing of a message
        /// </summary>
        private ProcessedMessage SimulateCpuBoundProcessing(Message message, byte[] serialized)
        {
            // Simulate CPU-bound work by computing a hash of the serialized data
            ulong hash = 0;
            for (int i = 0; i < serialized.Length; i++)
            {
                hash = hash * 31 + serialized[i];
            }
            
            // Create a result with the computed hash
            return new ProcessedMessage(
                message.Data.Fid, 
                message.Data.Timestamp, 
                serialized.Length, 
                hash);
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
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            var stringBuilder = new System.Text.StringBuilder(approximateByteLength / 2); // Each char is ~2 bytes in UTF-16
            
            for (int i = 0; i < approximateByteLength / 2; i++)
            {
                stringBuilder.Append(chars[_random.Next(chars.Length)]);
            }
            
            return stringBuilder.ToString();
        }
        
        /// <summary>
        /// Represents a processed message result
        /// </summary>
        public readonly struct ProcessedMessage
        {
            /// <summary>
            /// The FID of the message
            /// </summary>
            public readonly ulong Fid;
            
            /// <summary>
            /// The timestamp of the message
            /// </summary>
            public readonly uint Timestamp;
            
            /// <summary>
            /// The size of the serialized message
            /// </summary>
            public readonly int SerializedSize;
            
            /// <summary>
            /// A hash value computed from the message
            /// </summary>
            public readonly ulong Hash;
            
            /// <summary>
            /// Creates a new ProcessedMessage
            /// </summary>
            public ProcessedMessage(ulong fid, uint timestamp, int serializedSize, ulong hash = 0)
            {
                Fid = fid;
                Timestamp = timestamp;
                SerializedSize = serializedSize;
                Hash = hash;
            }
        }
    }
} 