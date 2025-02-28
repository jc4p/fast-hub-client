using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using HubClient.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using static HubClient.Benchmarks.ResilienceBenchmarks; // Import the MockGrpcService from ResilienceBenchmarks

namespace HubClient.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class ScalabilityBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        // Extreme message counts to test scalability
        [Params(5000, 10000, 25000)]
        public int MessageCount { get; set; }
        
        // Higher concurrency levels
        [Params(20, 50, 100)]
        public int ConcurrentConnections { get; set; }

        // Simulation error rate
        public double ErrorRate { get; set; } = 0.05; // 5% error rate

        // Metrics tracking
        private double _messagesPerSecond;
        private int _successfulCalls;
        private int _failedCalls;
        private double[] _channelDistribution = Array.Empty<double>();
        private TimeSpan _averageCallTime;

        [GlobalSetup]
        public void Setup()
        {
            // Warmup to avoid cold start penalties
            using (var manager = new MultiplexedChannelManager(ServerEndpoint, 8, 100))
            {
                var client = manager.CreateResilientClient<MockGrpcService.MockGrpcServiceClient>();
                // Make a few calls to warm up connections
                for (int i = 0; i < 10; i++)
                {
                    client.CallAsync((c, ct) => c.GetUserDataByFidAsync(
                        new MockGrpcService.UserDataRequest { Fid = "24", SimulatedErrorRate = 0 }, 
                        cancellationToken: ct),
                        "Warmup").GetAwaiter().GetResult();
                }
            }
        }

        [Benchmark(Description = "MultiplexedChannel-Fixed8")]
        public async Task MultiplexedChannelFixed()
        {
            // Fixed configuration with 8 channels regardless of concurrency level
            int channelCount = 8;
            int maxConcurrentCallsPerChannel = Math.Max(5, ConcurrentConnections / channelCount);
            
            using var connectionManager = new MultiplexedChannelManager(
                ServerEndpoint, 
                channelCount, 
                maxConcurrentCallsPerChannel);
                
            await RunConcurrentOperations(connectionManager);
            
            // Capture metrics
            _channelDistribution = connectionManager.Metrics.ChannelDistributionPercentages;
            _averageCallTime = connectionManager.Metrics.AverageTotalCallTime;
        }

        [Benchmark(Description = "MultiplexedChannel-Dynamic")]
        public async Task MultiplexedChannelDynamic()
        {
            // Dynamic configuration that scales with concurrency
            int channelCount = Math.Min(ConcurrentConnections / 5, 16);
            if (channelCount < 4) channelCount = 4;
            
            int maxConcurrentCallsPerChannel = Math.Max(10, ConcurrentConnections / channelCount * 2);
            
            using var connectionManager = new MultiplexedChannelManager(
                ServerEndpoint, 
                channelCount, 
                maxConcurrentCallsPerChannel);
                
            await RunConcurrentOperations(connectionManager);
            
            // Capture metrics
            _channelDistribution = connectionManager.Metrics.ChannelDistributionPercentages;
            _averageCallTime = connectionManager.Metrics.AverageTotalCallTime;
        }

        [Benchmark(Description = "Optimized", Baseline = true)]
        public async Task OptimizedManager()
        {
            // Include the baseline for comparison
            using var connectionManager = new OptimizedGrpcConnectionManager(ServerEndpoint, ConcurrentConnections);
            await RunConcurrentOperations(connectionManager);
        }

        private async Task RunConcurrentOperations(IGrpcConnectionManager connectionManager)
        {
            // Time the entire operation for throughput calculation
            var stopwatch = Stopwatch.StartNew();
            
            // Use a resilient client
            var resilientClient = connectionManager.CreateResilientClient<MockGrpcService.MockGrpcServiceClient>();
            
            // Reset tracking
            _successfulCalls = 0;
            _failedCalls = 0;
            
            // Create a semaphore to control concurrency
            using var semaphore = new System.Threading.SemaphoreSlim(ConcurrentConnections);
            var tasks = new List<Task>(ConcurrentConnections);
            
            Console.WriteLine($"Starting benchmark with {MessageCount} messages, {ConcurrentConnections} concurrent connections");
            
            // Create a random number generator for IDs
            var random = new Random();
            var testIds = new string[1000];
            for (int i = 0; i < testIds.Length; i++)
            {
                testIds[i] = Guid.NewGuid().ToString();
            }
            
            for (int i = 0; i < MessageCount; i++)
            {
                await semaphore.WaitAsync();
                
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        // Generate a random ID
                        string id = testIds[random.Next(testIds.Length)];
                        
                        // Create a request
                        var request = new MockGrpcService.UserDataRequest { 
                            Fid = id, 
                            SimulatedErrorRate = ErrorRate 
                        };
                        
                        // Make the call using the resilient client
                        var response = await resilientClient.CallAsync(
                            (client, ct) => client.GetUserDataByFidAsync(
                                request, 
                                cancellationToken: ct),
                            $"GetUserData-{id}");
                            
                        // Validate response
                        if (response != null)
                        {
                            System.Threading.Interlocked.Increment(ref _successfulCalls);
                        }
                        else
                        {
                            System.Threading.Interlocked.Increment(ref _failedCalls);
                        }
                    }
                    catch (Exception)
                    {
                        System.Threading.Interlocked.Increment(ref _failedCalls);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
                
                // Manage active tasks to avoid creating too many at once
                if (tasks.Count >= ConcurrentConnections * 2)
                {
                    var completed = await Task.WhenAny(tasks);
                    tasks.Remove(completed);
                }
            }
            
            // Wait for all remaining tasks
            await Task.WhenAll(tasks);
            
            stopwatch.Stop();
            
            // Calculate throughput
            _messagesPerSecond = MessageCount / stopwatch.Elapsed.TotalSeconds;
            
            Console.WriteLine($"Completed benchmark: {_messagesPerSecond:F2} msgs/sec, " +
                          $"Success: {_successfulCalls}, Failed: {_failedCalls}, " +
                          $"Duration: {stopwatch.Elapsed.TotalSeconds:F2}s");
        }
        
        [IterationCleanup]
        public void IterationCleanup()
        {
            // Output detailed metrics per iteration
            Console.WriteLine($"Messages/sec: {_messagesPerSecond:F2}");
            Console.WriteLine($"Successful calls: {_successfulCalls}, Failed calls: {_failedCalls}");
            Console.WriteLine($"Average call time: {_averageCallTime.TotalMilliseconds:F2}ms");
            Console.WriteLine($"Channel distribution: {string.Join(", ", _channelDistribution)}");
            
            // Force garbage collection between runs to minimize interference
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
        }
    }
} 