using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using HubClient.Core;
using HubClient.Core.Grpc;
using HubClient.Core.Resilience;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using static HubClient.Benchmarks.ResilienceBenchmarks;
using Grpc.Net.Client;
using System.Collections.Concurrent;
using System.Threading;

namespace HubClient.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class ExtremeConcurrencyBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        // Fixed message count at the highest level
        [Params(25000)]
        public int MessageCount { get; set; }
        
        // Testing extreme concurrency levels
        [Params(100, 250, 500, 750, 1000)]
        public int ConcurrentConnections { get; set; }

        // Simulation error rate
        public double ErrorRate { get; set; } = 0.05; // 5% error rate

        // Stats tracking
        private double _messagesPerSecond;
        private TimeSpan _establishmentTime;
        private TimeSpan _timeToFirstByte;
        private double[] _channelDistribution = Array.Empty<double>();
        private int _failedCalls;
        private int _successfulCalls;
        
        // Test ids for consistent usage - simple numeric strings
        private string[] _testIds;

        private MultiplexedChannelManager _multiplexedChannelManagerFixed8;
        private MultiplexedChannelManager _multiplexedChannelManagerDynamicPool;
        private DirectChannelManager _directChannelManager;

        [GlobalSetup]
        public void Setup()
        {
            // Create a range of simple numeric Fids
            _testIds = new string[50000];
            for (int i = 0; i < _testIds.Length; i++)
            {
                _testIds[i] = (i + 1).ToString();
            }
            
            Console.WriteLine("Setup complete - connecting to real server");
            
            // Initialize connection managers with real server endpoint
            _multiplexedChannelManagerFixed8 = new MultiplexedChannelManager(ServerEndpoint, 8);
            _multiplexedChannelManagerDynamicPool = new MultiplexedChannelManager(ServerEndpoint, ConcurrentConnections);
            _directChannelManager = new DirectChannelManager(ServerEndpoint, 1);
            
            // Minimal connection test output
            try
            {
                var resilientClient = _multiplexedChannelManagerFixed8.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
                var fidRequest = new FidRequest { Fid = 24UL };
                
                var response = resilientClient.CallAsync(
                    (client, ct) => client.GetUserDataByFidAsync(fidRequest, cancellationToken: ct).ResponseAsync,
                    "GetUserDataByFid").Result;
                    
                Console.WriteLine("Server connection verified successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WARNING: Server connection failed: {ex.Message}");
            }
        }

        [Benchmark(Description = "MultiplexedChannel-Fixed8")]
        public async Task MultiplexedChannelFixed()
        {
            // Fixed configuration with 8 channels regardless of concurrency level
            int channelCount = 8;
            int maxConcurrentCallsPerChannel = Math.Max(10, ConcurrentConnections / channelCount);
            
            using var connectionManager = new MultiplexedChannelManager(
                ServerEndpoint, 
                channelCount, 
                maxConcurrentCallsPerChannel);
                
            await RunConcurrentOperations(connectionManager);
            
            // Capture multiplexing metrics
            _channelDistribution = connectionManager.Metrics.ChannelDistributionPercentages;
            _timeToFirstByte = connectionManager.Metrics.AverageTotalCallTime;
        }

        [Benchmark(Description = "MultiplexedChannel-Fixed16")]
        public async Task MultiplexedChannelFixed16()
        {
            // Fixed configuration with 16 channels
            int channelCount = 16;
            int maxConcurrentCallsPerChannel = Math.Max(10, ConcurrentConnections / channelCount);
            
            using var connectionManager = new MultiplexedChannelManager(
                ServerEndpoint, 
                channelCount, 
                maxConcurrentCallsPerChannel);
                
            await RunConcurrentOperations(connectionManager);
            
            // Capture multiplexing metrics
            _channelDistribution = connectionManager.Metrics.ChannelDistributionPercentages;
            _timeToFirstByte = connectionManager.Metrics.AverageTotalCallTime;
        }

        [Benchmark(Description = "MultiplexedChannel-Dynamic", Baseline = true)]
        public async Task MultiplexedChannelDynamic()
        {
            // Dynamic configuration that scales with concurrency
            // For very high concurrency, use more channels
            int channelCount = Math.Min(ConcurrentConnections / 20, 32);
            if (channelCount < 4) channelCount = 4;
            
            int maxConcurrentCallsPerChannel = Math.Max(20, ConcurrentConnections / channelCount * 2);
            
            using var connectionManager = new MultiplexedChannelManager(
                ServerEndpoint, 
                channelCount, 
                maxConcurrentCallsPerChannel);
                
            await RunConcurrentOperations(connectionManager);
            
            // Capture multiplexing metrics
            _channelDistribution = connectionManager.Metrics.ChannelDistributionPercentages;
            _timeToFirstByte = connectionManager.Metrics.AverageTotalCallTime;
            
            // Log the configuration used
            Console.WriteLine($"Dynamic config used {channelCount} channels with {maxConcurrentCallsPerChannel} max concurrent calls per channel");
        }

        private async Task RunConcurrentOperations(IGrpcConnectionManager connectionManager)
        {
            var stopwatch = Stopwatch.StartNew();
            var tasks = new List<Task>(ConcurrentConnections);
            int successCount = 0;
            int errorCount = 0;
            
            // Create a resilient client like in ConnectionManagerBenchmarks
            var resilientClient = connectionManager.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
            
            using var semaphore = new SemaphoreSlim(ConcurrentConnections);
            var random = new Random();
            
            Console.WriteLine($"Starting benchmark with {MessageCount} messages, {ConcurrentConnections} concurrent connections");
            
            for (int i = 0; i < MessageCount; i++)
            {
                await semaphore.WaitAsync();
                
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        // Parse the string ID to an integer
                        int numericId = int.Parse(_testIds[random.Next(_testIds.Length)]);
                        
                        // Cast the int to ulong when setting the Fid property
                        var fidRequest = new FidRequest { Fid = (ulong)numericId };
                        
                        // Use the resilient client to call the real service
                        var response = await resilientClient.CallAsync(
                            (client, ct) => client.GetUserDataByFidAsync(fidRequest, cancellationToken: ct).ResponseAsync,
                            "GetUserDataByFid");
                        
                        // Just increment success count without logging samples
                        Interlocked.Increment(ref successCount);
                    }
                    catch (Exception ex)
                    {
                        // Only log error type and message, not every single error
                        if (Interlocked.Increment(ref errorCount) <= 5)
                        {
                            Console.WriteLine($"Error in benchmark: {ex.GetType().Name}: {ex.Message}");
                        }
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
                
                // If we've hit our concurrency limit, wait for at least one task to complete
                if (tasks.Count >= ConcurrentConnections)
                {
                    await Task.WhenAny(tasks);
                    tasks.RemoveAll(t => t.IsCompleted);
                }
            }
            
            // Wait for all remaining tasks to complete
            await Task.WhenAll(tasks);
            
            stopwatch.Stop();
            double messagesPerSecond = MessageCount / stopwatch.Elapsed.TotalSeconds;
            
            Console.WriteLine($"Completed benchmark: {messagesPerSecond:F2} msgs/sec, Success: {successCount}, Failed: {errorCount}, Duration: {stopwatch.Elapsed.TotalSeconds:F2}s");
        }
        
        [IterationCleanup]
        public void IterationCleanup()
        {
            // Force garbage collection between runs to minimize interference
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
        }
    }
} 