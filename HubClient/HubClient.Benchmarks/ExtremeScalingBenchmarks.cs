using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using HubClient.Core;
using HubClient.Core.Grpc;
using HubClient.Core.Resilience;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class ExtremeScalingBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        // Fixed message count for consistency
        [Params(25000)]
        public int MessageCount { get; set; }
        
        // Higher concurrency levels to stress test
        [Params(1000, 2500, 5000, 7500, 10000)]
        public int ConcurrentConnections { get; set; }

        // Test ids for consistent usage - simple numeric strings
        private string[] _testIds;
        
        // Stats tracking
        private double _messagesPerSecond;
        private TimeSpan _timeToFirstByte;
        private double[] _channelDistribution = Array.Empty<double>();
        private int _failedCalls;
        private int _successfulCalls;

        [GlobalSetup]
        public void Setup()
        {
            Console.WriteLine($"Setting up benchmark with {ConcurrentConnections} concurrent connections");
            
            // Create a range of simple numeric Fids
            _testIds = new string[50000];
            for (int i = 0; i < _testIds.Length; i++)
            {
                _testIds[i] = (i + 1).ToString();
            }
            
            Console.WriteLine("Setup complete - connecting to real server");
            
            // Minimal connection test output
            try
            {
                using var testConnectionManager = new MultiplexedChannelManager(ServerEndpoint, 4);
                var resilientClient = testConnectionManager.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
                var fidRequest = new FidRequest { Fid = 24UL };
                
                var response = resilientClient.CallAsync(
                    (client, ct) => client.GetUserDataByFidAsync(fidRequest, cancellationToken: ct).ResponseAsync,
                    "GetUserDataByFid").Result;
                    
                Console.WriteLine("Server connection verified successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WARNING: Server connection failed: {ex.Message}");
                Console.WriteLine($"Exception details: {ex}");
            }
        }

        [Benchmark(Description = "DynamicMultiplexer")]
        public async Task DynamicMultiplexer()
        {
            // Calculate appropriate channel count based on concurrency
            int channelCount = Math.Min(ConcurrentConnections / 20, 100);
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
            
            // Create a resilient client like in ExtremeConcurrencyBenchmarks
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