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

namespace HubClient.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class ConnectionManagerBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293"; // Update with the actual server endpoint
        
        // UPDATED: Changed to much larger message counts for extreme testing
        [Params(5000, 10000, 25000)]
        public int MessageCount { get; set; }
        
        // UPDATED: Higher concurrency levels
        [Params(20, 50, 100)]
        public int ConcurrentConnections { get; set; }

        // Stats for all benchmarks
        private double _messagesPerSecond;
        private TimeSpan _establishmentTime;
        private TimeSpan _timeToFirstByte;
        private double[] _channelDistribution = Array.Empty<double>();
        private int _failedCalls;
        private int _successfulCalls;

        // Removing the baseline benchmark since it's too slow (80 req/s vs 1200 req/s for multiplexed)
        // [Benchmark(Baseline = true)]
        // public async Task BaselineConnectionManager()
        // {
        //     using var connectionManager = new BaselineGrpcConnectionManager(ServerEndpoint);
        //     await RunConcurrentOperations(connectionManager);
        // }

        [Benchmark(Baseline = true)]
        public async Task OptimizedConnectionManager()
        {
            using var connectionManager = new OptimizedGrpcConnectionManager(ServerEndpoint, ConcurrentConnections);
            await RunConcurrentOperations(connectionManager);
        }

        [Benchmark]
        public async Task MultiplexedChannelManager()
        {
            // Determine optimal number of channels based on concurrent connections
            int channelCount = Math.Max(4, Math.Min(ConcurrentConnections / 5, 16));
            int maxConcurrentCallsPerChannel = Math.Max(10, ConcurrentConnections / channelCount * 2);
            
            using var connectionManager = new MultiplexedChannelManager(ServerEndpoint, channelCount, maxConcurrentCallsPerChannel);
            await RunConcurrentOperations(connectionManager);
            
            // Capture multiplexing metrics
            _channelDistribution = connectionManager.Metrics.ChannelDistributionPercentages;
            _timeToFirstByte = connectionManager.Metrics.AverageTotalCallTime;
        }

        private async Task RunConcurrentOperations(IGrpcConnectionManager connectionManager)
        {
            // Time the entire operation for throughput calculation
            var stopwatch = Stopwatch.StartNew();
            
            // Use a resilient client instead of a regular one
            var resilientClient = connectionManager.CreateResilientClient<MinimalHubService.MinimalHubServiceClient>();
            
            // Create a list of tasks for concurrent execution
            var tasks = new List<Task>(ConcurrentConnections);
            
            // Set up a semaphore to control concurrency
            using var semaphore = new System.Threading.SemaphoreSlim(ConcurrentConnections);
            
            // Reset success/failure tracking
            _successfulCalls = 0;
            _failedCalls = 0;
            
            for (int i = 0; i < MessageCount; i++)
            {
                await semaphore.WaitAsync();
                
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        // Use a fixed FID for testing
                        var fidRequest = new FidRequest { Fid = 24 };
                        
                        // Time the actual gRPC call
                        var callStopwatch = Stopwatch.StartNew();
                        
                        // Use the resilient client's CallAsync method
                        var response = await resilientClient.CallAsync(
                            (client, ct) => client.GetUserDataByFidAsync(fidRequest, cancellationToken: ct).ResponseAsync,
                            "GetUserDataByFid");
                            
                        callStopwatch.Stop();
                        
                        // Record TTFB if connection manager supports it
                        if (connectionManager is DirectChannelManager directManager)
                        {
                            directManager.ConnectionStats.RecordTimeToFirstByte(callStopwatch.Elapsed);
                        }
                        
                        // Simple validation to ensure the call worked
                        if (response == null)
                        {
                            throw new InvalidOperationException("Response was null");
                        }
                        
                        // Track success
                        System.Threading.Interlocked.Increment(ref _successfulCalls);
                    }
                    catch (Exception ex)
                    {
                        // Track failure, but don't let it fail the benchmark
                        System.Threading.Interlocked.Increment(ref _failedCalls);
                        Console.WriteLine($"Error in operation: {ex.Message}");
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
            
            // Calculate messages per second
            _messagesPerSecond = MessageCount / stopwatch.Elapsed.TotalSeconds;
        }
        
        // Properties to expose metrics in benchmark results
        [IterationSetup]
        public void IterationSetup()
        {
            _messagesPerSecond = 0;
            _establishmentTime = TimeSpan.Zero;
            _timeToFirstByte = TimeSpan.Zero;
            _channelDistribution = Array.Empty<double>();
            _failedCalls = 0;
            _successfulCalls = 0;
        }
        
        // Output metrics using custom columns
        [IterationCleanup]
        public void IterationCleanup()
        {
            // These values will be available in the benchmark logs
            Console.WriteLine($"Messages/sec: {_messagesPerSecond:F2}");
            Console.WriteLine($"Avg establishment time: {_establishmentTime.TotalMilliseconds:F2} ms");
            Console.WriteLine($"Avg time to first byte: {_timeToFirstByte.TotalMilliseconds:F2} ms");
            Console.WriteLine($"Successful calls: {_successfulCalls}, Failed calls: {_failedCalls}");
            
            if (_channelDistribution != null)
            {
                Console.WriteLine($"Channel distribution: {string.Join(", ", _channelDistribution.Select(p => $"{p:F2}%"))}");
            }
        }
    }
} 