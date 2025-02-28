using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using Grpc.Core;
using Grpc.Net.Client;
using HubClient.Core;
using HubClient.Core.Resilience;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Benchmarks
{
    /// <summary>
    /// Benchmarks the resilience capabilities of the different connection manager approaches
    /// </summary>
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    public class ResilienceBenchmarks
    {
        private const string ServerEndpoint = "http://localhost:5293";
        
        // Parameters for the benchmark
        [Params(10, 100, 1000)]
        public int ConcurrentOperations { get; set; }
        
        [Params(0.05, 0.10, 0.20)]
        public double ErrorRate { get; set; }
        
        // Connection managers
        // private BaselineGrpcConnectionManager _baselineManager; // Removed - too slow (80 req/s vs 1200 req/s)
        private OptimizedGrpcConnectionManager _optimizedManager;
        private DirectChannelManager _directManager;
        private MultiplexedChannelManager _multiplexedManager;
        
        // Test service clients
        // private MockGrpcService.MockGrpcServiceClient _baselineClient; // Removed - too slow
        // private ResilientGrpcClient<MockGrpcService.MockGrpcServiceClient> _baselineResilientClient; // Removed - too slow
        private ResilientGrpcClient<MockGrpcService.MockGrpcServiceClient> _optimizedResilientClient;
        private ResilientGrpcClient<MockGrpcService.MockGrpcServiceClient> _directResilientClient;
        private ResilientMultiplexingClient<MockGrpcService.MockGrpcServiceClient> _multiplexedResilientClient;
        
        // Mock random IDs to request
        private string[] _testIds;
        
        [GlobalSetup]
        public void Setup()
        {
            // Configure mock IDs to use for testing
            _testIds = Enumerable.Range(0, 1000)
                .Select(_ => Guid.NewGuid().ToString())
                .ToArray();
                
            // Initialize connection managers
            // _baselineManager = new BaselineGrpcConnectionManager(ServerEndpoint); // Removed - too slow
            _optimizedManager = new OptimizedGrpcConnectionManager(ServerEndpoint);
            _directManager = new DirectChannelManager(ServerEndpoint);
            _multiplexedManager = new MultiplexedChannelManager(ServerEndpoint);
            
            // Initialize clients
            // _baselineClient = _baselineManager.CreateClient<MockGrpcService.MockGrpcServiceClient>(); // Removed - too slow
            // _baselineResilientClient = _baselineManager.CreateResilientClient<MockGrpcService.MockGrpcServiceClient>(); // Removed - too slow
            _optimizedResilientClient = _optimizedManager.CreateResilientClient<MockGrpcService.MockGrpcServiceClient>();
            _directResilientClient = _directManager.CreateResilientClient<MockGrpcService.MockGrpcServiceClient>();
            
            // Initialize multiplexed client
            var clientFactory = new Func<Grpc.Net.Client.GrpcChannel, MockGrpcService.MockGrpcServiceClient>(
                channel => new MockGrpcService.MockGrpcServiceClient(channel));
            _multiplexedResilientClient = _multiplexedManager.CreateResilientMultiplexingClient(clientFactory);
        }
        
        [GlobalCleanup]
        public void Cleanup()
        {
            // Dispose all managers
            // _baselineManager.Dispose(); // Removed - too slow
            _optimizedManager.Dispose();
            _directManager.Dispose();
            _multiplexedManager.Dispose();
        }
        
        // [Benchmark(Baseline = true, Description = "No resilience")]
        // [BenchmarkCategory("Resilience")]
        // public async Task BaselineWithoutResilience()
        // {
        //     // Run operations without resilience
        //     try
        //     {
        //         await RunConcurrentOperations(
        //             id => _baselineClient.GetUserDataByFidAsync(
        //                 new MockGrpcService.UserDataRequest { Fid = id, SimulatedErrorRate = ErrorRate }));
        //     }
        //     catch (RpcException)
        //     {
        //         // Expected some failures without resilience
        //     }
        // }
        
        // [Benchmark(Description = "Baseline with resilience")]
        // [BenchmarkCategory("Resilience")]
        // public async Task BaselineWithResilience()
        // {
        //     // Run operations with baseline resilience
        //     await RunConcurrentOperations(
        //         id => _baselineResilientClient.CallAsync(
        //             (client, ct) => client.GetUserDataByFidAsync(
        //                 new MockGrpcService.UserDataRequest { Fid = id, SimulatedErrorRate = ErrorRate },
        //                 cancellationToken: ct),
        //             $"GetUserData-{id}"));
        // }
        
        [Benchmark(Baseline = true, Description = "Optimized with resilience")]
        [BenchmarkCategory("Resilience")]
        public async Task OptimizedWithResilience()
        {
            // Run operations with optimized resilience
            await RunConcurrentOperations(
                id => _optimizedResilientClient.CallAsync(
                    (client, ct) => client.GetUserDataByFidAsync(
                        new MockGrpcService.UserDataRequest { Fid = id, SimulatedErrorRate = ErrorRate },
                        cancellationToken: ct),
                    $"GetUserData-{id}"));
        }
        
        [Benchmark(Description = "Direct with resilience")]
        [BenchmarkCategory("Resilience")]
        public async Task DirectWithResilience()
        {
            // Run operations with direct channel resilience
            await RunConcurrentOperations(
                id => _directResilientClient.CallAsync(
                    (client, ct) => client.GetUserDataByFidAsync(
                        new MockGrpcService.UserDataRequest { Fid = id, SimulatedErrorRate = ErrorRate },
                        cancellationToken: ct),
                    $"GetUserData-{id}"));
        }
        
        [Benchmark(Description = "Multiplexed with resilience")]
        [BenchmarkCategory("Resilience")]
        public async Task MultiplexedWithResilience()
        {
            // Run operations with multiplexed resilience
            await RunConcurrentOperations(
                id => _multiplexedResilientClient.CallAsync(
                    client => client.GetUserDataByFidAsync(
                        new MockGrpcService.UserDataRequest { Fid = id, SimulatedErrorRate = ErrorRate }),
                    $"GetUserData-{id}"));
        }
        
        /// <summary>
        /// Helper method to run concurrent operations
        /// </summary>
        private async Task RunConcurrentOperations<TResponse>(Func<string, Task<TResponse>> operation)
        {
            // Create a random number generator for selecting IDs
            var random = new Random();
            
            // Create tasks for concurrent operations
            var tasks = new Task<TResponse>[ConcurrentOperations];
            for (int i = 0; i < ConcurrentOperations; i++)
            {
                // Select a random ID
                var id = _testIds[random.Next(_testIds.Length)];
                
                // Start the operation
                tasks[i] = operation(id);
            }
            
            // Wait for all operations to complete
            // Note: With resilience, we expect all operations to complete successfully
            //       Without resilience, some operations will fail
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (AggregateException ex)
            {
                // Count the number of failed operations
                var failedCount = ex.InnerExceptions.Count;
                
                // For non-resilient benchmarks, we expect failures
                // This prevents the benchmark from failing completely
            }
        }
    }
    
    /// <summary>
    /// Mock gRPC service client for benchmarking
    /// This would be generated from your actual .proto file
    /// </summary>
    public static class MockGrpcService
    {
        public class UserDataRequest
        {
            public required string Fid { get; set; }
            public double SimulatedErrorRate { get; set; }
        }
        
        public class UserDataResponse
        {
            public required string Fid { get; set; }
            public required string DisplayName { get; set; }
            public required string Data { get; set; }
        }
        
        public class MockGrpcServiceClient
        {
            private readonly Random _random = new Random();
            private static bool _loggedInfo = false;
            
            public MockGrpcServiceClient(GrpcChannel channel)
            {
                // Log once that we're using the mock client
                if (!_loggedInfo)
                {
                    Console.WriteLine($"MockGrpcServiceClient initialized with URL: {channel.Target}");
                    _loggedInfo = true;
                }
            }
            
            public Task<UserDataResponse> GetUserDataByFidAsync(UserDataRequest request, CancellationToken cancellationToken = default)
            {
                // Log every 10000th request to show we're processing
                if (new Random().Next(10000) == 0)
                {
                    Console.WriteLine($"Processing request for FID: {request.Fid}");
                }
                
                // Simulate error rate
                if (_random.NextDouble() < request.SimulatedErrorRate)
                {
                    throw new RpcException(new Status(StatusCode.Unavailable, "Simulated error"));
                }

                string hash;
                
                // Handle any Fid format - simple numeric or GUID
                if (request.Fid.Length > 8)
                {
                    hash = request.Fid.Substring(0, 8);
                }
                else
                {
                    hash = request.Fid;
                }

                return Task.FromResult(new UserDataResponse
                {
                    Fid = request.Fid,
                    DisplayName = $"User {hash}",
                    Data = $"Data for {hash}"
                });
            }
            
            public Task<UserDataResponse> GetUserDataByFidAsync(UserDataRequest request, CallOptions options)
            {
                return GetUserDataByFidAsync(request, options.CancellationToken);
            }
        }
    }
} 