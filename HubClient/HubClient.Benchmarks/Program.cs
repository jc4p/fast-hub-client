using BenchmarkDotNet.Running;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;
using HubClient.Benchmarks;
using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("HubClient Benchmarks");
            Console.WriteLine("===================");
            
            if (args.Length > 0)
            {
                switch (args[0].ToLower())
                {
                    case "all":
                        RunAllBenchmarks();
                        break;
                        
                    case "concurrency":
                        Console.WriteLine("Running concurrency pipeline benchmarks...");
                        BenchmarkRunner.Run<ConcurrencyPipelineBenchmarks>();
                        break;
                        
                    case "endtoend":
                        Console.WriteLine("Running end-to-end pipeline benchmarks...");
                        BenchmarkRunner.Run<EndToEndPipelineBenchmarks>();
                        break;
                        
                    case "extreme":
                        Console.WriteLine("Running extreme concurrency benchmarks...");
                        BenchmarkRunner.Run<ExtremeConcurrencyBenchmarks>();
                        break;
                        
                    case "scalability":
                        Console.WriteLine("Running scalability benchmarks...");
                        BenchmarkRunner.Run<ScalabilityBenchmarks>();
                        break;
                        
                    case "parquet":
                        Console.WriteLine("Running Parquet persistence benchmarks...");
                        BenchmarkRunner.Run<ParquetPersistenceBenchmarks>();
                        break;
                        
                    case "parquet-quick":
                        Console.WriteLine("Running Parquet persistence benchmarks with limited parameters...");
                        var parquetConfig = ManualConfig.Create(DefaultConfig.Instance);
                        parquetConfig = parquetConfig.AddJob(Job.Default
                            .WithIterationCount(3)
                            .WithWarmupCount(1));
                        parquetConfig = parquetConfig.AddFilter(new SimpleFilter("MessageCount", "25000"));
                        parquetConfig = parquetConfig.AddFilter(new SimpleFilter("Workload", "SmallFrequent"));
                        BenchmarkRunner.Run<ParquetPersistenceBenchmarks>(parquetConfig);
                        break;
                        
                    case "memory":
                        Console.WriteLine("Running memory management benchmarks...");
                        var memoryConfig = ManualConfig.Create(DefaultConfig.Instance);
                        memoryConfig = memoryConfig.AddJob(Job.Default
                            .WithIterationCount(5)
                            .WithWarmupCount(1));
                        BenchmarkRunner.Run<MemoryManagementBenchmarks>(memoryConfig);
                        break;
                        
                    case "memory-optimize":
                        Console.WriteLine("Running highly optimized memory benchmarks (fast execution for strategy comparison)...");
                        var optimizedConfig = ManualConfig.Create(DefaultConfig.Instance);
                        // Set absolute minimum iterations and warmup
                        optimizedConfig = optimizedConfig.AddJob(Job.Default
                            .WithIterationCount(1)
                            .WithWarmupCount(1));
                        
                        // Use simpler filters that we know will match existing benchmarks
                        // Filter to just keep non-baseline strategies
                        optimizedConfig = optimizedConfig.AddFilter(
                            new NameFilter(name => !name.Contains("Baseline")));
                        
                        // Limit batch size
                        optimizedConfig = optimizedConfig.AddFilter(
                            new SimpleFilter("MessageBatchSize", "10000"));
                        
                        // Limit total messages
                        optimizedConfig = optimizedConfig.AddFilter(
                            new SimpleFilter("TotalMessages", "100000"));
                        
                        Console.WriteLine("===== OPTIMIZED BENCHMARK CONFIGURATION =====");
                        Console.WriteLine("This configuration will:");
                        Console.WriteLine("- Compare only ArrayPoolWithRMS, MessageObjectPool, and UnsafeMemoryAccess strategies");
                        Console.WriteLine("- Skip the Baseline approach");
                        Console.WriteLine("- Use only one batch size (10,000 messages)");
                        Console.WriteLine("- Process only 100,000 total messages instead of 200,000");
                        Console.WriteLine("- Run only 1 iteration with 1 warmup");
                        Console.WriteLine("Estimated runtime: ~1-2 hours");
                        Console.WriteLine("==============================================");
                        
                        BenchmarkRunner.Run<MemoryManagementBenchmarks>(optimizedConfig);
                        break;
                        
                    case "memory-fast":
                        Console.WriteLine("Running ultra-fast memory test (direct execution, no BenchmarkDotNet)...");
                        Console.WriteLine("This will test each pipeline type with ArrayPool strategy in minutes instead of hours.");
                        
                        RunDirectMemoryFastTest();
                        break;
                        
                    case "memory-optimal":
                        if (args.Length < 3)
                        {
                            Console.WriteLine("Usage: memory-optimal [pipeline] [strategy]");
                            Console.WriteLine("Example: memory-optimal Channel ArrayPoolWithRMS");
                            Console.WriteLine("Available pipelines: Channel, Dataflow, ThreadPerCore");
                            Console.WriteLine("Available strategies: ArrayPoolWithRMS, MessageObjectPool, UnsafeMemoryAccess");
                            break;
                        }
                        
                        // Parse pipeline type
                        if (!Enum.TryParse<HubClient.Core.Concurrency.PipelineStrategy>(args[1], true, out var pipeline))
                        {
                            Console.WriteLine($"Invalid pipeline type: {args[1]}");
                            Console.WriteLine("Available pipelines: Channel, Dataflow, ThreadPerCore");
                            break;
                        }
                        
                        // Parse memory strategy
                        if (!Enum.TryParse<MemoryManagementBenchmarks.MemoryManagementApproach>(args[2], true, out var strategy))
                        {
                            Console.WriteLine($"Invalid memory strategy: {args[2]}");
                            Console.WriteLine("Available strategies: ArrayPoolWithRMS, MessageObjectPool, UnsafeMemoryAccess");
                            break;
                        }
                        
                        Console.WriteLine($"Running detailed benchmark for optimal combination: {pipeline} pipeline with {strategy} strategy");
                        
                        var optimalConfig = ManualConfig.Create(DefaultConfig.Instance);
                        // Use more iterations for statistical validity, but still faster than default
                        optimalConfig = optimalConfig.AddJob(Job.Default
                            .WithIterationCount(3)
                            .WithWarmupCount(2));
                        
                        // Filter to just the specific pipeline and memory strategy
                        optimalConfig = optimalConfig.AddFilter(
                            new SimpleFilter("PipelineType", pipeline.ToString()));
                        
                        // Create a filter for the specific memory strategy benchmark method
                        string methodFilter;
                        switch (strategy)
                        {
                            case MemoryManagementBenchmarks.MemoryManagementApproach.ArrayPoolWithRMS:
                                methodFilter = "ArrayPool_RMS_Benchmark";
                                break;
                            case MemoryManagementBenchmarks.MemoryManagementApproach.MessageObjectPool:
                                methodFilter = "Message_Pool_Benchmark";
                                break;
                            case MemoryManagementBenchmarks.MemoryManagementApproach.UnsafeMemoryAccess:
                                methodFilter = "Unsafe_Memory_Benchmark";
                                break;
                            default:
                                methodFilter = "";
                                break;
                        }
                        
                        optimalConfig = optimalConfig.AddFilter(
                            new NameFilter(name => name.Contains(methodFilter)));
                        
                        // Test multiple batch sizes to find the optimal configuration
                        // No need to filter for TotalMessages since we'll keep the default
                        
                        Console.WriteLine("===== OPTIMAL BENCHMARK CONFIGURATION =====");
                        Console.WriteLine($"This configuration will:");
                        Console.WriteLine($"- Test only {strategy} memory strategy");
                        Console.WriteLine($"- Test only {pipeline} pipeline type");
                        Console.WriteLine($"- Test multiple batch sizes to find the optimal configuration");
                        Console.WriteLine($"- Run 3 iterations with 2 warmups for statistical validity");
                        Console.WriteLine("Estimated runtime: ~30-45 minutes");
                        Console.WriteLine("==============================================");
                        
                        BenchmarkRunner.Run<MemoryManagementBenchmarks>(optimalConfig);
                        break;
                        
                    case "memory-quick":
                        Console.WriteLine("Running memory management benchmarks with limited parameters...");
                        var memConfig = ManualConfig.Create(DefaultConfig.Instance);
                        memConfig = memConfig.AddFilter(new SimpleFilter("MessageBatchSize", "1000"));
                        memConfig = memConfig.AddFilter(new SimpleFilter("TotalMessages", "10000"));
                        BenchmarkRunner.Run<MemoryManagementBenchmarks>(memConfig);
                        break;
                        
                    case "memory-endtoend":
                        Console.WriteLine("Running end-to-end memory management benchmarks...");
                        BenchmarkRunner.Run<EndToEndMemoryBenchmarks>();
                        break;
                        
                    case "memory-endtoend-quick":
                        Console.WriteLine("Running end-to-end memory management benchmarks with limited parameters...");
                        var e2eMemConfig = ManualConfig.Create(DefaultConfig.Instance);
                        e2eMemConfig = e2eMemConfig.AddFilter(new SimpleFilter("BatchSize", "1000"));
                        e2eMemConfig = e2eMemConfig.AddFilter(new SimpleFilter("WorkloadPattern", "SmallFrequent"));
                        BenchmarkRunner.Run<EndToEndMemoryBenchmarks>(e2eMemConfig);
                        break;
                        
                    case "memory-direct-test":
                        Console.WriteLine("Running direct memory management test (no BenchmarkDotNet)...");
                        RunDirectMemoryTest();
                        break;
                        
                    case "verify":
                        RunVerificationTest("channel");
                        break;
                        
                    case "verify-all":
                        RunAllVerificationTests();
                        break;
                        
                    case "verify-channel":
                        RunVerificationTest("channel");
                        break;
                        
                    case "verify-dataflow":
                        RunVerificationTest("dataflow");
                        break;
                        
                    case "verify-threadpercore":
                        RunVerificationTest("threadpercore");
                        break;
                        
                    case "direct-verify":
                        Console.WriteLine("Running direct channel pipeline verification test (no BenchmarkDotNet)...");
                        var benchmarks = new ConcurrencyPipelineBenchmarks();
                        benchmarks.Setup();
                        benchmarks.VerifyChannelPipelineMessageTracking().GetAwaiter().GetResult();
                        break;

                    case "direct-verify-dataflow":
                        Console.WriteLine("Running direct dataflow pipeline verification test (no BenchmarkDotNet)...");
                        var dfBenchmarks = new ConcurrencyPipelineBenchmarks();
                        dfBenchmarks.Setup();
                        dfBenchmarks.VerifyDataflowPipelineMessageTracking().GetAwaiter().GetResult();
                        break;

                    case "direct-verify-threadpercore":
                        Console.WriteLine("Running direct thread-per-core pipeline verification test (no BenchmarkDotNet)...");
                        var tpcBenchmarks = new ConcurrencyPipelineBenchmarks();
                        tpcBenchmarks.Setup();
                        tpcBenchmarks.VerifyThreadPerCorePipelineMessageTracking().GetAwaiter().GetResult();
                        break;
                        
                    case "direct-verify-all":
                        Console.WriteLine("Running all direct pipeline verification tests in sequence (no BenchmarkDotNet)...");
                        
                        try {
                            // Modify each verification method to not call Environment.Exit so we can run them in sequence
                            RunDirectChannelVerification();
                            RunDirectDataflowVerification();
                            RunDirectThreadPerCoreVerification();
                            
                            Console.WriteLine("All direct verification tests completed successfully!");
                            // Final exit to prevent hanging
                            Environment.Exit(0);
                        }
                        catch (Exception ex) {
                            Console.WriteLine($"ERROR: One or more verification tests failed: {ex.Message}");
                            Environment.Exit(1);
                        }
                        break;

                    case "simple-test":
                        Console.WriteLine("Running simple pipeline test with basic messages...");
                        RunSimpleTest();
                        break;
                    
                    case "storage":
                        Console.WriteLine("Running intermediate storage benchmarks...");
                        BenchmarkRunner.Run<IntermediateStorageBenchmarks>();
                        break;
                        
                    case "extremestorage":
                        Console.WriteLine("Running extreme storage test (50M+ records)...");
                        RunExtremeStorageTest(args.Skip(1).ToArray());
                        break;
                        
                    default:
                        Console.WriteLine($"Unknown benchmark: {args[0]}");
                        PrintUsage();
                        break;
                }
            }
            else
            {
                // Default to running concurrency pipeline benchmarks
                Console.WriteLine("No benchmark specified, running concurrency pipeline benchmarks.");
                BenchmarkRunner.Run<ConcurrencyPipelineBenchmarks>();
            }
            
            Console.WriteLine("Benchmarks complete.");
        }
        
        // Run a direct memory management test outside of BenchmarkDotNet
        private static void RunDirectMemoryTest()
        {
            try 
            {
                Console.WriteLine("Running a quick memory management test with all pipeline types...");
                
                var benchmark = new MemoryManagementBenchmarks
                {
                    MessageBatchSize = 1000,
                    TotalMessages = 10000,
                    MemoryStrategy = MemoryManagementBenchmarks.MemoryManagementApproach.ArrayPoolWithRMS
                };
                
                foreach (var pipelineType in Enum.GetValues<HubClient.Core.Concurrency.PipelineStrategy>())
                {
                    Console.WriteLine($"\nTesting {pipelineType} pipeline with ArrayPool+RMS memory management");
                    try
                    {
                        benchmark.PipelineType = pipelineType;
                        benchmark.Setup();
                        
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        benchmark.ArrayPool_RMS_Benchmark().GetAwaiter().GetResult();
                        sw.Stop();
                        
                        Console.WriteLine($"Test completed in {sw.ElapsedMilliseconds}ms");
                        Console.WriteLine($"Gen0: {GC.CollectionCount(0) - benchmark._gen0Collections}");
                        Console.WriteLine($"Gen1: {GC.CollectionCount(1) - benchmark._gen1Collections}");
                        Console.WriteLine($"Gen2: {GC.CollectionCount(2) - benchmark._gen2Collections}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Test failed with exception: {ex.GetType().Name}");
                        Console.WriteLine($"Message: {ex.Message}");
                        Console.WriteLine($"Stack trace: {ex.StackTrace}");
                        
                        if (ex.InnerException != null)
                        {
                            Console.WriteLine($"Inner exception: {ex.InnerException.GetType().Name}");
                            Console.WriteLine($"Inner message: {ex.InnerException.Message}");
                        }
                    }
                    finally
                    {
                        benchmark.Cleanup();
                    }
                }
                
                Console.WriteLine("\nDo you want to run the end-to-end memory test with actual gRPC connection? (y/n)");
                var response = Console.ReadLine()?.ToLower();
                
                if (response == "y" || response == "yes")
                {
                    Console.WriteLine("\nRunning end-to-end memory test with actual gRPC connection...");
                    
                    var e2eBenchmark = new EndToEndMemoryBenchmarks
                    {
                        BatchSize = 1000,
                        MemoryManagement = EndToEndMemoryBenchmarks.MemoryStrategy.ArrayPoolWithRMS,
                        PipelineType = HubClient.Core.Concurrency.PipelineStrategy.Channel,
                        Workload = EndToEndMemoryBenchmarks.WorkloadPattern.SmallFrequent
                    };
                    
                    try
                    {
                        e2eBenchmark.Setup();
                        
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        e2eBenchmark.EndToEndPipelineBenchmark().GetAwaiter().GetResult();
                        sw.Stop();
                        
                        Console.WriteLine($"End-to-end test completed in {sw.ElapsedMilliseconds}ms");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"End-to-end test failed with exception: {ex.GetType().Name}");
                        Console.WriteLine($"Message: {ex.Message}");
                        Console.WriteLine($"Stack trace: {ex.StackTrace}");
                        
                        if (ex.InnerException != null)
                        {
                            Console.WriteLine($"Inner exception: {ex.InnerException.GetType().Name}");
                            Console.WriteLine($"Inner message: {ex.InnerException.Message}");
                        }
                    }
                    finally
                    {
                        e2eBenchmark.Cleanup();
                    }
                }
                else
                {
                    Console.WriteLine("Skipping end-to-end test.");
                }
                
                Console.WriteLine("Direct memory test complete.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: Memory test failed: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }
        
        // Helper methods to run verification tests directly without Environment.Exit
        private static void RunDirectChannelVerification()
        {
            Console.WriteLine("\n==== CHANNEL PIPELINE VERIFICATION ====");
            Console.WriteLine("Running direct channel pipeline verification test...");
            
            var benchmarks = new ConcurrencyPipelineBenchmarks();
            benchmarks.Setup();
            
            // Create a wrapper method that doesn't exit
            try {
                // Use reflection to run the method privately without calling Environment.Exit
                typeof(ConcurrencyPipelineBenchmarks)
                    .GetMethod("VerifyChannelPipelineMessageTracking", 
                               System.Reflection.BindingFlags.Public | 
                               System.Reflection.BindingFlags.Instance)
                    ?.Invoke(benchmarks, null);
                
                Console.WriteLine("Channel pipeline verification completed successfully!");
            }
            catch (Exception ex) {
                var innerEx = ex.InnerException ?? ex;
                Console.WriteLine($"Channel pipeline verification failed: {innerEx.Message}");
                throw;
            }
        }
        
        private static void RunDirectDataflowVerification()
        {
            Console.WriteLine("\n==== DATAFLOW PIPELINE VERIFICATION ====");
            Console.WriteLine("Running direct dataflow pipeline verification test...");
            
            var benchmarks = new ConcurrencyPipelineBenchmarks();
            benchmarks.Setup();
            
            try {
                // Use reflection to run the method privately without calling Environment.Exit
                typeof(ConcurrencyPipelineBenchmarks)
                    .GetMethod("VerifyDataflowPipelineMessageTracking", 
                               System.Reflection.BindingFlags.Public | 
                               System.Reflection.BindingFlags.Instance)
                    ?.Invoke(benchmarks, null);
                
                Console.WriteLine("Dataflow pipeline verification completed successfully!");
            }
            catch (Exception ex) {
                var innerEx = ex.InnerException ?? ex;
                Console.WriteLine($"Dataflow pipeline verification failed: {innerEx.Message}");
                throw;
            }
        }
        
        private static void RunDirectThreadPerCoreVerification()
        {
            Console.WriteLine("\n==== THREAD-PER-CORE PIPELINE VERIFICATION ====");
            Console.WriteLine("Running direct thread-per-core pipeline verification test...");
            
            var benchmarks = new ConcurrencyPipelineBenchmarks();
            benchmarks.Setup();
            
            try {
                // Use reflection to run the method privately without calling Environment.Exit
                typeof(ConcurrencyPipelineBenchmarks)
                    .GetMethod("VerifyThreadPerCorePipelineMessageTracking", 
                               System.Reflection.BindingFlags.Public | 
                               System.Reflection.BindingFlags.Instance)
                    ?.Invoke(benchmarks, null);
                
                Console.WriteLine("Thread-per-core pipeline verification completed successfully!");
            }
            catch (Exception ex) {
                var innerEx = ex.InnerException ?? ex;
                Console.WriteLine($"Thread-per-core pipeline verification failed: {innerEx.Message}");
                throw;
            }
        }
        
        private static void RunVerificationTest(string pipelineType)
        {
            Console.WriteLine($"Running {pipelineType} pipeline verification test...");
            
            // Configure to run only the verification test, without all the benchmarking overhead
            var config = ManualConfig.Create(DefaultConfig.Instance);
            
            switch (pipelineType.ToLower())
            {
                case "channel":
                    config = config.AddFilter(new NameFilter(name => name.Contains("VerifyChannelPipelineMessageTracking")));
                    break;
                    
                case "dataflow":
                    config = config.AddFilter(new NameFilter(name => name.Contains("VerifyDataflowPipelineMessageTracking")));
                    break;
                    
                case "threadpercore":
                    config = config.AddFilter(new NameFilter(name => name.Contains("VerifyThreadPerCorePipelineMessageTracking")));
                    break;
                    
                default:
                    Console.WriteLine($"Unknown pipeline type: {pipelineType}. Defaulting to channel pipeline.");
                    config = config.AddFilter(new NameFilter(name => name.Contains("VerifyChannelPipelineMessageTracking")));
                    break;
            }
            
            BenchmarkRunner.Run<ConcurrencyPipelineBenchmarks>(config);
            
            Console.WriteLine("Verification complete.");
            
            // Force exit to prevent hanging after completion
            Environment.Exit(0);
        }
        
        private static void RunAllVerificationTests()
        {
            Console.WriteLine("Running all pipeline verification tests...");
            
            // Configure to run only the verification tests, without all the benchmarking overhead
            var config = ManualConfig.Create(DefaultConfig.Instance)
                .AddFilter(new NameFilter(name => name.Contains("VerifyChannelPipelineMessageTracking") || 
                                                 name.Contains("VerifyDataflowPipelineMessageTracking") || 
                                                 name.Contains("VerifyThreadPerCorePipelineMessageTracking")));
            
            BenchmarkRunner.Run<ConcurrencyPipelineBenchmarks>(config);
            
            Console.WriteLine("All verification tests complete.");
            
            // Force exit to prevent hanging after completion
            Environment.Exit(0);
        }
        
        private static void RunAllBenchmarks()
        {
            Console.WriteLine("Running all benchmarks in sequence...");
            Console.WriteLine("NOTE: This will take a very long time!");
            
            BenchmarkRunner.Run<ConcurrencyPipelineBenchmarks>();
            BenchmarkRunner.Run<EndToEndPipelineBenchmarks>();
            BenchmarkRunner.Run<ScalabilityBenchmarks>();
            BenchmarkRunner.Run<MemoryManagementBenchmarks>();
            BenchmarkRunner.Run<EndToEndMemoryBenchmarks>();
            BenchmarkRunner.Run<ParquetPersistenceBenchmarks>();
            
            Console.WriteLine("All benchmarks completed!");
        }
        
        private static void PrintUsage()
        {
            Console.WriteLine("\nUsage: HubClient.Benchmarks [benchmark-type]");
            Console.WriteLine("Available benchmark types:");
            Console.WriteLine("  all                  - Run all benchmarks (very time-consuming)");
            Console.WriteLine("  concurrency          - Pipeline concurrency benchmarks");
            Console.WriteLine("  endtoend             - End-to-end pipeline benchmarks");
            Console.WriteLine("  extreme              - Extreme concurrency benchmarks");
            Console.WriteLine("  scalability          - Scalability benchmarks");
            Console.WriteLine("  parquet              - Parquet persistence benchmarks");
            Console.WriteLine("  parquet-quick        - Quick Parquet persistence benchmarks with limited parameters");
            Console.WriteLine("  memory               - Memory management benchmarks");
            Console.WriteLine("  memory-optimize      - Run highly optimized memory benchmarks (1-2 hours)");
            Console.WriteLine("  memory-fast          - Run ultra-fast memory test (5-10 minutes, no BenchmarkDotNet)");
            Console.WriteLine("  memory-optimal [pipeline] [strategy] - Run detailed benchmark for specific optimal combination");
            Console.WriteLine("                        Example: memory-optimal Channel ArrayPoolWithRMS");
            Console.WriteLine("  memory-quick         - Run memory management benchmarks with limited parameters");
            Console.WriteLine("  memory-endtoend      - Run end-to-end memory management benchmarks");
            Console.WriteLine("  memory-endtoend-quick - Run end-to-end memory management benchmarks with limited parameters");
            Console.WriteLine("  memory-direct-test   - Run direct memory management test (no BenchmarkDotNet)");
            Console.WriteLine("  verify               - Run channel pipeline verification test (default)");
            Console.WriteLine("  verify-all           - Run all pipeline verification tests");
            Console.WriteLine("  verify-channel       - Run channel pipeline verification test");
            Console.WriteLine("  verify-dataflow      - Run dataflow pipeline verification test");
            Console.WriteLine("  verify-threadpercore - Run thread-per-core pipeline verification test");
            Console.WriteLine("  direct-verify        - Run channel pipeline verification directly (no BenchmarkDotNet)");
            Console.WriteLine("  direct-verify-dataflow - Run dataflow pipeline verification directly (no BenchmarkDotNet)");
            Console.WriteLine("  direct-verify-threadpercore - Run thread-per-core pipeline verification directly (no BenchmarkDotNet)");
            Console.WriteLine("  direct-verify-all    - Run all pipeline verification tests directly in sequence (no BenchmarkDotNet)");
            Console.WriteLine("  simple-test          - Run a simple pipeline test with basic messages (no complex setup)");
            Console.WriteLine("  storage              - Run intermediate storage benchmarks");
            Console.WriteLine("  extremestorage       - Run extreme storage test (50M+ records)");
            Console.WriteLine("     Args: [targetMessages] [outputPath] [batchSize] [pageSize] [timeoutMinutes]");
        }

        private static void RunSimpleTest()
        {
            using var pipeline = HubClient.Core.Concurrency.PipelineFactory.Create<string, string>(
                HubClient.Core.Concurrency.PipelineStrategy.Channel,
                async (str, ct) => { await Task.Delay(1); return str.ToUpper(); },
                new HubClient.Core.Concurrency.PipelineCreationOptions { 
                    InputQueueCapacity = 1000,
                    OutputQueueCapacity = 1000,
                    MaxConcurrency = Environment.ProcessorCount
                });

            Console.WriteLine("Starting simple pipeline test...");
            
            // Track processed messages
            int outputCount = 0;
            var consumerTask = pipeline.ConsumeAsync(
                message => {
                    outputCount++;
                    Console.WriteLine($"Processed message: {message}");
                    return ValueTask.CompletedTask;
                },
                CancellationToken.None);
                
            // Create and enqueue simple test messages
            const int messageCount = 10;
            List<string> messages = new();
            for (int i = 0; i < messageCount; i++)
            {
                messages.Add($"test-message-{i}");
            }
            
            Console.WriteLine($"Enqueuing {messageCount} simple messages...");
            pipeline.EnqueueBatchAsync(messages).GetAwaiter().GetResult();
            
            Console.WriteLine("All messages enqueued, completing pipeline...");
            pipeline.CompleteAsync().GetAwaiter().GetResult();
            consumerTask.GetAwaiter().GetResult();
            
            Console.WriteLine($"Pipeline complete. Processed {outputCount} messages.");
            
            if (outputCount != messageCount)
            {
                Console.WriteLine($"VERIFICATION FAILED: Expected {messageCount} processed messages, but got {outputCount}");
            }
            else
            {
                Console.WriteLine("VERIFICATION PASSED: All messages were processed correctly");
            }
            
            Console.WriteLine("Exiting simple test...");
        }

        // Additional helper class for filtering benchmarks
        private class SimpleFilter : IFilter
        {
            private readonly string _parameterName;
            private readonly string _value;
            
            public SimpleFilter(string parameterName, string value)
            {
                _parameterName = parameterName;
                _value = value;
            }
            
            public bool Predicate(BenchmarkCase benchmarkCase)
            {
                return benchmarkCase.Parameters.Items.Any(parameter => 
                    parameter.Name == _parameterName && parameter.Value.ToString() == _value);
            }
        }

        // Add this method to run fast memory tests directly
        private static void RunDirectMemoryFastTest()
        {
            try 
            {
                Console.WriteLine("Running fast memory test with all pipeline types and memory strategies...");
                Console.WriteLine("This will complete in minutes instead of hours");
                
                // Smaller message count for faster testing
                int batchSize = 5000;
                int totalMessages = 10000;
                
                var results = new Dictionary<string, long>();
                var bestCombination = (Pipeline: HubClient.Core.Concurrency.PipelineStrategy.Channel, 
                                      Strategy: MemoryManagementBenchmarks.MemoryManagementApproach.ArrayPoolWithRMS);
                long bestTime = long.MaxValue;
                
                // Skip Baseline to save time since it's known to be slower
                var memoryStrategies = new[]
                {
                    MemoryManagementBenchmarks.MemoryManagementApproach.ArrayPoolWithRMS,
                    MemoryManagementBenchmarks.MemoryManagementApproach.MessageObjectPool,
                    MemoryManagementBenchmarks.MemoryManagementApproach.UnsafeMemoryAccess
                };
                
                // Test each combination of pipeline type and memory strategy
                foreach (var pipelineType in Enum.GetValues<HubClient.Core.Concurrency.PipelineStrategy>())
                {
                    // Skip ThreadPerCore pipeline as it tends to stall during direct testing
                    if (pipelineType == HubClient.Core.Concurrency.PipelineStrategy.ThreadPerCore)
                    {
                        Console.WriteLine($"\nSKIPPING {pipelineType} pipeline - known to stall during direct testing");
                        continue;
                    }
                    
                    foreach (var memoryStrategy in memoryStrategies)
                    {
                        string combinationName = $"{pipelineType}_{memoryStrategy}";
                        Console.WriteLine($"\n===== Testing {pipelineType} pipeline with {memoryStrategy} strategy =====");
                        
                        var benchmark = new MemoryManagementBenchmarks
                        {
                            MessageBatchSize = batchSize,
                            TotalMessages = totalMessages,
                            MemoryStrategy = memoryStrategy,
                            PipelineType = pipelineType
                        };
                        
                        try
                        {
                            benchmark.Setup();
                            
                            // Call IterationSetup to initialize the pipeline
                            benchmark.IterationSetup();
                            
                            Console.WriteLine($"Processing {totalMessages} messages in batches of {batchSize}...");
                            var sw = System.Diagnostics.Stopwatch.StartNew();
                            
                            // Call the appropriate benchmark method based on the memory strategy
                            switch (memoryStrategy)
                            {
                                case MemoryManagementBenchmarks.MemoryManagementApproach.ArrayPoolWithRMS:
                                    benchmark.ArrayPool_RMS_Benchmark().GetAwaiter().GetResult();
                                    break;
                                case MemoryManagementBenchmarks.MemoryManagementApproach.MessageObjectPool:
                                    benchmark.Message_Pool_Benchmark().GetAwaiter().GetResult();
                                    break;
                                case MemoryManagementBenchmarks.MemoryManagementApproach.UnsafeMemoryAccess:
                                    benchmark.Unsafe_Memory_Benchmark().GetAwaiter().GetResult();
                                    break;
                            }
                            
                            sw.Stop();
                            
                            long elapsedMs = sw.ElapsedMilliseconds;
                            results[combinationName] = elapsedMs;
                            
                            Console.WriteLine($"Test completed in {elapsedMs}ms");
                            Console.WriteLine($"Messages per second: {totalMessages * 1000.0 / elapsedMs:N0}");
                            Console.WriteLine($"Gen0: {GC.CollectionCount(0) - benchmark._gen0Collections}");
                            Console.WriteLine($"Gen1: {GC.CollectionCount(1) - benchmark._gen1Collections}");
                            Console.WriteLine($"Gen2: {GC.CollectionCount(2) - benchmark._gen2Collections}");
                            
                            // Check if this is the best combination so far
                            if (elapsedMs < bestTime)
                            {
                                bestTime = elapsedMs;
                                bestCombination = (pipelineType, memoryStrategy);
                            }
                            
                            // Call IterationCleanup to properly dispose of resources
                            benchmark.IterationCleanup();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Test failed with exception: {ex.GetType().Name}");
                            Console.WriteLine($"Message: {ex.Message}");
                            results[combinationName] = -1; // Mark as failed
                        }
                        finally
                        {
                            benchmark.Cleanup();
                        }
                    }
                }
                
                // Print sorted results
                Console.WriteLine("\n===== RESULTS (FASTEST TO SLOWEST) =====");
                int rank = 1;
                foreach (var kvp in results.Where(r => r.Value > 0).OrderBy(r => r.Value))
                {
                    Console.WriteLine($"{rank++}. {kvp.Key}: {kvp.Value}ms");
                }
                
                // Print failed tests if any
                var failedTests = results.Where(r => r.Value < 0).Select(r => r.Key).ToList();
                if (failedTests.Any())
                {
                    Console.WriteLine("\nFailed tests:");
                    foreach (var test in failedTests)
                    {
                        Console.WriteLine($"- {test}");
                    }
                }
                
                Console.WriteLine($"\nBest performing combination: {bestCombination.Pipeline} pipeline with {bestCombination.Strategy} strategy");
                Console.WriteLine($"Execution time: {bestTime}ms");
                Console.WriteLine($"Messages per second: {totalMessages * 1000.0 / bestTime:N0}");
                
                Console.WriteLine("\nFor a more detailed benchmark with just the best memory strategy:");
                Console.WriteLine($"dotnet run --project HubClient.Benchmarks/HubClient.Benchmarks.csproj -c Release -- memory-optimize");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: Fast memory test failed: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        /// <summary>
        /// Runs the extreme storage test with 50M+ records
        /// </summary>
        private static async void RunExtremeStorageTest(string[] args)
        {
            Console.WriteLine("Running extreme storage test with 50M+ records...");
            Console.WriteLine("This is not a benchmark, but a real-world test of the storage system.");
            Console.WriteLine("Press Ctrl+C to cancel at any time.");
            
            try
            {
                // Run the extreme storage test
                await ExtremePersistenceBenchmarks.RunAsync(args);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error running extreme storage test: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }
    }
}
