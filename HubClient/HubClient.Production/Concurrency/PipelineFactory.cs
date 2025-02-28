using System;
using System.Threading;
using System.Threading.Tasks;
using HubClient.Core.Concurrency;

namespace HubClient.Production.Concurrency
{
    /// <summary>
    /// Factory for creating pipeline implementations based on benchmarked strategies
    /// </summary>
    public static class PipelineFactory
    {
        /// <summary>
        /// Creates a concurrency pipeline using the specified strategy
        /// </summary>
        /// <typeparam name="TInput">Input item type</typeparam>
        /// <typeparam name="TOutput">Output item type</typeparam>
        /// <param name="strategy">Pipeline strategy to use</param>
        /// <param name="processor">Processor function</param>
        /// <param name="options">Creation options</param>
        /// <returns>An IConcurrencyPipeline implementation</returns>
        public static IConcurrencyPipeline<TInput, TOutput> Create<TInput, TOutput>(
            PipelineStrategy strategy,
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions options)
        {
            // Validate input parameters
            if (processor == null) throw new ArgumentNullException(nameof(processor));
            
            // Create pipeline based on strategy
            // Channel is the most balanced based on benchmarks
            return strategy switch
            {
                PipelineStrategy.Channel => new ChannelPipeline<TInput, TOutput>(processor, options),
                PipelineStrategy.TaskParallel => new TaskParallelPipeline<TInput, TOutput>(processor, options),
                PipelineStrategy.Dataflow => new DataflowPipeline<TInput, TOutput>(processor, options),
                _ => throw new ArgumentException($"Unsupported pipeline strategy: {strategy}")
            };
        }
        
        /// <summary>
        /// Creates a concurrency pipeline optimized for the current environment based on benchmarks
        /// </summary>
        /// <typeparam name="TInput">Input item type</typeparam>
        /// <typeparam name="TOutput">Output item type</typeparam>
        /// <param name="processor">Processor function</param>
        /// <param name="options">Creation options</param>
        /// <returns>An IConcurrencyPipeline implementation</returns>
        public static IConcurrencyPipeline<TInput, TOutput> CreateOptimized<TInput, TOutput>(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions? options = null)
        {
            options ??= new PipelineCreationOptions
            {
                MaxConcurrency = Math.Min(Environment.ProcessorCount, 16), // Cap at 16 cores based on benchmarks
                InputQueueCapacity = 50000,
                OutputQueueCapacity = 50000,
                AllowSynchronousContinuations = false,
                PreserveOrderInBatch = false,
                BoundedCapacity = 100000
            };
            
            // Based on benchmarks, Channel is the most balanced for real-world workloads
            return new ChannelPipeline<TInput, TOutput>(processor, options);
        }
    }
} 