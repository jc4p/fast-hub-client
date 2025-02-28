using System;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Core.Concurrency
{
    /// <summary>
    /// Factory for creating concurrency pipeline instances
    /// </summary>
    public static class PipelineFactory
    {
        /// <summary>
        /// Creates a pipeline using the specified strategy
        /// </summary>
        /// <typeparam name="TInput">Type of input items</typeparam>
        /// <typeparam name="TOutput">Type of output items</typeparam>
        /// <param name="strategy">The pipeline strategy to use</param>
        /// <param name="processor">The function that processes items</param>
        /// <param name="options">Options for configuring the pipeline</param>
        /// <returns>An implementation of IConcurrencyPipeline</returns>
        public static IConcurrencyPipeline<TInput, TOutput> Create<TInput, TOutput>(
            PipelineStrategy strategy,
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions? options = null)
        {
            options ??= new PipelineCreationOptions();
            
            return strategy switch
            {
                PipelineStrategy.Dataflow => CreateDataflowPipeline(processor, options),
                PipelineStrategy.Channel => CreateChannelPipeline(processor, options),
                PipelineStrategy.ThreadPerCore => CreateThreadPerCorePipeline(processor, options),
                _ => throw new ArgumentException($"Unknown pipeline strategy: {strategy}", nameof(strategy))
            };
        }
        
        /// <summary>
        /// Creates a TPL Dataflow pipeline
        /// </summary>
        private static IConcurrencyPipeline<TInput, TOutput> CreateDataflowPipeline<TInput, TOutput>(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions options)
        {
            var dataflowOptions = new DataflowPipelineOptions
            {
                InputQueueCapacity = options.InputQueueCapacity,
                OutputQueueCapacity = options.OutputQueueCapacity,
                MaxConcurrentProcessors = options.MaxConcurrency
            };
            
            return new DataflowPipeline<TInput, TOutput>(processor, dataflowOptions);
        }
        
        /// <summary>
        /// Creates a System.Threading.Channels pipeline
        /// </summary>
        private static IConcurrencyPipeline<TInput, TOutput> CreateChannelPipeline<TInput, TOutput>(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions options)
        {
            var channelOptions = new ChannelPipelineOptions
            {
                InputQueueCapacity = options.InputQueueCapacity,
                OutputQueueCapacity = options.OutputQueueCapacity,
                MaxConcurrentProcessors = options.MaxConcurrency,
                AllowSynchronousContinuations = options.AllowSynchronousContinuations
            };
            
            return new ChannelPipeline<TInput, TOutput>(processor, channelOptions);
        }
        
        /// <summary>
        /// Creates a thread-per-core pipeline
        /// </summary>
        private static IConcurrencyPipeline<TInput, TOutput> CreateThreadPerCorePipeline<TInput, TOutput>(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions options)
        {
            var threadOptions = new ThreadPerCorePipelineOptions
            {
                ThreadCount = options.MaxConcurrency,
                OutputQueueCapacity = options.OutputQueueCapacity,
                MaxItemsPerQueue = options.InputQueueCapacity / Math.Max(1, options.MaxConcurrency),
                MaxTotalItems = options.InputQueueCapacity,
                PreserveOrderInBatch = options.PreserveOrderInBatch
            };
            
            return new ThreadPerCorePipeline<TInput, TOutput>(processor, threadOptions);
        }
    }
    
    /// <summary>
    /// Strategies for pipeline implementation
    /// </summary>
    public enum PipelineStrategy
    {
        /// <summary>
        /// TPL Dataflow implementation (baseline)
        /// </summary>
        Dataflow,
        
        /// <summary>
        /// System.Threading.Channels implementation with bounded capacity
        /// </summary>
        Channel,
        
        /// <summary>
        /// Thread-per-core implementation with work stealing
        /// </summary>
        ThreadPerCore
    }
    
    /// <summary>
    /// Options for creating a pipeline
    /// </summary>
    public class PipelineCreationOptions
    {
        /// <summary>
        /// Maximum capacity of the input queue
        /// </summary>
        public int InputQueueCapacity { get; set; } = 10000;
        
        /// <summary>
        /// Maximum capacity of the output queue
        /// </summary>
        public int OutputQueueCapacity { get; set; } = 10000;
        
        /// <summary>
        /// Maximum concurrency (threads, tasks, etc.) for processing
        /// </summary>
        public int MaxConcurrency { get; set; } = Environment.ProcessorCount * 2;
        
        /// <summary>
        /// Whether to allow synchronous continuations in async operations
        /// </summary>
        public bool AllowSynchronousContinuations { get; set; } = false;
        
        /// <summary>
        /// Whether to preserve order within a batch of items in the pipeline
        /// </summary>
        public bool PreserveOrderInBatch { get; set; } = false;
    }
} 