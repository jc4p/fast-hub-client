using System;

namespace RealtimeListener.Production.Concurrency
{
    /// <summary>
    /// Options for creating concurrency pipelines
    /// </summary>
    public class PipelineCreationOptions
    {
        /// <summary>
        /// Maximum concurrent processing operations (defaults to processor count)
        /// </summary>
        public int MaxConcurrency { get; set; } = Environment.ProcessorCount;
        
        /// <summary>
        /// Capacity of the input queue before backpressure is applied
        /// Optimal value from benchmarks: 50000
        /// </summary>
        public int InputQueueCapacity { get; set; } = 50000;
        
        /// <summary>
        /// Capacity of the output queue
        /// Optimal value from benchmarks: 50000
        /// </summary>
        public int OutputQueueCapacity { get; set; } = 50000;
        
        /// <summary>
        /// Bounded capacity for overall pipeline
        /// </summary>
        public int BoundedCapacity { get; set; } = 100000;
        
        /// <summary>
        /// Whether the pipeline should preserve order of items
        /// </summary>
        public bool PreserveOrderInBatch { get; set; } = false;
        
        /// <summary>
        /// Whether to allow synchronous continuations for performance
        /// </summary>
        public bool AllowSynchronousContinuations { get; set; } = false;
    }
} 