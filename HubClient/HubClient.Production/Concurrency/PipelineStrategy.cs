namespace HubClient.Production.Concurrency
{
    /// <summary>
    /// Concurrency pipeline strategy options
    /// Based on benchmarks, Channel is the most balanced for real-world workloads
    /// </summary>
    public enum PipelineStrategy
    {
        /// <summary>
        /// Channel-based pipeline (best balanced performance in benchmarks)
        /// </summary>
        Channel,
        
        /// <summary>
        /// Task-based parallel pipeline (best for CPU-bound workloads)
        /// </summary>
        TaskParallel,
        
        /// <summary>
        /// TPL Dataflow pipeline (best for complex workflows)
        /// </summary>
        Dataflow
    }
} 