using System;
using HubClient.Core.Serialization;
using HubClient.Production.Concurrency;

namespace HubClient.Production
{
    /// <summary>
    /// Configuration options for the OptimizedHubClient with defaults based on benchmarking
    /// </summary>
    public class OptimizedHubClientOptions
    {
        /// <summary>
        /// Server endpoint URL
        /// </summary>
        public string ServerEndpoint { get; set; } = null!;

        /// <summary>
        /// Number of channels to create in the channel manager
        /// Default: 8 (optimal based on benchmarks)
        /// </summary>
        public int ChannelCount { get; set; } = 8;

        /// <summary>
        /// Maximum number of concurrent calls per channel
        /// Default: 1000
        /// </summary>
        public int MaxConcurrentCallsPerChannel { get; set; } = 1000;

        /// <summary>
        /// The concurrency pipeline strategy to use
        /// Default: Channel (most balanced for real-world workloads)
        /// </summary>
        public PipelineStrategy PipelineStrategy { get; set; } = PipelineStrategy.Channel;

        /// <summary>
        /// The serializer type to use for message serialization
        /// Default: UnsafeMemory (highest performance)
        /// </summary>
        public MessageSerializerFactory.SerializerType SerializerType { get; set; } = MessageSerializerFactory.SerializerType.UnsafeMemory;

        /// <summary>
        /// Batch size for processing messages
        /// Default: 25000 (optimal for high throughput)
        /// </summary>
        public int BatchSize { get; set; } = 25000;

        /// <summary>
        /// Timeout in milliseconds for gRPC operations
        /// Default: 10000 (10 seconds)
        /// </summary>
        public int TimeoutMilliseconds { get; set; } = 10000;

        /// <summary>
        /// Maximum number of concurrent calls across all channels
        /// Default: 8000 (8 channels * 1000 calls per channel)
        /// </summary>
        public int MaxConcurrentCalls { get; set; } = 8000;

        /// <summary>
        /// Maximum number of concurrent workers in the pipeline
        /// Default: Min(ProcessorCount, 16)
        /// </summary>
        public int MaxPipelineConcurrency { get; set; } = Math.Min(Environment.ProcessorCount, 16);

        /// <summary>
        /// Capacity of the input queue for the pipeline
        /// Default: 25000 (based on benchmark findings)
        /// </summary>
        public int PipelineInputQueueCapacity { get; set; } = 25000;

        /// <summary>
        /// Capacity of the output queue for the pipeline
        /// Default: 25000 (based on benchmark findings)
        /// </summary>
        public int PipelineOutputQueueCapacity { get; set; } = 25000;

        /// <summary>
        /// Bounded capacity for backpressure in the pipeline
        /// Default: 50000 (2x queue capacity)
        /// </summary>
        public int PipelineBoundedCapacity { get; set; } = 50000;

        /// <summary>
        /// Whether to preserve message order in the pipeline
        /// Default: false (better performance with large workloads)
        /// </summary>
        public bool PreserveMessageOrder { get; set; } = false;

        /// <summary>
        /// Whether to allow synchronous continuations in the pipeline
        /// Default: false (better predictability)
        /// </summary>
        public bool AllowSynchronousContinuations { get; set; } = false;

        /// <summary>
        /// Block size for the RecyclableMemoryStreamManager
        /// Default: 4096 bytes
        /// </summary>
        public int BlockSize { get; set; } = 4096;

        /// <summary>
        /// Large buffer multiple for the RecyclableMemoryStreamManager
        /// Default: 4096 bytes
        /// </summary>
        public int LargeBufferMultiple { get; set; } = 4096;

        /// <summary>
        /// Maximum buffer size for the RecyclableMemoryStreamManager
        /// Default: 4MB
        /// </summary>
        public int MaximumBufferSize { get; set; } = 4 * 1024 * 1024;

        /// <summary>
        /// Validates the options to ensure they are valid
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when options are invalid</exception>
        public void Validate()
        {
            if (string.IsNullOrEmpty(ServerEndpoint))
            {
                throw new ArgumentException("ServerEndpoint must be specified", nameof(ServerEndpoint));
            }

            if (ChannelCount <= 0)
            {
                throw new ArgumentException("ChannelCount must be positive", nameof(ChannelCount));
            }

            if (MaxConcurrentCallsPerChannel <= 0)
            {
                throw new ArgumentException("MaxConcurrentCallsPerChannel must be positive", nameof(MaxConcurrentCallsPerChannel));
            }

            if (MaxPipelineConcurrency <= 0)
            {
                throw new ArgumentException("MaxPipelineConcurrency must be positive", nameof(MaxPipelineConcurrency));
            }

            if (PipelineInputQueueCapacity <= 0)
            {
                throw new ArgumentException("PipelineInputQueueCapacity must be positive", nameof(PipelineInputQueueCapacity));
            }

            if (PipelineOutputQueueCapacity <= 0)
            {
                throw new ArgumentException("PipelineOutputQueueCapacity must be positive", nameof(PipelineOutputQueueCapacity));
            }

            if (PipelineBoundedCapacity <= 0)
            {
                throw new ArgumentException("PipelineBoundedCapacity must be positive", nameof(PipelineBoundedCapacity));
            }

            if (BlockSize <= 0)
            {
                throw new ArgumentException("BlockSize must be positive", nameof(BlockSize));
            }

            if (LargeBufferMultiple <= 0)
            {
                throw new ArgumentException("LargeBufferMultiple must be positive", nameof(LargeBufferMultiple));
            }

            if (MaximumBufferSize <= 0)
            {
                throw new ArgumentException("MaximumBufferSize must be positive", nameof(MaximumBufferSize));
            }
        }
    }
} 