using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Core.Concurrency
{
    /// <summary>
    /// Interface for a concurrency pipeline that processes items through a producer/consumer pattern
    /// with backpressure management and work distribution
    /// </summary>
    /// <typeparam name="TInput">Type of items being input to the pipeline</typeparam>
    /// <typeparam name="TOutput">Type of items being output from the pipeline</typeparam>
    public interface IConcurrencyPipeline<TInput, TOutput> : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Current capacity of items that can be handled before backpressure is applied
        /// </summary>
        int CurrentCapacity { get; }
        
        /// <summary>
        /// Total number of items processed since the pipeline was created
        /// </summary>
        long ProcessedItemCount { get; }
        
        /// <summary>
        /// Metrics gathered by the pipeline during operation
        /// </summary>
        PipelineMetrics Metrics { get; }
        
        /// <summary>
        /// Enqueues an item for processing
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when the item is enqueued (not when processed)</returns>
        ValueTask EnqueueAsync(TInput item, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Enqueues a batch of items for processing
        /// </summary>
        /// <param name="items">The items to enqueue</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when all items are enqueued (not when processed)</returns>
        ValueTask EnqueueBatchAsync(IEnumerable<TInput> items, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Attempts to dequeue a processed item
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes with a tuple of (success, item). If success is true, item contains the dequeued item. If false, item is default.</returns>
        ValueTask<(bool Success, TOutput? Item)> TryDequeueAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Waits for all currently enqueued items to be processed
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when all items are processed</returns>
        Task CompleteAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Creates a consumer that processes the pipeline output
        /// </summary>
        /// <param name="consumer">Function that processes each output item</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when the consumer is done or cancelled</returns>
        Task ConsumeAsync(Func<TOutput, ValueTask> consumer, CancellationToken cancellationToken = default);
    }
} 