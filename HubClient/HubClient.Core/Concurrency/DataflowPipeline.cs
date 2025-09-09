using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace HubClient.Core.Concurrency
{
    /// <summary>
    /// Implementation of a concurrency pipeline using TPL Dataflow
    /// </summary>
    /// <typeparam name="TInput">Type of items being input to the pipeline</typeparam>
    /// <typeparam name="TOutput">Type of items being output from the pipeline</typeparam>
    public sealed class DataflowPipeline<TInput, TOutput> : IConcurrencyPipeline<TInput, TOutput>
    {
        private readonly DataflowPipelineOptions _options;
        private readonly Func<TInput, CancellationToken, ValueTask<TOutput>> _processor;
        private readonly TransformBlock<ItemWithTimestamp<TInput>, ItemWithTimestamp<TOutput>> _processingBlock;
        private readonly BufferBlock<ItemWithTimestamp<TOutput>> _outputBlock;
        private readonly CancellationTokenSource _cancellationTokenSource = new();
        private readonly SemaphoreSlim _backpressureSemaphore;
        private readonly SemaphoreSlim _completionLock = new(1, 1);
        private readonly TaskCompletionSource _allItemsProcessedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private bool _isDisposed;
        
        private long _processedItemCount;
        private long _enqueuedItemCount;
        
        /// <summary>
        /// Gets the current capacity of the pipeline before backpressure is applied
        /// </summary>
        public int CurrentCapacity => _backpressureSemaphore.CurrentCount;
        
        /// <summary>
        /// Gets the total number of items processed by the pipeline
        /// </summary>
        public long ProcessedItemCount => Interlocked.Read(ref _processedItemCount);
        
        /// <summary>
        /// Gets the total number of items enqueued to the pipeline
        /// </summary>
        private long EnqueuedItemCount => Interlocked.Read(ref _enqueuedItemCount);
        
        /// <summary>
        /// Gets the metrics gathered by the pipeline
        /// </summary>
        public PipelineMetrics Metrics { get; } = new PipelineMetrics();
        
        /// <summary>
        /// Creates a new instance of the DataflowPipeline
        /// </summary>
        /// <param name="processor">The function that processes items</param>
        /// <param name="options">Options for configuring the pipeline</param>
        public DataflowPipeline(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            DataflowPipelineOptions? options = null)
        {
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _options = options ?? new DataflowPipelineOptions();
            
            // Setup concurrency control
            _backpressureSemaphore = new SemaphoreSlim(_options.MaxConcurrentProcessors);
            
            // Create the processing block
            var processingBlockOptions = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = _options.MaxConcurrentProcessors,
                BoundedCapacity = _options.InputQueueCapacity,
                CancellationToken = _cancellationTokenSource.Token
            };
            
            _processingBlock = new TransformBlock<ItemWithTimestamp<TInput>, ItemWithTimestamp<TOutput>>(
                async item =>
                {
                    // Acquire a permit to process the item - this implements backpressure
                    await _backpressureSemaphore.WaitAsync(_cancellationTokenSource.Token).ConfigureAwait(false);
                    
                    try
                    {
                        // Calculate queue wait time
                        var processingStartTime = Stopwatch.GetTimestamp();
                        var queueTime = TimeSpan.FromTicks(
                            (processingStartTime - item.Timestamp) * 
                            TimeSpan.TicksPerSecond / Stopwatch.Frequency);
                        
                        Metrics.RecordQueueWaitTime(queueTime);
                        
                        // Process the item
                        var output = await _processor(item.Item, _cancellationTokenSource.Token).ConfigureAwait(false);
                        
                        // Calculate processing time
                        var processingEndTime = Stopwatch.GetTimestamp();
                        var processingTime = TimeSpan.FromTicks(
                            (processingEndTime - processingStartTime) * 
                            TimeSpan.TicksPerSecond / Stopwatch.Frequency);
                        
                        Metrics.RecordProcessingTime(processingTime);
                        
                        // Update the counter
                        var processedCount = Interlocked.Increment(ref _processedItemCount);
                        Metrics.RecordItemProcessed();
                        
                        // Check if we've processed all enqueued items
                        var completion = _processingBlock!.Completion;
                        if (processedCount == EnqueuedItemCount && completion.IsCompleted)
                        {
                            // Signal that all items have been processed
                            _allItemsProcessedTcs.TrySetResult();
                        }
                        
                        // Return the output with the original timestamp for end-to-end latency
                        return new ItemWithTimestamp<TOutput>(output, item.Timestamp);
                    }
                    finally
                    {
                        // Always release the permit
                        _backpressureSemaphore.Release();
                    }
                },
                processingBlockOptions);
                
            // Create the output block
            var outputBlockOptions = new DataflowBlockOptions
            {
                BoundedCapacity = _options.OutputQueueCapacity,
                CancellationToken = _cancellationTokenSource.Token
            };
            
            _outputBlock = new BufferBlock<ItemWithTimestamp<TOutput>>(outputBlockOptions);
            
            // Link the blocks
            _processingBlock.LinkTo(_outputBlock, new DataflowLinkOptions { PropagateCompletion = true });
        }
        
        /// <summary>
        /// Enqueues an item for processing
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async ValueTask EnqueueAsync(TInput item, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            // Check for completion
            if (_processingBlock.Completion.IsCompleted)
            {
                throw new InvalidOperationException("Cannot add items to a completed pipeline");
            }
            
            // Create a linked token to handle both external cancellation and disposal
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _cancellationTokenSource.Token);
                
            // Create timestamped item
            var timestampedItem = new ItemWithTimestamp<TInput>(item, Stopwatch.GetTimestamp());
            
            // Try to post the item to the processing block
            if (!_processingBlock.Post(timestampedItem))
            {
                // If the block is full, we need to wait for capacity
                Metrics.RecordBackpressureEvent();
                
                // Wait for the block to accept the item
                await _processingBlock.SendAsync(timestampedItem, linkedCts.Token).ConfigureAwait(false);
            }
            
            // Increment the enqueued count
            Interlocked.Increment(ref _enqueuedItemCount);
        }
        
        /// <summary>
        /// Enqueues a batch of items for processing
        /// </summary>
        /// <param name="items">The items to enqueue</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async ValueTask EnqueueBatchAsync(IEnumerable<TInput> items, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            if (items == null)
                throw new ArgumentNullException(nameof(items));
                
            // Check for completion
            if (_processingBlock.Completion.IsCompleted)
            {
                throw new InvalidOperationException("Cannot add items to a completed pipeline");
            }
                
            // Create a linked token to handle both external cancellation and disposal
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _cancellationTokenSource.Token);
                
            long currentTimestamp = Stopwatch.GetTimestamp();
            int itemCount = 0;
            
            foreach (var item in items)
            {
                // Create timestamped item
                var timestampedItem = new ItemWithTimestamp<TInput>(item, currentTimestamp);
                
                // Try to post the item to the processing block
                if (!_processingBlock.Post(timestampedItem))
                {
                    // If the block is full, we need to wait for capacity
                    Metrics.RecordBackpressureEvent();
                    
                    // Wait for the block to accept the item
                    await _processingBlock.SendAsync(timestampedItem, linkedCts.Token).ConfigureAwait(false);
                }
                
                // Increment counter
                itemCount++;
                
                // Use a new timestamp after every 100 items to provide more accurate timing
                if (itemCount % 100 == 0)
                {
                    currentTimestamp = Stopwatch.GetTimestamp();
                }
            }
            
            // Increment the enqueued count
            Interlocked.Add(ref _enqueuedItemCount, itemCount);
        }
        
        /// <summary>
        /// Attempts to dequeue a processed item
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes with a tuple of (success, item). If success is true, item contains the dequeued item. If false, item is default.</returns>
        public async ValueTask<(bool Success, TOutput? Item)> TryDequeueAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            // Create a linked token to handle both external cancellation and disposal
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _cancellationTokenSource.Token);
                
            try
            {
                if (_outputBlock.TryReceive(out var item))
                {
                    // Calculate and record latency
                    var currentTimestamp = Stopwatch.GetTimestamp();
                    var latency = TimeSpan.FromTicks(
                        (currentTimestamp - item.Timestamp) * 
                        TimeSpan.TicksPerSecond / Stopwatch.Frequency);
                    
                    Metrics.RecordLatency(latency);
                    
                    return (true, item.Item);
                }
                
                // Try to receive with a timeout
                if (await _outputBlock.OutputAvailableAsync(linkedCts.Token).ConfigureAwait(false))
                {
                    if (_outputBlock.TryReceive(out var result))
                    {
                        // Calculate and record latency
                        var currentTimestamp = Stopwatch.GetTimestamp();
                        var latency = TimeSpan.FromTicks(
                            (currentTimestamp - result.Timestamp) * 
                            TimeSpan.TicksPerSecond / Stopwatch.Frequency);
                        
                        Metrics.RecordLatency(latency);
                        
                        return (true, result.Item);
                    }
                }
                
                return (false, default);
            }
            catch (TaskCanceledException)
            {
                // Task was cancelled, return no item
                return (false, default);
            }
        }
        
        /// <summary>
        /// Waits for all currently enqueued items to be processed
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when all items are processed</returns>
        public async Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            // Ensure only one completion process can run at a time
            await _completionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            
            try
            {
                // Early exit if we've already completed
                if (_processingBlock.Completion.IsCompleted)
                {
                    await WaitForAllItemsToBeProcessedAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }
                
                // Signal that no more items will be added
                _processingBlock.Complete();
                
                // Wait for all items to be processed
                await WaitForAllItemsToBeProcessedAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _completionLock.Release();
            }
        }
        
        /// <summary>
        /// Waits for all items to be fully processed through the pipeline
        /// </summary>
        private async Task WaitForAllItemsToBeProcessedAsync(CancellationToken cancellationToken)
        {
            try
            {
                // First wait for processing to complete
                await Task.WhenAny(
                    _outputBlock.Completion, 
                    Task.Delay(Timeout.Infinite, cancellationToken)
                ).ConfigureAwait(false);
                
                // If we're here due to cancellation, throw
                cancellationToken.ThrowIfCancellationRequested();
                
                // Now check for the accurate count of items processed vs. enqueued
                // We'll poll until they match or until cancelled
                var completed = false;
                
                // We'll use a max timeout of 30 seconds as a fallback, but with intelligent polling
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, timeoutCts.Token);
                
                try
                {
                    // Quick check for exact match first
                    if (ProcessedItemCount == EnqueuedItemCount)
                    {
                        completed = true;
                    }
                    else
                    {
                        // Try waiting on the completion task
                        await Task.WhenAny(
                            _allItemsProcessedTcs.Task,
                            Task.Delay(TimeSpan.FromSeconds(1), linkedCts.Token)
                        ).ConfigureAwait(false);
                        
                        linkedCts.Token.ThrowIfCancellationRequested();
                        
                        // Final check
                        completed = ProcessedItemCount == EnqueuedItemCount;
                    }
                }
                catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
                {
                    // If we've timed out, check one last time
                    completed = ProcessedItemCount == EnqueuedItemCount;
                    
                    if (!completed)
                    {
                        throw new TimeoutException(
                            $"Timed out waiting for all items to process. Expected: {EnqueuedItemCount}, Processed: {ProcessedItemCount}");
                    }
                }
                
                if (!completed)
                {
                    throw new InvalidOperationException(
                        $"Pipeline completed with missing items. Expected: {EnqueuedItemCount}, Processed: {ProcessedItemCount}");
                }
            }
            catch (OperationCanceledException)
            {
                // Cancellation is expected here
                throw;
            }
            catch (Exception ex)
            {
                // Other exceptions should be propagated
                throw new InvalidOperationException("Failed to complete pipeline processing", ex);
            }
        }
        
        /// <summary>
        /// Creates a consumer that processes the pipeline output
        /// </summary>
        /// <param name="consumer">Function that processes each output item</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when the consumer is done or cancelled</returns>
        public async Task ConsumeAsync(Func<TOutput, ValueTask> consumer, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            if (consumer == null)
                throw new ArgumentNullException(nameof(consumer));
                
            // Create a linked token to handle both external cancellation and disposal
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _cancellationTokenSource.Token);
                
            try
            {
                while (await _outputBlock.OutputAvailableAsync(linkedCts.Token).ConfigureAwait(false))
                {
                    // Try to receive an item
                    if (_outputBlock.TryReceive(out var timestampedItem))
                    {
                        // Calculate and record latency
                        var currentTimestamp = Stopwatch.GetTimestamp();
                        var latency = TimeSpan.FromTicks(
                            (currentTimestamp - timestampedItem.Timestamp) * 
                            TimeSpan.TicksPerSecond / Stopwatch.Frequency);
                        
                        Metrics.RecordLatency(latency);
                        
                        // Process the item
                        await consumer(timestampedItem.Item).ConfigureAwait(false);
                    }
                }
            }
            catch (TaskCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Cancellation is expected here
                throw;
            }
        }
        
        /// <summary>
        /// Disposes the pipeline, cancels processing, and releases resources
        /// </summary>
        public void Dispose()
        {
            if (_isDisposed)
                return;
                
            _isDisposed = true;
            
            try
            {
                // Cancel ongoing processing
                _cancellationTokenSource.Cancel();
                
                // Complete the blocks if not already completed
                if (!_processingBlock.Completion.IsCompleted)
                    _processingBlock.Complete();
                    
                if (!_outputBlock.Completion.IsCompleted)
                    _outputBlock.Complete();
                
                // Clean up resources
                _cancellationTokenSource.Dispose();
                _backpressureSemaphore.Dispose();
                _completionLock.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during dispose: {ex}");
            }
        }
        
        /// <summary>
        /// Asynchronously disposes the pipeline, cancels processing, and releases resources
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_isDisposed)
                return;
                
            _isDisposed = true;
            
            try
            {
                // Cancel ongoing processing
                _cancellationTokenSource.Cancel();
                
                // Complete the blocks if not already completed
                if (!_processingBlock.Completion.IsCompleted)
                    _processingBlock.Complete();
                    
                if (!_outputBlock.Completion.IsCompleted)
                    _outputBlock.Complete();
                
                // Try to wait for processing to complete
                var completionTimeout = Task.Delay(TimeSpan.FromSeconds(5));
                await Task.WhenAny(
                    Task.WhenAll(_processingBlock.Completion, _outputBlock.Completion),
                    completionTimeout
                ).ConfigureAwait(false);
                
                // Clean up resources
                _cancellationTokenSource.Dispose();
                _backpressureSemaphore.Dispose();
                _completionLock.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during async dispose: {ex}");
            }
        }
        
        /// <summary>
        /// Throws an exception if the pipeline is disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(DataflowPipeline<TInput, TOutput>));
        }
        
        /// <summary>
        /// Struct that holds an item with a timestamp for tracking
        /// </summary>
        private readonly struct ItemWithTimestamp<T>
        {
            public readonly T Item;
            public readonly long Timestamp;
            
            public ItemWithTimestamp(T item, long timestamp)
            {
                Item = item;
                Timestamp = timestamp;
            }
        }
    }
    
    /// <summary>
    /// Options for configuring a dataflow pipeline
    /// </summary>
    public class DataflowPipelineOptions
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
        /// Maximum number of concurrent processors
        /// </summary>
        public int MaxConcurrentProcessors { get; set; } = Environment.ProcessorCount * 2;
    }
} 
