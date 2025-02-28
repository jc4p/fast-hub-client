using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Core.Concurrency
{
    /// <summary>
    /// Implementation of a concurrency pipeline using dedicated thread-per-core model with work stealing
    /// for maximum performance with CPU-intensive workloads
    /// </summary>
    /// <typeparam name="TInput">Type of items being input to the pipeline</typeparam>
    /// <typeparam name="TOutput">Type of items being output from the pipeline</typeparam>
    public sealed class ThreadPerCorePipeline<TInput, TOutput> : IConcurrencyPipeline<TInput, TOutput>
    {
        private readonly ThreadPerCorePipelineOptions _options;
        private readonly Func<TInput, CancellationToken, ValueTask<TOutput>> _processor;
        private readonly Thread[] _workerThreads;
        private readonly ConcurrentQueue<ItemWithTimestamp<TInput>>[] _workerQueues;
        private readonly BlockingCollection<ItemWithTimestamp<TOutput>> _outputQueue;
        private readonly CancellationTokenSource _cancellationTokenSource = new();
        private readonly ManualResetEventSlim _completionEvent = new(false);
        private readonly AutoResetEvent[] _queueEvents;
        private readonly SemaphoreSlim _completionLock = new(1, 1);
        private readonly TaskCompletionSource _allItemsProcessedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _totalItemsQueued;
        private long _processedItemCount;
        private long _enqueuedItemCount;
        private volatile bool _isCompleting;
        private volatile bool _isDisposed;
        
        /// <summary>
        /// Gets the current capacity of the pipeline before backpressure is applied
        /// </summary>
        public int CurrentCapacity => 
            _isDisposed ? 0 : Math.Max(0, _options.MaxItemsPerQueue * _workerQueues.Length - _totalItemsQueued);
        
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
        /// Creates a new instance of the ThreadPerCorePipeline
        /// </summary>
        /// <param name="processor">The function that processes items</param>
        /// <param name="options">Options for configuring the pipeline</param>
        public ThreadPerCorePipeline(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            ThreadPerCorePipelineOptions? options = null)
        {
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _options = options ?? new ThreadPerCorePipelineOptions();
            
            // Calculate the actual number of worker threads to use
            int workerCount = _options.ThreadCount > 0 
                ? _options.ThreadCount 
                : Environment.ProcessorCount;
                
            // Initialize worker queues (one per thread for work distribution)
            _workerQueues = new ConcurrentQueue<ItemWithTimestamp<TInput>>[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                _workerQueues[i] = new ConcurrentQueue<ItemWithTimestamp<TInput>>();
            }
            
            // Initialize queue signal events (for waking up worker threads)
            _queueEvents = new AutoResetEvent[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                _queueEvents[i] = new AutoResetEvent(false);
            }
            
            // Initialize output queue
            _outputQueue = new BlockingCollection<ItemWithTimestamp<TOutput>>(_options.OutputQueueCapacity);
            
            // Create and start worker threads
            _workerThreads = new Thread[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                int threadIndex = i; // Capture for closure
                _workerThreads[i] = new Thread(() => WorkerThreadProc(threadIndex))
                {
                    Name = $"Worker-{i}",
                    IsBackground = true
                };
                _workerThreads[i].Start();
            }
        }
        
        /// <summary>
        /// Enqueues an item for processing
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public ValueTask EnqueueAsync(TInput item, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            ThrowIfCompleting();
            cancellationToken.ThrowIfCancellationRequested();
            
            // Wait for capacity if needed
            while (Interlocked.CompareExchange(ref _totalItemsQueued, 0, 0) >= _options.MaxTotalItems)
            {
                Metrics.RecordBackpressureEvent();
                
                // Check for cancellation
                cancellationToken.ThrowIfCancellationRequested();
                _cancellationTokenSource.Token.ThrowIfCancellationRequested();
                
                // Sleep a bit to reduce CPU usage during backpressure
                Thread.Sleep(1);
            }
            
            // Create timestamped item
            var timestampedItem = new ItemWithTimestamp<TInput>(item, Stopwatch.GetTimestamp());
            
            // Select a worker queue to add the item to 
            // - Round-robin based on a hash of the item to distribute work evenly
            int queueIndex = GetTargetQueueIndex(item);
            
            // Add the item to the selected queue
            _workerQueues[queueIndex].Enqueue(timestampedItem);
            
            // Increment the total items counter and enqueued items counter
            Interlocked.Increment(ref _totalItemsQueued);
            Interlocked.Increment(ref _enqueuedItemCount);
            
            // Signal the worker thread to wake up and process items
            _queueEvents[queueIndex].Set();
            
            return ValueTask.CompletedTask;
        }
        
        /// <summary>
        /// Enqueues a batch of items for processing
        /// </summary>
        /// <param name="items">The items to enqueue</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public ValueTask EnqueueBatchAsync(IEnumerable<TInput> items, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            ThrowIfCompleting();
            
            if (items == null)
                throw new ArgumentNullException(nameof(items));
            
            // Process items in batches
            var itemList = items as IReadOnlyList<TInput> ?? new List<TInput>(items);
            int count = itemList.Count;
            int processedCount = 0;
            int batchItemCount = 0;
            
            // Track enqueued count
            Interlocked.Add(ref _enqueuedItemCount, count);
            
            // Most efficient to add items to the queue in a batch
            while (processedCount < count)
            {
                // Wait for capacity if needed
                while (Interlocked.CompareExchange(ref _totalItemsQueued, 0, 0) >= _options.MaxTotalItems)
                {
                    Metrics.RecordBackpressureEvent();
                    
                    // Check for cancellation
                    cancellationToken.ThrowIfCancellationRequested();
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();
                    
                    // Sleep a bit to reduce CPU usage during backpressure
                    Thread.Sleep(1);
                }
                
                // Calculate how many items we can add in this batch
                int remainingItems = count - processedCount;
                int availableCapacity = _options.MaxTotalItems - Interlocked.CompareExchange(ref _totalItemsQueued, 0, 0);
                int batchSize = Math.Min(remainingItems, availableCapacity);
                
                // Process the batch
                long currentTimestamp = Stopwatch.GetTimestamp();
                for (int i = 0; i < batchSize; i++)
                {
                    // Get the item
                    var item = itemList[processedCount + i];
                    
                    // Create timestamped item
                    var timestampedItem = new ItemWithTimestamp<TInput>(item, currentTimestamp);
                    
                    // Get target queue and add item
                    int queueIndex = GetTargetQueueIndex(item);
                    _workerQueues[queueIndex].Enqueue(timestampedItem);
                    
                    batchItemCount++;
                    
                    // Use a new timestamp after every 100 items to provide more accurate timing
                    if (batchItemCount % 100 == 0)
                    {
                        currentTimestamp = Stopwatch.GetTimestamp();
                    }
                }
                
                // Update counters
                Interlocked.Add(ref _totalItemsQueued, batchSize);
                processedCount += batchSize;
                
                // Signal all worker threads to wake up
                for (int i = 0; i < _queueEvents.Length; i++)
                {
                    _queueEvents[i].Set();
                }
            }
            
            return ValueTask.CompletedTask;
        }
        
        /// <summary>
        /// Attempts to dequeue a processed item
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Tuple containing (success, item). If success is true, item contains the dequeued item.</returns>
        public ValueTask<(bool Success, TOutput? Item)> TryDequeueAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            if (_outputQueue.TryTake(out var timestampedItem))
            {
                // Calculate and record latency
                var currentTimestamp = Stopwatch.GetTimestamp();
                var latency = TimeSpan.FromTicks(
                    (currentTimestamp - timestampedItem.Timestamp) * 
                    TimeSpan.TicksPerSecond / Stopwatch.Frequency);
                
                Metrics.RecordLatency(latency);
                
                return new ValueTask<(bool, TOutput?)>((true, timestampedItem.Item));
            }
            
            return new ValueTask<(bool, TOutput?)>((false, default));
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
                if (_isCompleting)
                {
                    await WaitForAllItemsToBeProcessedAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }
                
                // Signal that no more items will be added
                _isCompleting = true;
                
                // Wake up all worker threads to check for completion
                for (int i = 0; i < _queueEvents.Length; i++)
                {
                    _queueEvents[i].Set();
                }
                
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
                // Wait for the completion event with polling
                // We'll use a max timeout of 30 seconds as a fallback, but with intelligent polling
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, timeoutCts.Token);
                
                // Quick check for exact match first
                if (ProcessedItemCount == EnqueuedItemCount && IsAllWorkComplete())
                {
                    _completionEvent.Set();
                    return;
                }
                
                // Wait for completion event or completion task
                while (!linkedCts.Token.IsCancellationRequested)
                {
                    // Check for completion
                    if (_completionEvent.IsSet || await Task.WhenAny(
                        Task.Run(() => _completionEvent.Wait(100, linkedCts.Token)),
                        _allItemsProcessedTcs.Task,
                        Task.Delay(1000, linkedCts.Token)
                    ).ConfigureAwait(false) == _allItemsProcessedTcs.Task)
                    {
                        // Double-check that all work is complete
                        if (ProcessedItemCount == EnqueuedItemCount && IsAllWorkComplete())
                        {
                            _completionEvent.Set();
                            return;
                        }
                    }
                    
                    // Check if all queues are empty and all items processed
                    if (IsAllWorkComplete() && ProcessedItemCount == EnqueuedItemCount)
                    {
                        _completionEvent.Set();
                        _allItemsProcessedTcs.TrySetResult();
                        return;
                    }
                }
                
                // If we've timed out, check one last time
                if (timeoutCts.IsCancellationRequested)
                {
                    var isComplete = IsAllWorkComplete() && ProcessedItemCount == EnqueuedItemCount;
                    
                    if (!isComplete)
                    {
                        throw new TimeoutException(
                            $"Timed out waiting for all items to process. Expected: {EnqueuedItemCount}, Processed: {ProcessedItemCount}");
                    }
                    
                    _completionEvent.Set();
                }
                
                // If we're here due to cancellation, throw
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (OperationCanceledException)
            {
                // Propagate cancellation
                throw;
            }
        }
        
        /// <summary>
        /// Creates a consumer that processes the pipeline output
        /// </summary>
        /// <param name="consumer">Function that processes each output item</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async Task ConsumeAsync(Func<TOutput, ValueTask> consumer, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            if (consumer == null)
                throw new ArgumentNullException(nameof(consumer));
            
            try
            {
                // Process items until completion or cancellation
                foreach (var timestampedItem in _outputQueue.GetConsumingEnumerable(cancellationToken))
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
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Cancellation is expected here
                throw;
            }
        }
        
        /// <summary>
        /// Main processing loop for worker threads
        /// </summary>
        private void WorkerThreadProc(int threadIndex)
        {
            try
            {
                // Get the queue for this thread
                var queue = _workerQueues[threadIndex];
                var queueEvent = _queueEvents[threadIndex];
                
                // Process items until shut down
                while (!_cancellationTokenSource.IsCancellationRequested && !_isDisposed)
                {
                    // Try to process an item from our own queue
                    if (queue.TryDequeue(out var item))
                    {
                        ProcessItem(item);
                        continue;
                    }
                    
                    // If our queue is empty, try to steal work from other queues
                    if (StealAndProcessWork())
                    {
                        continue;
                    }
                    
                    // If we're completing and all work is done, signal completion and exit
                    if (_isCompleting && IsAllWorkComplete())
                    {
                        _completionEvent.Set();
                        _allItemsProcessedTcs.TrySetResult();
                        break;
                    }
                    
                    // Wait for more work or a termination signal
                    queueEvent.WaitOne(10); // Short timeout to periodically check for completion
                }
            }
            catch (Exception ex)
            {
                // Log any unhandled exceptions in the worker thread
                Console.WriteLine($"Error in worker thread {threadIndex}: {ex}");
            }
        }
        
        /// <summary>
        /// Processes a single item
        /// </summary>
        private void ProcessItem(ItemWithTimestamp<TInput> item)
        {
            try
            {
                // Calculate queue wait time
                var processingStartTime = Stopwatch.GetTimestamp();
                var queueTime = TimeSpan.FromTicks(
                    (processingStartTime - item.Timestamp) * 
                    TimeSpan.TicksPerSecond / Stopwatch.Frequency);
                
                Metrics.RecordQueueWaitTime(queueTime);
                
                // Process the item
                // For CPU-bound operations, we'll run synchronously unless the processor is explicitly async
                ValueTask<TOutput> task = _processor(item.Item, _cancellationTokenSource.Token);
                
                TOutput result;
                if (task.IsCompletedSuccessfully)
                {
                    // Synchronous completion
                    result = task.Result;
                }
                else
                {
                    // Asynchronous completion - wait for it
                    result = task.AsTask().GetAwaiter().GetResult();
                }
                
                // Calculate processing time
                var processingEndTime = Stopwatch.GetTimestamp();
                var processingTime = TimeSpan.FromTicks(
                    (processingEndTime - processingStartTime) * 
                    TimeSpan.TicksPerSecond / Stopwatch.Frequency);
                
                Metrics.RecordProcessingTime(processingTime);
                
                // Add the result to the output queue with the original timestamp
                if (!_isDisposed && !_cancellationTokenSource.IsCancellationRequested)
                {
                    _outputQueue.Add(new ItemWithTimestamp<TOutput>(result, item.Timestamp));
                    
                    // Update counters
                    long processedCount = Interlocked.Increment(ref _processedItemCount);
                    Interlocked.Decrement(ref _totalItemsQueued);
                    Metrics.RecordItemProcessed();
                    
                    // Check if we've processed all enqueued items
                    if (processedCount == EnqueuedItemCount && _isCompleting && IsAllWorkComplete())
                    {
                        _completionEvent.Set();
                        _allItemsProcessedTcs.TrySetResult();
                    }
                }
            }
            catch (OperationCanceledException) when (_cancellationTokenSource.IsCancellationRequested)
            {
                // Cancellation is expected, just decrement the counter
                Interlocked.Decrement(ref _totalItemsQueued);
            }
            catch (Exception ex)
            {
                // Log error but continue processing other items
                Console.WriteLine($"Error processing item: {ex}");
                Interlocked.Decrement(ref _totalItemsQueued);
            }
        }
        
        /// <summary>
        /// Tries to steal work from other queues and process it
        /// </summary>
        /// <returns>True if work was stolen and processed, false otherwise</returns>
        private bool StealAndProcessWork()
        {
            // Try to steal work from other queues
            for (int i = 0; i < _workerQueues.Length; i++)
            {
                if (_workerQueues[i].TryDequeue(out var item))
                {
                    // Process the stolen item
                    ProcessItem(item);
                    return true;
                }
            }
            
            return false;
        }
        
        /// <summary>
        /// Checks if all work is complete (queues empty and no in-flight processing)
        /// </summary>
        private bool IsAllWorkComplete()
        {
            // Check if all queues are empty
            for (int i = 0; i < _workerQueues.Length; i++)
            {
                if (!_workerQueues[i].IsEmpty)
                    return false;
            }
            
            // Check if all items have been processed
            return _totalItemsQueued == 0;
        }
        
        /// <summary>
        /// Gets the target queue index for an item
        /// </summary>
        /// <param name="item">The item to get the queue index for</param>
        /// <returns>The queue index</returns>
        private int GetTargetQueueIndex(TInput item)
        {
            // If item is null, use a default hash
            int hash = item?.GetHashCode() ?? 0;
            
            // Use positive hash and modulo to get an index
            return (hash & 0x7FFFFFFF) % _workerQueues.Length;
        }
        
        /// <summary>
        /// Throws an exception if the pipeline is disposed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(ThreadPerCorePipeline<TInput, TOutput>));
            }
        }
        
        /// <summary>
        /// Throws an exception if the pipeline is completing
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfCompleting()
        {
            if (_isCompleting)
            {
                throw new InvalidOperationException("Cannot add items to a completing pipeline");
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
                // Cancel processing
                _cancellationTokenSource.Cancel();
                
                // Signal all worker threads to exit
                for (int i = 0; i < _queueEvents.Length; i++)
                {
                    _queueEvents[i].Set();
                }
                
                // Close the output queue
                _outputQueue.CompleteAdding();
                
                // Wait for worker threads to exit (with a reasonable timeout)
                // In a production environment, we might want to adjust this timeout
                foreach (var thread in _workerThreads)
                {
                    if (!thread.Join(100))
                    {
                        // Thread didn't exit in time, but we tried
                    }
                }
                
                // Dispose resources
                _cancellationTokenSource.Dispose();
                _completionEvent.Dispose();
                _outputQueue.Dispose();
                _completionLock.Dispose();
                
                for (int i = 0; i < _queueEvents.Length; i++)
                {
                    _queueEvents[i].Dispose();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during dispose: {ex}");
            }
        }
        
        /// <summary>
        /// Asynchronously disposes the pipeline
        /// </summary>
        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
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
    /// Options for configuring a thread-per-core pipeline
    /// </summary>
    public class ThreadPerCorePipelineOptions
    {
        /// <summary>
        /// Number of worker threads (default is ProcessorCount)
        /// </summary>
        public int ThreadCount { get; set; } = 0;
        
        /// <summary>
        /// Capacity of the output queue
        /// </summary>
        public int OutputQueueCapacity { get; set; } = 10000;
        
        /// <summary>
        /// Maximum number of items per queue before backpressure
        /// </summary>
        public int MaxItemsPerQueue { get; set; } = 1000;
        
        /// <summary>
        /// Maximum total items across all queues before backpressure
        /// </summary>
        public int MaxTotalItems { get; set; } = 50000;
        
        /// <summary>
        /// Whether to preserve the order of items within a batch
        /// </summary>
        public bool PreserveOrderInBatch { get; set; } = false;
    }
} 