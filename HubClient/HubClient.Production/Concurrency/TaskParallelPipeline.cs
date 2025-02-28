using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using HubClient.Core.Concurrency;

namespace HubClient.Production.Concurrency
{
    /// <summary>
    /// Implementation of IConcurrencyPipeline using Task-based parallelism
    /// This implementation is optimized for CPU-bound processing tasks.
    /// </summary>
    /// <typeparam name="TInput">Type of input items</typeparam>
    /// <typeparam name="TOutput">Type of output items</typeparam>
    public class TaskParallelPipeline<TInput, TOutput> : IConcurrencyPipeline<TInput, TOutput>
    {
        private readonly BlockingCollection<TInput> _inputQueue;
        private readonly BlockingCollection<TOutput> _outputQueue;
        private readonly Func<TInput, CancellationToken, ValueTask<TOutput>> _processor;
        private readonly int _maxConcurrency;
        private readonly CancellationTokenSource _internalCts = new();
        private readonly PipelineMetrics _metrics = new();
        private readonly Task[] _workers;
        private bool _disposed;
        private long _processedCount;

        /// <summary>
        /// Current capacity before backpressure is applied
        /// </summary>
        public int CurrentCapacity => _inputQueue.BoundedCapacity - _inputQueue.Count;

        /// <summary>
        /// Number of items processed by the pipeline
        /// </summary>
        public long ProcessedItemCount => Interlocked.Read(ref _processedCount);

        /// <summary>
        /// Metrics collected by the pipeline
        /// </summary>
        public PipelineMetrics Metrics => _metrics;

        /// <summary>
        /// Creates a new TaskParallelPipeline with the specified processor and options
        /// </summary>
        /// <param name="processor">Function that processes each input item</param>
        /// <param name="options">Pipeline creation options</param>
        public TaskParallelPipeline(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions options)
        {
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            
            // Create bounded blocking collections for queuing
            _inputQueue = new BlockingCollection<TInput>(options.InputQueueCapacity);
            _outputQueue = new BlockingCollection<TOutput>(options.OutputQueueCapacity);
            
            // Configure concurrency
            _maxConcurrency = Math.Max(1, options.MaxConcurrency);
            
            // Start the worker tasks
            _workers = new Task[_maxConcurrency];
            for (int i = 0; i < _maxConcurrency; i++)
            {
                _workers[i] = Task.Run(() => ProcessItemsAsync(_internalCts.Token));
            }
        }

        /// <summary>
        /// Enqueues an item for processing
        /// </summary>
        public ValueTask EnqueueAsync(TInput item, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(TaskParallelPipeline<TInput, TOutput>));
            
            try
            {
                // BlockingCollection will block if capacity is reached (backpressure)
                _inputQueue.Add(item, cancellationToken);
                return ValueTask.CompletedTask;
            }
            catch (OperationCanceledException)
            {
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Enqueues a batch of items for processing
        /// </summary>
        public ValueTask EnqueueBatchAsync(IEnumerable<TInput> items, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(TaskParallelPipeline<TInput, TOutput>));
            if (items == null) throw new ArgumentNullException(nameof(items));
            
            try
            {
                foreach (var item in items)
                {
                    // Will block if capacity is reached
                    _inputQueue.Add(item, cancellationToken);
                }
                
                return ValueTask.CompletedTask;
            }
            catch (OperationCanceledException)
            {
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Attempts to dequeue a processed item
        /// </summary>
        public ValueTask<(bool Success, TOutput? Item)> TryDequeueAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(TaskParallelPipeline<TInput, TOutput>));
            
            try
            {
                if (_outputQueue.TryTake(out var item))
                {
                    return new ValueTask<(bool, TOutput?)>((true, item));
                }
                
                return new ValueTask<(bool, TOutput?)>((false, default));
            }
            catch (OperationCanceledException)
            {
                return new ValueTask<(bool, TOutput?)>((false, default));
            }
        }

        /// <summary>
        /// Waits for all currently enqueued items to be processed
        /// </summary>
        public async Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(TaskParallelPipeline<TInput, TOutput>));
            
            // Mark the input queue as complete
            _inputQueue.CompleteAdding();
            
            // Create a linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            // Wait for workers to drain the input queue
            while (_inputQueue.Count > 0 && !linkedCts.Token.IsCancellationRequested)
            {
                await Task.Delay(10, linkedCts.Token);
            }
            
            // Mark the output queue as complete
            _outputQueue.CompleteAdding();
        }

        /// <summary>
        /// Creates a consumer that processes the pipeline output
        /// </summary>
        public async Task ConsumeAsync(Func<TOutput, ValueTask> consumer, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(TaskParallelPipeline<TInput, TOutput>));
            if (consumer == null) throw new ArgumentNullException(nameof(consumer));
            
            // Create a linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            var token = linkedCts.Token;
            
            try
            {
                // Process items until cancellation or completion
                while (!_outputQueue.IsCompleted && !token.IsCancellationRequested)
                {
                    try
                    {
                        var item = _outputQueue.Take(token);
                        await consumer(item);
                    }
                    catch (InvalidOperationException)
                    {
                        // Queue is complete
                        break;
                    }
                    catch (OperationCanceledException)
                    {
                        // Cancellation is expected
                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // This is normal during cancellation
            }
        }

        /// <summary>
        /// Worker method that processes items from the input queue
        /// </summary>
        private async Task ProcessItemsAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Process until cancellation or completion
                while (!_inputQueue.IsCompleted && !cancellationToken.IsCancellationRequested)
                {
                    TInput item;
                    
                    try
                    {
                        // Try to get the next item
                        item = _inputQueue.Take(cancellationToken);
                    }
                    catch (InvalidOperationException)
                    {
                        // Queue is complete
                        break;
                    }
                    catch (OperationCanceledException)
                    {
                        // Cancellation is expected
                        break;
                    }
                    
                    try
                    {
                        // Track processing time
                        var processingStopwatch = System.Diagnostics.Stopwatch.StartNew();
                        
                        // Process the item
                        var outputItem = await _processor(item, cancellationToken);
                        
                        // Record metrics
                        processingStopwatch.Stop();
                        _metrics.RecordLatency(processingStopwatch.Elapsed);
                        _metrics.IncrementSuccessCount();
                        
                        // Add to the output queue
                        _outputQueue.Add(outputItem, cancellationToken);
                        
                        // Increment the processed count
                        Interlocked.Increment(ref _processedCount);
                    }
                    catch (OperationCanceledException)
                    {
                        // Cancellation is normal, just exit
                        break;
                    }
                    catch (Exception ex)
                    {
                        // Log processing errors
                        _metrics.RecordException(ex);
                        _metrics.IncrementFailureCount();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // This is normal during cancellation
            }
            catch (Exception ex)
            {
                // Unexpected error
                _metrics.RecordException(ex);
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            
            // Cancel ongoing processing
            _internalCts.Cancel();
            
            // Dispose resources
            _inputQueue.Dispose();
            _outputQueue.Dispose();
            _internalCts.Dispose();
            
            _disposed = true;
            
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            
            // Cancel ongoing processing
            _internalCts.Cancel();
            
            // Wait for workers to complete (with timeout)
            await Task.WhenAny(
                Task.WhenAll(_workers),
                Task.Delay(TimeSpan.FromSeconds(5))
            );
            
            // Dispose resources
            _inputQueue.Dispose();
            _outputQueue.Dispose();
            _internalCts.Dispose();
            
            _disposed = true;
            
            GC.SuppressFinalize(this);
        }
    }
} 