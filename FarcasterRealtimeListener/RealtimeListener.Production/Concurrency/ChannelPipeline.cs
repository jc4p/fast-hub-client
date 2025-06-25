using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CoreConcurrency = HubClient.Core.Concurrency;

namespace RealtimeListener.Production.Concurrency
{
    /// <summary>
    /// Interface for a concurrency pipeline in the Production project
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
        ValueTask EnqueueAsync(TInput item, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Enqueues a batch of items for processing
        /// </summary>
        ValueTask EnqueueBatchAsync(IEnumerable<TInput> items, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Attempts to dequeue a processed item
        /// </summary>
        ValueTask<(bool Success, TOutput? Item)> TryDequeueAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Waits for all currently enqueued items to be processed
        /// </summary>
        Task CompleteAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Creates a consumer that processes the pipeline output
        /// </summary>
        Task ConsumeAsync(Func<TOutput, ValueTask> consumer, CancellationToken cancellationToken = default);
    }

    /// <summary>
    /// Implementation of IConcurrencyPipeline using System.Threading.Channels
    /// Based on benchmark findings, this provides the best balance of performance,
    /// backpressure handling, and clean cancellation for real-world workloads.
    /// </summary>
    /// <typeparam name="TInput">Type of input items</typeparam>
    /// <typeparam name="TOutput">Type of output items</typeparam>
    public class ChannelPipeline<TInput, TOutput> : IConcurrencyPipeline<TInput, TOutput>
    {
        private readonly Channel<TInput> _inputChannel;
        private readonly Channel<TOutput> _outputChannel;
        private readonly Func<TInput, CancellationToken, ValueTask<TOutput>> _processor;
        private readonly int _maxConcurrency;
        private readonly SemaphoreSlim _concurrencyLimiter;
        private readonly CancellationTokenSource _internalCts = new();
        private readonly PipelineMetrics _metrics = new();
        private readonly Task[] _workers;
        private bool _disposed;
        private long _processedCount;
        private readonly PipelineCreationOptions _options;

        /// <summary>
        /// Current capacity before backpressure is applied
        /// </summary>
        public int CurrentCapacity => ((Channel<TInput>)_inputChannel).Reader.CanCount ? 
            Math.Max(0, _options.BoundedCapacity - _inputChannel.Reader.Count) : 
            int.MaxValue;

        /// <summary>
        /// Number of items processed by the pipeline
        /// </summary>
        public long ProcessedItemCount => Interlocked.Read(ref _processedCount);

        /// <summary>
        /// Gets metrics gathered by the pipeline during operation
        /// </summary>
        public PipelineMetrics Metrics => _metrics;

        /// <summary>
        /// Creates a new ChannelPipeline with the specified processor and options
        /// </summary>
        /// <param name="processor">Function that processes each input item</param>
        /// <param name="options">Pipeline creation options</param>
        public ChannelPipeline(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions options)
        {
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            
            // Create the input channel with backpressure support
            _inputChannel = Channel.CreateBounded<TInput>(new BoundedChannelOptions(options.InputQueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = options.AllowSynchronousContinuations
            });
            
            // Create the output channel with backpressure support
            _outputChannel = Channel.CreateBounded<TOutput>(new BoundedChannelOptions(options.OutputQueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = options.AllowSynchronousContinuations
            });
            
            // Configure concurrency
            _maxConcurrency = Math.Max(1, options.MaxConcurrency);
            _concurrencyLimiter = new SemaphoreSlim(_maxConcurrency);
            
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
        public async ValueTask EnqueueAsync(TInput item, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ChannelPipeline<TInput, TOutput>));
            
            // Create a linked token to handle both user cancellation and internal disposal
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            // Write the item to the input channel
            await _inputChannel.Writer.WriteAsync(item, linkedCts.Token);
        }

        /// <summary>
        /// Enqueues a batch of items for processing
        /// </summary>
        public async ValueTask EnqueueBatchAsync(IEnumerable<TInput> items, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ChannelPipeline<TInput, TOutput>));
            if (items == null) throw new ArgumentNullException(nameof(items));
            
            // Create a linked token to handle both user cancellation and internal disposal
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            foreach (var item in items)
            {
                await _inputChannel.Writer.WriteAsync(item, linkedCts.Token);
            }
        }

        /// <summary>
        /// Attempts to dequeue a processed item
        /// </summary>
        public async ValueTask<(bool Success, TOutput? Item)> TryDequeueAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ChannelPipeline<TInput, TOutput>));
            
            // Create a linked token to handle both user cancellation and internal disposal
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            // Try to read from the output channel
            if (await _outputChannel.Reader.WaitToReadAsync(linkedCts.Token))
            {
                if (_outputChannel.Reader.TryRead(out var item))
                {
                    return (true, item);
                }
            }
            
            return (false, default);
        }

        /// <summary>
        /// Waits for all currently enqueued items to be processed
        /// </summary>
        public async Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ChannelPipeline<TInput, TOutput>));
            
            // Mark the input channel as complete (no more items will be accepted)
            _inputChannel.Writer.Complete();
            
            // Create a linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            // Wait for all items to be processed (until the channel is empty)
            while (!_inputChannel.Reader.Completion.IsCompleted)
            {
                await Task.Delay(10, linkedCts.Token);
            }
            
            // Wait for processing to complete
            await _inputChannel.Reader.Completion;
            
            // Mark the output channel as complete
            _outputChannel.Writer.Complete();
        }

        /// <summary>
        /// Creates a consumer that processes the pipeline output
        /// </summary>
        public async Task ConsumeAsync(Func<TOutput, ValueTask> consumer, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ChannelPipeline<TInput, TOutput>));
            if (consumer == null) throw new ArgumentNullException(nameof(consumer));
            
            // Create a linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            var token = linkedCts.Token;
            
            // Process items from the output channel
            await foreach (var item in _outputChannel.Reader.ReadAllAsync(token))
            {
                await consumer(item);
            }
        }

        /// <summary>
        /// Worker method that processes items from the input channel
        /// </summary>
        private async Task ProcessItemsAsync(CancellationToken cancellationToken)
        {
            // Process until cancellation or completion
            while (!cancellationToken.IsCancellationRequested)
            {
                // Wait for an item to be available
                bool hasItem;
                try
                {
                    hasItem = await _inputChannel.Reader.WaitToReadAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // Cancellation is normal, just exit
                    break;
                }
                catch (Exception ex)
                {
                    // Log unexpected errors
                    _metrics.RecordException(ex);
                    continue;
                }
                
                if (!hasItem) break;
                
                // Try to get the next item
                if (!_inputChannel.Reader.TryRead(out var inputItem))
                {
                    continue;
                }
                
                // Limit concurrency
                await _concurrencyLimiter.WaitAsync(cancellationToken);
                
                // Process the item
                try
                {
                    // Track processing time
                    var processingStopwatch = System.Diagnostics.Stopwatch.StartNew();
                    
                    // Process the item
                    var outputItem = await _processor(inputItem, cancellationToken);
                    
                    // Record metrics
                    processingStopwatch.Stop();
                    _metrics.RecordLatency(processingStopwatch.Elapsed);
                    _metrics.IncrementSuccessCount();
                    
                    // Write to the output channel
                    await _outputChannel.Writer.WriteAsync(outputItem, cancellationToken);
                    
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
                finally
                {
                    _concurrencyLimiter.Release();
                }
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            
            // Cancel ongoing processing
            _internalCts.Cancel();
            
            // Dispose resources
            _concurrencyLimiter.Dispose();
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
            _concurrencyLimiter.Dispose();
            _internalCts.Dispose();
            
            _disposed = true;
            
            GC.SuppressFinalize(this);
        }
    }
} 