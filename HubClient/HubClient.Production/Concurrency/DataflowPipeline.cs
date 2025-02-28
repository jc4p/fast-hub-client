using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using HubClient.Core.Concurrency;

namespace HubClient.Production.Concurrency
{
    /// <summary>
    /// Implementation of IConcurrencyPipeline using TPL Dataflow
    /// This implementation provides good composability and explicit flow control.
    /// </summary>
    /// <typeparam name="TInput">Type of input items</typeparam>
    /// <typeparam name="TOutput">Type of output items</typeparam>
    public class DataflowPipeline<TInput, TOutput> : IConcurrencyPipeline<TInput, TOutput>
    {
        private readonly TransformBlock<TInput, TOutput> _processingBlock;
        private readonly BufferBlock<TOutput> _outputBuffer;
        private readonly Func<TInput, CancellationToken, ValueTask<TOutput>> _processor;
        private readonly CancellationTokenSource _internalCts = new();
        private readonly PipelineMetrics _metrics = new();
        private readonly int _boundedCapacity;
        private readonly DataflowLinkOptions _linkOptions;
        private bool _disposed;
        private long _processedCount;

        /// <summary>
        /// Current capacity before backpressure is applied
        /// </summary>
        public int CurrentCapacity => _boundedCapacity - _processingBlock.InputCount;

        /// <summary>
        /// Number of items processed by the pipeline
        /// </summary>
        public long ProcessedItemCount => Interlocked.Read(ref _processedCount);

        /// <summary>
        /// Metrics collected by the pipeline
        /// </summary>
        public PipelineMetrics Metrics => _metrics;

        /// <summary>
        /// Creates a new DataflowPipeline with the specified processor and options
        /// </summary>
        /// <param name="processor">Function that processes each input item</param>
        /// <param name="options">Pipeline creation options</param>
        public DataflowPipeline(
            Func<TInput, CancellationToken, ValueTask<TOutput>> processor,
            PipelineCreationOptions options)
        {
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _boundedCapacity = options.BoundedCapacity;
            
            // Configure dataflow options for the processing block
            var transformOptions = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = options.MaxConcurrency,
                BoundedCapacity = options.InputQueueCapacity,
                CancellationToken = _internalCts.Token,
                SingleProducerConstrained = false,
                EnsureOrdered = options.PreserveOrderInBatch
            };
            
            // Configure dataflow options for the output buffer
            var bufferOptions = new DataflowBlockOptions
            {
                BoundedCapacity = options.OutputQueueCapacity,
                CancellationToken = _internalCts.Token
            };
            
            // Configure link options
            _linkOptions = new DataflowLinkOptions
            {
                PropagateCompletion = true
            };
            
            // Create the processing block
            _processingBlock = new TransformBlock<TInput, TOutput>(
                async input =>
                {
                    try
                    {
                        // Track processing time
                        var processingStopwatch = System.Diagnostics.Stopwatch.StartNew();
                        
                        // Process the item
                        var result = await _processor(input, _internalCts.Token);
                        
                        // Record metrics
                        processingStopwatch.Stop();
                        _metrics.RecordLatency(processingStopwatch.Elapsed);
                        _metrics.IncrementSuccessCount();
                        
                        // Increment the processed count
                        Interlocked.Increment(ref _processedCount);
                        
                        return result;
                    }
                    catch (OperationCanceledException)
                    {
                        // Rethrow cancellations
                        throw;
                    }
                    catch (Exception ex)
                    {
                        // Log processing errors
                        _metrics.RecordException(ex);
                        _metrics.IncrementFailureCount();
                        throw; // Rethrow to fail the block
                    }
                },
                transformOptions);
            
            // Create the output buffer
            _outputBuffer = new BufferBlock<TOutput>(bufferOptions);
            
            // Link the blocks
            _processingBlock.LinkTo(_outputBuffer, _linkOptions);
            
            // Handle completion to propagate failures
            _processingBlock.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    // Log exceptions from the processing block
                    foreach (var ex in t.Exception?.InnerExceptions ?? Enumerable.Empty<Exception>())
                    {
                        _metrics.RecordException(ex);
                    }
                }
            });
        }

        /// <summary>
        /// Enqueues an item for processing
        /// </summary>
        public async ValueTask EnqueueAsync(TInput item, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DataflowPipeline<TInput, TOutput>));
            
            // Create linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            // Post the item and wait if backpressure is applied
            if (!await _processingBlock.SendAsync(item, linkedCts.Token))
            {
                throw new InvalidOperationException("Failed to enqueue item for processing");
            }
        }

        /// <summary>
        /// Enqueues a batch of items for processing
        /// </summary>
        public async ValueTask EnqueueBatchAsync(IEnumerable<TInput> items, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DataflowPipeline<TInput, TOutput>));
            if (items == null) throw new ArgumentNullException(nameof(items));
            
            // Create linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            foreach (var item in items)
            {
                // Post the item and wait if backpressure is applied
                if (!await _processingBlock.SendAsync(item, linkedCts.Token))
                {
                    throw new InvalidOperationException("Failed to enqueue item for processing");
                }
            }
        }

        /// <summary>
        /// Attempts to dequeue a processed item
        /// </summary>
        public async ValueTask<(bool Success, TOutput? Item)> TryDequeueAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DataflowPipeline<TInput, TOutput>));
            
            // Create linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            // Try to receive an item from the output buffer
            if (_outputBuffer.TryReceive(out var item))
            {
                return (true, item);
            }
            
            // If no items are available immediately, wait for a short time
            if (await _outputBuffer.OutputAvailableAsync(linkedCts.Token))
            {
                if (_outputBuffer.TryReceive(out item))
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
            if (_disposed) throw new ObjectDisposedException(nameof(DataflowPipeline<TInput, TOutput>));
            
            // Create linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            
            // Complete the processing block (no more items will be accepted)
            _processingBlock.Complete();
            
            // Wait for all blocks to complete
            try
            {
                await _outputBuffer.Completion.WaitAsync(linkedCts.Token);
            }
            catch (OperationCanceledException)
            {
                // Cancellation is expected
            }
        }

        /// <summary>
        /// Creates a consumer that processes the pipeline output
        /// </summary>
        public async Task ConsumeAsync(Func<TOutput, ValueTask> consumer, CancellationToken cancellationToken = default)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DataflowPipeline<TInput, TOutput>));
            if (consumer == null) throw new ArgumentNullException(nameof(consumer));
            
            // Create linked token for cancellation
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);
            var token = linkedCts.Token;
            
            try
            {
                // Process items until cancellation or completion
                while (!token.IsCancellationRequested)
                {
                    // Get the next item (or wait if none are available)
                    TOutput item;
                    try
                    {
                        if (!_outputBuffer.TryReceive(out item))
                        {
                            if (!await _outputBuffer.OutputAvailableAsync(token))
                            {
                                // No more items available and the block is complete
                                break;
                            }
                            
                            if (!_outputBuffer.TryReceive(out item))
                            {
                                // Something else dequeued the item before we could
                                continue;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Cancellation is expected
                        break;
                    }
                    
                    // Process the item
                    await consumer(item);
                }
            }
            catch (OperationCanceledException)
            {
                // This is normal during cancellation
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            
            // Cancel ongoing processing
            _internalCts.Cancel();
            _internalCts.Dispose();
            
            _disposed = true;
            
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            
            // Cancel ongoing processing
            _internalCts.Cancel();
            
            // Wait for completion (with timeout)
            await Task.WhenAny(
                Task.WhenAll(_processingBlock.Completion, _outputBuffer.Completion),
                Task.Delay(TimeSpan.FromSeconds(5))
            );
            
            // Dispose resources
            _internalCts.Dispose();
            
            _disposed = true;
            
            GC.SuppressFinalize(this);
        }
    }
} 