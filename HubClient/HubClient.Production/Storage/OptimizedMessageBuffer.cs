using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using HubClient.Core.Storage;
using Microsoft.Extensions.Logging;

namespace HubClient.Production.Storage
{
    /// <summary>
    /// High-performance in-memory buffer for messages that flushes to persistent storage when full.
    /// Optimized based on benchmarking results for minimal allocations and maximum throughput.
    /// </summary>
    /// <typeparam name="T">Type of message to buffer, must implement IMessage</typeparam>
    public class OptimizedMessageBuffer<T> : IIntermediateStorage<T> where T : IMessage<T>
    {
        private readonly ConcurrentQueue<T> _messages = new();
        private readonly SemaphoreSlim _flushLock = new(1, 1);
        private readonly IParquetWriter<T> _parquetWriter;
        private readonly int _batchSize;
        private readonly StorageMetrics _metrics = new();
        private readonly Stopwatch _flushStopwatch = new();
        private readonly ILogger<OptimizedMessageBuffer<T>> _logger;
        private readonly bool _useBackgroundFlushing;
        
        private volatile int _messageCount;
        private long _totalMessagesWritten;
        private long _totalBatchesFlushed;
        private bool _isDisposed;
        private bool _isCompleting;
        private Task _backgroundFlushTask = Task.CompletedTask;
        private readonly CancellationTokenSource _backgroundFlushCts = new();
        
        /// <summary>
        /// Gets the total number of messages written to this buffer
        /// </summary>
        public long TotalMessagesWritten => _totalMessagesWritten;
        
        /// <summary>
        /// Gets the total number of batches flushed to persistent storage
        /// </summary>
        public long TotalBatchesFlushed => _totalBatchesFlushed;
        
        /// <summary>
        /// Gets storage metrics including memory usage, throughput, and I/O statistics
        /// </summary>
        public StorageMetrics Metrics => _metrics;
        
        /// <summary>
        /// Creates a new instance of the <see cref="OptimizedMessageBuffer{T}"/> class
        /// with settings optimized for high throughput based on benchmarking results.
        /// </summary>
        /// <param name="parquetWriter">Writer for flushing batches to Parquet files</param>
        /// <param name="logger">Logger for logging information and warnings</param>
        /// <param name="batchSize">Size of each batch before automatic flush (25000 recommended from benchmarks)</param>
        /// <param name="useBackgroundFlushing">Whether to use background thread for flushing (recommended)</param>
        public OptimizedMessageBuffer(
            IParquetWriter<T> parquetWriter,
            ILogger<OptimizedMessageBuffer<T>> logger,
            int batchSize = 25000,
            bool useBackgroundFlushing = true)
        {
            _parquetWriter = parquetWriter ?? throw new ArgumentNullException(nameof(parquetWriter));
            _batchSize = batchSize > 0 ? batchSize : throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero");
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _useBackgroundFlushing = useBackgroundFlushing;
            
            _logger.LogInformation(
                "Created OptimizedMessageBuffer with batchSize={BatchSize}, backgroundFlushing={BackgroundFlushing}",
                _batchSize, _useBackgroundFlushing);
                
            if (_useBackgroundFlushing)
            {
                StartBackgroundFlushing();
            }
        }
        
        private void StartBackgroundFlushing()
        {
            _backgroundFlushTask = Task.Run(async () =>
            {
                try
                {
                    while (!_backgroundFlushCts.IsCancellationRequested)
                    {
                        // Check if we have enough messages to flush
                        if (_messageCount >= _batchSize)
                        {
                            // Use a non-cancelable token so shutdown cancellation doesn't abort in-flight writes
                            await TryFlushInternalAsync(false, CancellationToken.None).ConfigureAwait(false);
                        }
                        
                        // Sleep for a short interval to avoid spinning
                        await Task.Delay(50, _backgroundFlushCts.Token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected when shutting down
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in background flushing task: {Message}", ex.Message);
                }
            });
        }
        
        /// <summary>
        /// Adds a single message to the buffer, triggering a flush if the buffer is full
        /// </summary>
        /// <param name="message">The message to add</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A ValueTask that completes when the message is added (not necessarily flushed)</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AddAsync(T message, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            _messages.Enqueue(message);
            
            // If we've reached the batch size and not using background flushing, trigger a flush
            if (!_useBackgroundFlushing && Interlocked.Increment(ref _messageCount) >= _batchSize)
            {
                // Run the flush in the background and don't await it here
                // This allows the caller to continue adding messages
                _ = TryFlushInternalAsync(false, cancellationToken);
            }
            else
            {
                // Just increment the count if using background flushing
                Interlocked.Increment(ref _messageCount);
            }
            
            return ValueTask.CompletedTask;
        }
        
        /// <summary>
        /// Adds a batch of messages to the buffer, triggering a flush if the buffer becomes full
        /// </summary>
        /// <param name="messages">The messages to add</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A ValueTask that completes when all messages are added (not necessarily flushed)</returns>
        public ValueTask AddBatchAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            int addedCount = 0;
            
            foreach (var message in messages)
            {
                _messages.Enqueue(message);
                addedCount++;
            }
            
            // Increment the message count by the number we added
            int newCount = Interlocked.Add(ref _messageCount, addedCount);
            
            // If we've reached the batch size and not using background flushing, try to flush
            if (!_useBackgroundFlushing && newCount >= _batchSize)
            {
                // Run the flush in the background and don't await it here
                _ = TryFlushInternalAsync(false, cancellationToken);
            }
            
            return ValueTask.CompletedTask;
        }
        
        /// <summary>
        /// Manually triggers a flush of all buffered messages to persistent storage
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that completes when the flush is complete</returns>
        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            // Drain until empty to avoid leaving remainder batches unflushed
            while (_messageCount > 0)
            {
                int before = _messageCount;
                await TryFlushInternalAsync(true, cancellationToken).ConfigureAwait(false);

                // If no progress was made (e.g., due to repeated transient errors), break to avoid a tight loop
                if (_messageCount >= before)
                {
                    break;
                }
            }
        }
        
        /// <summary>
        /// Completes writing to this storage, flushing any remaining buffered messages
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that completes when all messages are flushed and the storage is closed</returns>
        public async Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            if (_isDisposed || _isCompleting)
                return;
                
            _isCompleting = true;
            
            try
            {
                await FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _isCompleting = false;
            }
        }
        
        /// <summary>
        /// Disposes resources used by the buffer
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_isDisposed)
                return;
                
            // Stop background flushing first
            if (_useBackgroundFlushing && _backgroundFlushTask != null)
            {
                try
                {
                    _backgroundFlushCts.Cancel();
                    await _backgroundFlushTask.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error stopping background flush task: {Message}", ex.Message);
                }
                finally
                {
                    _backgroundFlushCts.Dispose();
                }
            }
            
            // Flush any remaining messages
            try
            {
                if (!_isCompleting)
                {
                    await CompleteAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during final flush in DisposeAsync: {Message}", ex.Message);
            }
            
            // Dispose other resources
            try
            {
                _isDisposed = true;
                _flushLock.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing resources: {Message}", ex.Message);
            }
        }
        
        /// <summary>
        /// Tries to flush messages to persistent storage
        /// </summary>
        /// <param name="force">Whether to force a flush even if the batch size hasn't been reached</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that completes when the flush attempt is finished</returns>
        private async Task TryFlushInternalAsync(bool force, CancellationToken cancellationToken)
        {
            // Quick check to avoid taking the lock unnecessarily
            if (_messageCount == 0 || (_messageCount < _batchSize && !force))
                return;
                
            // Ensure only one flush happens at a time
            if (!await _flushLock.WaitAsync(0, cancellationToken).ConfigureAwait(false))
            {
                // Another thread is already flushing
                return;
            }
            
            try
            {
                _flushStopwatch.Restart();
                
                // Don't flush if no messages or if we haven't reached the batch size (unless forced)
                if (_messageCount == 0 || (_messageCount < _batchSize && !force))
                {
                    return;
                }
                
                // Calculate how many messages to dequeue
                int batchSize = Math.Min(_messageCount, _batchSize);
                var batch = new List<T>(batchSize);
                int dequeued = 0;
                
                // Dequeue messages
                for (int i = 0; i < batchSize; i++)
                {
                    if (_messages.TryDequeue(out var message))
                    {
                        batch.Add(message);
                        dequeued++;
                    }
                    else
                    {
                        // Queue is empty
                        break;
                    }
                }
                
                // Adjust the message count
                Interlocked.Add(ref _messageCount, -dequeued);
                
                if (batch.Count > 0)
                {
                    // Generate a unique batch ID
                    string batchId = $"batch_{DateTime.UtcNow:yyyyMMdd_HHmmss}_{Interlocked.Increment(ref _totalBatchesFlushed)}";
                    
                    try
                    {
                        // Write the batch to persistent storage
                        await _parquetWriter.WriteMessagesAsync(batch, batchId, cancellationToken).ConfigureAwait(false);
                        
                        // Update counts
                        Interlocked.Add(ref _totalMessagesWritten, batch.Count);
                        
                        // Get the last batch info for metrics
                        var batchInfo = _parquetWriter.GetLastBatchInfo();
                        
                        // Stop timing and update metrics
                        _flushStopwatch.Stop();
                        UpdateMetrics(batch.Count, _flushStopwatch.Elapsed.TotalMilliseconds, batchInfo);
                        
                        _logger.LogDebug(
                            "Flushed {Count} messages in {ElapsedMs:F2}ms. Total written: {TotalWritten}, " +
                            "Total batches: {TotalBatches}",
                            batch.Count,
                            _flushStopwatch.Elapsed.TotalMilliseconds,
                            _totalMessagesWritten,
                            _totalBatchesFlushed);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error flushing batch {BatchId}: {Message}", batchId, ex.Message);
                        
                        // Put the messages back in the queue
                        foreach (var message in batch)
                        {
                            _messages.Enqueue(message);
                        }
                        
                        // Restore the message count
                        Interlocked.Add(ref _messageCount, batch.Count);
                        
                        throw;
                    }
                    finally
                    {
                        // Clear the batch to help the GC
                        batch.Clear();
                    }
                }
            }
            finally
            {
                _flushLock.Release();
            }
        }
        
        /// <summary>
        /// Updates metrics based on the last flush operation
        /// </summary>
        private void UpdateMetrics(int messageCount, double flushTimeMs, BatchWriteInfo batchInfo)
        {
            // Update total metrics
            _metrics.MessagesWritten = Interlocked.Read(ref _totalMessagesWritten);
            _metrics.BatchesFlushed = Interlocked.Read(ref _totalBatchesFlushed);
            
            // Update timing metrics
            _metrics.TotalFlushTimeMs += flushTimeMs;
            _metrics.AverageFlushTimeMs = _metrics.TotalFlushTimeMs / _metrics.BatchesFlushed;
            _metrics.PeakFlushTimeMs = Math.Max(_metrics.PeakFlushTimeMs, flushTimeMs);
            
            // Update storage metrics
            _metrics.BytesWritten += batchInfo.FileSizeBytes;
            
            // Estimate current memory usage (rough approximation)
            _metrics.CurrentMemoryUsage = _messageCount * EstimateMessageSize(messageCount);
            _metrics.PeakMemoryUsage = Math.Max(_metrics.PeakMemoryUsage, _metrics.CurrentMemoryUsage);
        }
        
        /// <summary>
        /// Estimates the average size of a message based on recent history
        /// </summary>
        private long EstimateMessageSize(int sampleSize)
        {
            // If we have written messages, use the average from the last batch
            if (_totalBatchesFlushed > 0 && _totalMessagesWritten > 0)
            {
                var batchInfo = _parquetWriter.GetLastBatchInfo();
                if (batchInfo.MessageCount > 0)
                {
                    return (long)(batchInfo.FileSizeBytes / batchInfo.CompressionRatio / batchInfo.MessageCount);
                }
            }
            
            // Default to a reasonable estimate
            return 1024; // 1KB per message as a default guess
        }
        
        /// <summary>
        /// Throws if the buffer has been disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(OptimizedMessageBuffer<T>));
        }
    }
} 
