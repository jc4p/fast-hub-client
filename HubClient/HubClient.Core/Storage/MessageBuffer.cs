using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace HubClient.Core.Storage
{
    /// <summary>
    /// In-memory buffer for messages that flushes to persistent storage when full
    /// </summary>
    /// <typeparam name="T">Type of message to buffer, must implement IMessage</typeparam>
    public class MessageBuffer<T> : IIntermediateStorage<T> where T : IMessage<T>
    {
        private readonly ConcurrentQueue<T> _messages = new();
        private readonly SemaphoreSlim _flushLock = new(1, 1);
        private readonly IParquetWriter<T> _parquetWriter;
        private readonly int _batchSize;
        private readonly StorageMetrics _metrics = new();
        private readonly Stopwatch _flushStopwatch = new();
        private volatile int _messageCount;
        private long _totalMessagesWritten;
        private long _totalBatchesFlushed;
        private bool _isDisposed;
        private bool _isCompleting;
        
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
        /// Creates a new instance of the <see cref="MessageBuffer{T}"/> class
        /// </summary>
        /// <param name="parquetWriter">Writer for flushing batches to Parquet files</param>
        /// <param name="batchSize">Size of each batch before automatic flush</param>
        public MessageBuffer(IParquetWriter<T> parquetWriter, int batchSize = 10000)
        {
            _parquetWriter = parquetWriter ?? throw new ArgumentNullException(nameof(parquetWriter));
            _batchSize = batchSize > 0 ? batchSize : throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero");
        }
        
        /// <summary>
        /// Adds a single message to the buffer, triggering a flush if the buffer is full
        /// </summary>
        /// <param name="message">The message to add</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A ValueTask that completes when the message is added (not necessarily flushed)</returns>
        public ValueTask AddAsync(T message, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            _messages.Enqueue(message);
            
            // If we've reached the batch size, try to flush
            if (Interlocked.Increment(ref _messageCount) >= _batchSize)
            {
                // Run the flush in the background and don't await it here
                // This allows the caller to continue adding messages
                _ = TryFlushInternalAsync(false, cancellationToken);
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
            
            // If we've reached the batch size, try to flush
            if (newCount >= _batchSize)
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
        /// <returns>A Task that completes when the flush is complete</returns>
        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            await TryFlushInternalAsync(true, cancellationToken).ConfigureAwait(false);
        }
        
        /// <summary>
        /// Completes writing to this buffer, flushing any remaining buffered messages
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A Task that completes when all messages are flushed and the buffer is closed</returns>
        public async Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            if (_isDisposed || _isCompleting)
                return;
                
            _isCompleting = true;
            
            // Flush any remaining messages
            await FlushAsync(cancellationToken).ConfigureAwait(false);
            
            // Dispose the buffer after flushing
            await DisposeAsync().ConfigureAwait(false);
        }
        
        /// <summary>
        /// Disposes the message buffer and associated resources
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_isDisposed)
                return;
                
            _isDisposed = true;
            
            try
            {
                // Try to flush any remaining messages
                await TryFlushInternalAsync(true, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Ignore exceptions during disposal
            }
            
            // Dispose the parquet writer
            await _parquetWriter.DisposeAsync().ConfigureAwait(false);
            
            // Dispose the flush lock
            _flushLock.Dispose();
        }
        
        /// <summary>
        /// Tries to flush messages to persistent storage
        /// </summary>
        /// <param name="force">If true, flush even if there are fewer messages than the batch size</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that completes when the flush is complete</returns>
        private async Task TryFlushInternalAsync(bool force, CancellationToken cancellationToken)
        {
            // Don't flush if we're already disposing
            if (_isDisposed)
                return;
                
            // If we're not forcing and we don't have enough messages, don't flush
            if (!force && _messageCount < _batchSize)
                return;
                
            // Try to acquire the flush lock, but don't block if another flush is in progress
            if (!await _flushLock.WaitAsync(0, cancellationToken).ConfigureAwait(false))
                return;
                
            try
            {
                // Now that we have the lock, check again if we should flush
                if (_isDisposed)
                    return;
                    
                if (!force && _messageCount < _batchSize)
                {
                    return;
                }
                
                // Dequeue messages up to the batch size
                var messagesToFlush = new List<T>(_batchSize);
                int dequeueCount = 0;
                
                while (dequeueCount < _batchSize && _messages.TryDequeue(out var message))
                {
                    messagesToFlush.Add(message);
                    dequeueCount++;
                }
                
                // If we didn't get any messages, just return
                if (messagesToFlush.Count == 0)
                {
                    return;
                }
                
                // Decrement the message count by the number we dequeued
                Interlocked.Add(ref _messageCount, -dequeueCount);
                
                // Start timing the flush
                _flushStopwatch.Restart();
                
                // Generate a batch ID using timestamp and number of batches already flushed
                string batchId = $"batch_{DateTime.UtcNow:yyyyMMdd_HHmmss}_{Interlocked.Increment(ref _totalBatchesFlushed)}";
                
                // Flush to Parquet
                await _parquetWriter.WriteMessagesAsync(messagesToFlush, batchId, cancellationToken).ConfigureAwait(false);
                
                // Stop timing the flush
                _flushStopwatch.Stop();
                double flushTimeMs = _flushStopwatch.Elapsed.TotalMilliseconds;
                
                // Update metrics
                _totalMessagesWritten += messagesToFlush.Count;
                
                // Get batch info for metrics
                var batchInfo = _parquetWriter.GetLastBatchInfo();
                
                // Update metrics with flush info
                UpdateMetrics(messagesToFlush.Count, flushTimeMs, batchInfo);
            }
            finally
            {
                // Release the flush lock
                _flushLock.Release();
            }
        }
        
        /// <summary>
        /// Updates storage metrics with information from the latest flush
        /// </summary>
        private void UpdateMetrics(int messageCount, double flushTimeMs, BatchWriteInfo batchInfo)
        {
            // Update flush counts and sizes
            _metrics.MessagesWritten = _totalMessagesWritten;
            _metrics.BatchesFlushed = _totalBatchesFlushed;
            _metrics.BytesWritten += batchInfo.FileSizeBytes;
            
            // Update timing metrics
            _metrics.TotalFlushTimeMs += flushTimeMs;
            _metrics.AverageFlushTimeMs = _metrics.TotalFlushTimeMs / _totalBatchesFlushed;
            
            if (flushTimeMs > _metrics.PeakFlushTimeMs)
            {
                _metrics.PeakFlushTimeMs = flushTimeMs;
            }
            
            // Update memory metrics - this is an approximation
            _metrics.CurrentMemoryUsage = _messageCount * 256; // rough estimate of 256 bytes per message
            
            if (_metrics.CurrentMemoryUsage > _metrics.PeakMemoryUsage)
            {
                _metrics.PeakMemoryUsage = _metrics.CurrentMemoryUsage;
            }
        }
        
        /// <summary>
        /// Throws an ObjectDisposedException if the buffer is disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(MessageBuffer<T>));
            }
        }
    }
} 