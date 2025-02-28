using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace HubClient.Core.Storage
{
    /// <summary>
    /// Interface for intermediate storage of messages that provides high-performance buffering
    /// and reliable persistence to disk.
    /// </summary>
    /// <typeparam name="T">Type of message to store, must implement IMessage</typeparam>
    public interface IIntermediateStorage<T> : IAsyncDisposable where T : IMessage<T>
    {
        /// <summary>
        /// Gets the total number of messages written to this storage
        /// </summary>
        long TotalMessagesWritten { get; }
        
        /// <summary>
        /// Gets the total number of batches flushed to persistent storage
        /// </summary>
        long TotalBatchesFlushed { get; }
        
        /// <summary>
        /// Gets storage metrics including memory usage, throughput, and I/O statistics
        /// </summary>
        StorageMetrics Metrics { get; }
        
        /// <summary>
        /// Adds a single message to the buffer, triggering a flush if the buffer is full
        /// </summary>
        /// <param name="message">The message to add</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that completes when the message is added (not necessarily flushed)</returns>
        ValueTask AddAsync(T message, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Adds a batch of messages to the buffer, triggering a flush if the buffer becomes full
        /// </summary>
        /// <param name="messages">The messages to add</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that completes when all messages are added (not necessarily flushed)</returns>
        ValueTask AddBatchAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Manually triggers a flush of all buffered messages to persistent storage
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that completes when the flush is complete</returns>
        Task FlushAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Completes writing to this storage, flushing any remaining buffered messages
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A task that completes when all messages are flushed and the storage is closed</returns>
        Task CompleteAsync(CancellationToken cancellationToken = default);
    }
    
    /// <summary>
    /// Storage metrics for performance monitoring and tuning
    /// </summary>
    public class StorageMetrics
    {
        /// <summary>
        /// Total number of messages written to this storage
        /// </summary>
        public long MessagesWritten { get; set; }
        
        /// <summary>
        /// Total number of batches flushed to persistent storage
        /// </summary>
        public long BatchesFlushed { get; set; }
        
        /// <summary>
        /// Total bytes written to persistent storage
        /// </summary>
        public long BytesWritten { get; set; }
        
        /// <summary>
        /// Current in-memory buffer usage in bytes
        /// </summary>
        public long CurrentMemoryUsage { get; set; }
        
        /// <summary>
        /// Peak in-memory buffer usage in bytes
        /// </summary>
        public long PeakMemoryUsage { get; set; }
        
        /// <summary>
        /// Average flush time in milliseconds
        /// </summary>
        public double AverageFlushTimeMs { get; set; }
        
        /// <summary>
        /// Peak flush time in milliseconds
        /// </summary>
        public double PeakFlushTimeMs { get; set; }
        
        /// <summary>
        /// Total flush time in milliseconds
        /// </summary>
        public double TotalFlushTimeMs { get; set; }
    }
} 