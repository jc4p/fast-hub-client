using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace HubClient.Core.Storage
{
    /// <summary>
    /// Interface for writing messages to Parquet files
    /// </summary>
    /// <typeparam name="T">Type of message to write, must implement IMessage</typeparam>
    public interface IParquetWriter<T> : IAsyncDisposable where T : IMessage<T>
    {
        /// <summary>
        /// Writes a batch of messages to a Parquet file
        /// </summary>
        /// <param name="messages">Messages to write</param>
        /// <param name="batchId">Unique identifier for this batch of messages</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when the write is finished</returns>
        Task WriteMessagesAsync(IReadOnlyList<T> messages, string batchId, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Gets information about the most recently written batch
        /// </summary>
        /// <returns>Batch information for the last write operation</returns>
        BatchWriteInfo GetLastBatchInfo();
    }
    
    /// <summary>
    /// Information about a batch write operation
    /// </summary>
    public class BatchWriteInfo
    {
        /// <summary>
        /// Unique identifier for the batch
        /// </summary>
        public string BatchId { get; set; } = string.Empty;
        
        /// <summary>
        /// Path to the file that was written
        /// </summary>
        public string FilePath { get; set; } = string.Empty;
        
        /// <summary>
        /// Number of messages in the batch
        /// </summary>
        public int MessageCount { get; set; }
        
        /// <summary>
        /// Size of the file in bytes
        /// </summary>
        public long FileSizeBytes { get; set; }
        
        /// <summary>
        /// Time taken to write the batch in milliseconds
        /// </summary>
        public double WriteTimeMs { get; set; }
        
        /// <summary>
        /// Compression ratio achieved (original size / compressed size)
        /// </summary>
        public double CompressionRatio { get; set; }
    }
} 