using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using HubClient.Core.Storage;
using Parquet;

namespace HubClient.Production.Storage
{
    /// <summary>
    /// Parquet writer that uses multiple parallel writers to maximize throughput for large datasets.
    /// Based on benchmarking results, this approach scales linearly with available CPU cores.
    /// </summary>
    /// <typeparam name="T">Type of message to write, must implement IMessage</typeparam>
    public class MultiFileParallelWriter<T> : IParquetWriter<T> where T : IMessage<T>, new()
    {
        private readonly string _outputDirectory;
        private readonly string _temporaryDirectory;
        private readonly List<OptimizedParquetWriter<T>> _parallelWriters = new();
        private readonly int _workerCount;
        private readonly ILogger<MultiFileParallelWriter<T>> _logger;
        private readonly ISchemaGenerator _schemaGenerator;
        private readonly Func<T, IDictionary<string, object>> _messageConverter;
        private readonly CompressionMethod _compressionMethod;
        private readonly int _rowGroupSize;
        private readonly int _pageSize;
        private readonly bool _enableDictionaryEncoding;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly Stopwatch _writeStopwatch = new();
        private BatchWriteInfo _lastBatchInfo = new();
        private bool _isDisposed;
        
        /// <summary>
        /// Creates a new instance of the <see cref="MultiFileParallelWriter{T}"/> class with settings
        /// optimized for high throughput on multi-core systems based on benchmarking results.
        /// </summary>
        /// <param name="outputDirectory">Directory where final Parquet files will be written</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <param name="logger">Logger for logging information and warnings</param>
        /// <param name="schemaGenerator">Generator for Parquet schemas</param>
        /// <param name="compressionMethod">Compression method to use for Parquet files</param>
        /// <param name="rowGroupSize">Size of row groups in the Parquet file</param>
        /// <param name="pageSize">Size of pages in bytes</param>
        /// <param name="enableDictionaryEncoding">Whether to enable dictionary encoding for repeated values</param>
        /// <param name="workerCount">Number of parallel writers to use (defaults to CPU core count, max 8)</param>
        public MultiFileParallelWriter(
            string outputDirectory,
            Func<T, IDictionary<string, object>> messageConverter,
            ILogger<MultiFileParallelWriter<T>> logger,
            ISchemaGenerator schemaGenerator,
            CompressionMethod compressionMethod = CompressionMethod.Snappy,
            int rowGroupSize = 50000,
            int pageSize = 8192,
            bool enableDictionaryEncoding = true,
            int workerCount = 0)
        {
            _outputDirectory = !string.IsNullOrWhiteSpace(outputDirectory)
                ? outputDirectory
                : throw new ArgumentNullException(nameof(outputDirectory));
                
            _messageConverter = messageConverter ?? throw new ArgumentNullException(nameof(messageConverter));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _schemaGenerator = schemaGenerator; // Allow null for automatic schema inference
            
            _compressionMethod = compressionMethod;
            _rowGroupSize = rowGroupSize;
            _pageSize = pageSize;
            _enableDictionaryEncoding = enableDictionaryEncoding;
            
            // Set up worker count - default to processor count, max 8
            _workerCount = workerCount > 0 
                ? workerCount 
                : Math.Min(Environment.ProcessorCount, 8);
                
            // Create the output directory
            Directory.CreateDirectory(_outputDirectory);
            
            // Create a temporary directory for worker files
            _temporaryDirectory = Path.Combine(_outputDirectory, $"temp_{Guid.NewGuid()}");
            Directory.CreateDirectory(_temporaryDirectory);
            
            // Initialize parallel writers
            InitializeParallelWriters();
            
            _logger.LogInformation(
                "Created MultiFileParallelWriter with {WorkerCount} workers, compression={Compression}, " +
                "rowGroupSize={RowGroupSize}, pageSize={PageSize}, dictEncoding={DictEncoding}",
                _workerCount, _compressionMethod, _rowGroupSize, _pageSize, _enableDictionaryEncoding);
        }
        
        private void InitializeParallelWriters()
        {
            for (int i = 0; i < _workerCount; i++)
            {
                var workerDir = Path.Combine(_temporaryDirectory, $"worker_{i}");
                Directory.CreateDirectory(workerDir);
                
                // Replace problematic reflection with direct logger creation
                ILogger<OptimizedParquetWriter<T>>? workerLogger;
                
                // Try to cast the logger instance to ILoggerFactory if it's actually a LoggerFactory
                if (_logger is ILoggerFactory factory)
                {
                    // Use the factory directly to create the properly typed logger
                    workerLogger = factory.CreateLogger<OptimizedParquetWriter<T>>();
                }
                else
                {
                    // Fall back to using the logger as is or casting it
                    workerLogger = _logger as ILogger<OptimizedParquetWriter<T>>;
                    
                    if (workerLogger == null)
                    {
                        // Create a simple wrapper logger
                        workerLogger = new TypedLoggerWrapper<OptimizedParquetWriter<T>>(_logger);
                    }
                }
                
                if (workerLogger == null)
                {
                    throw new InvalidOperationException("Failed to create logger for worker");
                }
                
                var parallelWriter = new OptimizedParquetWriter<T>(
                    workerDir,
                    _messageConverter,
                    workerLogger!,
                    _schemaGenerator,
                    _compressionMethod,
                    _rowGroupSize,
                    _pageSize,
                    _enableDictionaryEncoding);
                    
                _parallelWriters.Add(parallelWriter);
            }
        }
        
        // Simple wrapper class to convert a generic ILogger to a typed ILogger<T>
        private class TypedLoggerWrapper<TCategory> : ILogger<TCategory>
        {
            private readonly ILogger _innerLogger;
            
            public TypedLoggerWrapper(ILogger innerLogger)
            {
                _innerLogger = innerLogger ?? throw new ArgumentNullException(nameof(innerLogger));
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull
                => _innerLogger.BeginScope(state);

            public bool IsEnabled(LogLevel logLevel) => _innerLogger.IsEnabled(logLevel);

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) where TState : notnull
                => _innerLogger.Log(logLevel, eventId, state, exception, formatter);
        }
        
        /// <summary>
        /// Writes a batch of messages to Parquet files using multiple parallel writers.
        /// </summary>
        /// <param name="messages">Messages to write</param>
        /// <param name="batchId">Unique identifier for this batch of messages</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when the write is finished</returns>
        public async Task WriteMessagesAsync(IReadOnlyList<T> messages, string batchId, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            if (messages == null || messages.Count == 0)
            {
                _logger.LogWarning("WriteMessagesAsync called with empty message batch. Skipping.");
                return;
            }
            
            // Acquire the write lock
            await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            
            try
            {
                // Start timing the write
                _writeStopwatch.Restart();
                
                // Calculate rough size estimate for statistics
                long estimatedSizeBytes = messages.Sum(m => m.CalculateSize());
                
                // Split messages into chunks for parallel processing
                var chunks = SplitIntoChunks(messages, _workerCount);
                var tasks = new List<Task>(_workerCount);
                var workerFiles = new List<string>(_workerCount);
                
                // Process each chunk in parallel
                for (int i = 0; i < chunks.Count; i++)
                {
                    var chunk = chunks[i];
                    if (chunk.Count == 0)
                        continue;
                        
                    var writer = _parallelWriters[i];
                    var chunkId = $"{batchId}_part{i}";
                    
                    // Add task to process this chunk
                    tasks.Add(writer.WriteMessagesAsync(chunk, chunkId, cancellationToken));
                }
                
                // Wait for all parallel writes to complete
                await Task.WhenAll(tasks).ConfigureAwait(false);
                
                // Collect worker file paths after successful writes
                for (int i = 0; i < _parallelWriters.Count && i < chunks.Count; i++)
                {
                    if (chunks[i].Count == 0) continue;
                    var info = _parallelWriters[i].GetLastBatchInfo();
                    workerFiles.Add(info.FilePath);
                }

                // Create the output file path for the main file (keeping this for compatibility)
                string outputFilePath = Path.Combine(_outputDirectory, $"{batchId}.parquet");
                
                // Save all worker files to the output directory
                for (int i = 0; i < workerFiles.Count; i++)
                {
                    string destFilePath = Path.Combine(_outputDirectory, $"{batchId}_part{i}.parquet");
                    File.Copy(workerFiles[i], destFilePath, true);
                }                
                // Stop timing
                _writeStopwatch.Stop();
                
                // Calculate total size of all files for statistics
                long totalSizeBytes = workerFiles.Sum(f => new FileInfo(f).Length);
                double compressionRatio = estimatedSizeBytes > 0 
                    ? (double)estimatedSizeBytes / totalSizeBytes
                    : 1.0;
                
                // Update the last batch info
                _lastBatchInfo = new BatchWriteInfo
                {
                    BatchId = batchId,
                    FilePath = outputFilePath,
                    MessageCount = messages.Count,
                    FileSizeBytes = totalSizeBytes,
                    WriteTimeMs = _writeStopwatch.Elapsed.TotalMilliseconds,
                    CompressionRatio = compressionRatio
                };
                
                _logger.LogInformation(
                    "Saved {Count} worker files to output directory for batch {BatchId}. " +
                    "Total time: {ElapsedMs:F2}ms, Size: {SizeMB:F2}MB, Compression ratio: {Ratio:F2}",
                    workerFiles.Count,
                    batchId,
                    _writeStopwatch.Elapsed.TotalMilliseconds,
                    totalSizeBytes / 1024.0 / 1024.0,
                    compressionRatio);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in parallel write for batch {BatchId}: {Message}", batchId, ex.Message);
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
        }
        
        /// <summary>
        /// Splits a list of messages into approximately equal chunks for parallel processing
        /// </summary>
        private List<IReadOnlyList<T>> SplitIntoChunks(IReadOnlyList<T> messages, int chunkCount)
        {
            var result = new List<IReadOnlyList<T>>(chunkCount);
            
            if (messages.Count == 0)
            {
                for (int i = 0; i < chunkCount; i++)
                {
                    result.Add(new List<T>());
                }
                return result;
            }
            
            int baseChunkSize = messages.Count / chunkCount;
            int remainder = messages.Count % chunkCount;
            
            int startIndex = 0;
            for (int i = 0; i < chunkCount; i++)
            {
                int chunkSize = baseChunkSize + (i < remainder ? 1 : 0);
                if (chunkSize == 0)
                {
                    result.Add(new List<T>());
                    continue;
                }
                
                var chunk = new List<T>(chunkSize);
                for (int j = 0; j < chunkSize; j++)
                {
                    chunk.Add(messages[startIndex + j]);
                }
                
                result.Add(chunk);
                startIndex += chunkSize;
            }
            
            return result;
        }
        
        /// <summary>
        /// Gets information about the most recently written batch
        /// </summary>
        /// <returns>Batch information for the last write operation</returns>
        public BatchWriteInfo GetLastBatchInfo()
        {
            return _lastBatchInfo;
        }
        
        /// <summary>
        /// Disposes resources used by the writer
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_isDisposed)
                return;
                
            await _writeLock.WaitAsync().ConfigureAwait(false);
            
            try
            {
                _isDisposed = true;
                
                // Dispose all parallel writers
                foreach (var writer in _parallelWriters)
                {
                    await writer.DisposeAsync().ConfigureAwait(false);
                }
                
                // Consolidate all parquet files from temporary directory to output directory
                try
                {
                    if (Directory.Exists(_temporaryDirectory))
                    {
                        ConsolidateParquetFiles();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error consolidating parquet files: {Message}", ex.Message);
                }
                
                // Clean up the temporary directory
                try
                {
                    if (Directory.Exists(_temporaryDirectory))
                    {
                        Directory.Delete(_temporaryDirectory, true);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error cleaning up temporary directory: {Message}", ex.Message);
                }
                
                _writeLock.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing MultiFileParallelWriter: {Message}", ex.Message);
            }
        }
        
        /// <summary>
        /// Consolidates all parquet files from the temporary directory to the output directory
        /// </summary>
        private void ConsolidateParquetFiles()
        {
            try
            {
                // Find all parquet files in the temporary directory and its subdirectories
                var parquetFiles = Directory.GetFiles(_temporaryDirectory, "*.parquet", SearchOption.AllDirectories)
                    .OrderBy(f => f)
                    .ToList();
                
                if (parquetFiles.Count == 0)
                {
                    _logger.LogInformation("No parquet files found in temporary directory to consolidate");
                    return;
                }
                
                _logger.LogInformation("Consolidating {Count} parquet files from temporary workers", parquetFiles.Count);
                
                // Move each file to the output directory with a unique name
                foreach (var sourceFile in parquetFiles)
                {
                    var fileName = Path.GetFileName(sourceFile);
                    var destFile = Path.Combine(_outputDirectory, fileName);
                    
                    // If the file already exists in destination, make it unique
                    if (File.Exists(destFile))
                    {
                        var uniqueName = $"{Path.GetFileNameWithoutExtension(fileName)}_{Guid.NewGuid():N}{Path.GetExtension(fileName)}";
                        destFile = Path.Combine(_outputDirectory, uniqueName);
                    }
                    
                    File.Move(sourceFile, destFile);
                    _logger.LogDebug("Moved {Source} to {Dest}", Path.GetFileName(sourceFile), Path.GetFileName(destFile));
                }
                
                _logger.LogInformation("Successfully consolidated {Count} parquet files to output directory", parquetFiles.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during parquet file consolidation: {Message}", ex.Message);
                throw;
            }
        }
        
        /// <summary>
        /// Throws if the writer has been disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(MultiFileParallelWriter<T>));
        }
    }
} 
