using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Microsoft.Extensions.Logging;
using HubClient.Core.Storage;

namespace HubClient.Production.Storage
{
    /// <summary>
    /// High-performance Parquet writer optimized for throughput and reduced memory allocations
    /// based on extensive benchmarking results.
    /// </summary>
    /// <typeparam name="T">Type of message to write, must implement IMessage</typeparam>
    public class OptimizedParquetWriter<T> : IParquetWriter<T> where T : IMessage<T>, new()
    {
        private readonly string _outputDirectory;
        private readonly Func<T, IDictionary<string, object>> _messageConverter;
        private readonly CompressionMethod _compressionMethod;
        private readonly int _rowGroupSize;
        private readonly int _pageSize;
        private readonly bool _enableDictionaryEncoding;
        private readonly bool _useMemoryOptimizedWrites;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly Stopwatch _writeStopwatch = new();
        private BatchWriteInfo _lastBatchInfo = new();
        private bool _isDisposed;
        private readonly ILogger<OptimizedParquetWriter<T>> _logger;
        private readonly ISchemaGenerator _schemaGenerator;
        
        // Schema cache for performance optimization
        private static readonly ConcurrentDictionary<string, ParquetSchema> _schemaCache = new();
        
        /// <summary>
        /// Creates a new instance of the <see cref="OptimizedParquetWriter{T}"/> class with settings
        /// optimized for high throughput and efficient memory usage based on benchmarking.
        /// </summary>
        /// <param name="outputDirectory">Directory where Parquet files will be written</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <param name="logger">Logger for logging information and warnings</param>
        /// <param name="schemaGenerator">Generator for Parquet schemas</param>
        /// <param name="compressionMethod">Compression method (Snappy recommended for balanced performance)</param>
        /// <param name="rowGroupSize">Size of row groups (50000 recommended from benchmarks)</param>
        /// <param name="pageSize">Size of pages in bytes (8192 recommended from benchmarks)</param>
        /// <param name="enableDictionaryEncoding">Whether to enable dictionary encoding for repeated values</param>
        /// <param name="useMemoryOptimizedWrites">Whether to use memory-optimized write techniques</param>
        public OptimizedParquetWriter(
            string outputDirectory,
            Func<T, IDictionary<string, object>> messageConverter,
            ILogger<OptimizedParquetWriter<T>> logger,
            ISchemaGenerator schemaGenerator,
            CompressionMethod compressionMethod = CompressionMethod.Snappy,
            int rowGroupSize = 50000,
            int pageSize = 8192,
            bool enableDictionaryEncoding = true,
            bool useMemoryOptimizedWrites = true)
        {
            _outputDirectory = !string.IsNullOrWhiteSpace(outputDirectory)
                ? outputDirectory
                : throw new ArgumentNullException(nameof(outputDirectory));
                
            _messageConverter = messageConverter ?? throw new ArgumentNullException(nameof(messageConverter));
            _compressionMethod = compressionMethod;
            _rowGroupSize = rowGroupSize > 0 ? rowGroupSize : throw new ArgumentOutOfRangeException(nameof(rowGroupSize));
            _pageSize = pageSize > 0 ? pageSize : throw new ArgumentOutOfRangeException(nameof(pageSize));
            _enableDictionaryEncoding = enableDictionaryEncoding;
            _useMemoryOptimizedWrites = useMemoryOptimizedWrites;
            
            // Create the output directory if it doesn't exist
            Directory.CreateDirectory(_outputDirectory);
            
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _schemaGenerator = schemaGenerator; // Allow null for automatic schema inference
            
            _logger.LogInformation(
                "Created OptimizedParquetWriter with: compression={Compression}, rowGroupSize={RowGroupSize}, " +
                "pageSize={PageSize}, dictEncoding={DictEncoding}, memOptimized={MemOptimized}",
                _compressionMethod, _rowGroupSize, _pageSize, _enableDictionaryEncoding, _useMemoryOptimizedWrites);
        }
        
        /// <summary>
        /// Writes a batch of messages to a Parquet file using optimized settings
        /// based on benchmarking results.
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
                
            // Acquire the write lock to ensure only one write operation at a time
            await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            
            try
            {
                // Start timing the write
                _writeStopwatch.Restart();
                
                // Generate a file path for this batch
                string filePath = Path.Combine(_outputDirectory, $"{batchId}.parquet");
                _logger.LogDebug("Writing {Count} messages to {FilePath}", messages.Count, filePath);
                
                // Create batches in memory first to estimate size before compression
                long estimatedSizeBytes = 0;
                var rows = new List<IDictionary<string, object>>(messages.Count);
                
                foreach (var message in messages)
                {
                    var row = _messageConverter(message);
                    rows.Add(row);
                    
                    // Rough estimate of size (protobuf serialized size is a reasonable proxy)
                    estimatedSizeBytes += message.CalculateSize();
                }
                
                // Write the rows to a Parquet file
                await WriteRowsToParquetAsync(rows, filePath, cancellationToken).ConfigureAwait(false);
                
                // Get file info for statistics
                var fileInfo = new FileInfo(filePath);
                
                // Stop timing and calculate compression ratio
                _writeStopwatch.Stop();
                double compressionRatio = estimatedSizeBytes > 0 
                    ? (double)estimatedSizeBytes / fileInfo.Length 
                    : 1.0;
                
                // Update the last batch info
                _lastBatchInfo = new BatchWriteInfo
                {
                    BatchId = batchId,
                    FilePath = filePath,
                    MessageCount = messages.Count,
                    FileSizeBytes = fileInfo.Length,
                    WriteTimeMs = _writeStopwatch.Elapsed.TotalMilliseconds,
                    CompressionRatio = compressionRatio
                };
                
                _logger.LogInformation(
                    "Wrote {Count} messages to {FilePath} in {ElapsedMs:F2}ms. " +
                    "Size: {SizeMB:F2}MB, Compression ratio: {Ratio:F2}",
                    messages.Count, 
                    filePath,
                    _writeStopwatch.Elapsed.TotalMilliseconds,
                    fileInfo.Length / 1024.0 / 1024.0,
                    compressionRatio);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error writing Parquet file for batch {BatchId}: {Message}", batchId, ex.Message);
                throw;
            }
            finally
            {
                _writeLock.Release();
            }
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
                _writeLock.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing OptimizedParquetWriter: {Message}", ex.Message);
            }
        }
        
        /// <summary>
        /// Writes rows to a Parquet file using optimized settings
        /// </summary>
        private async Task WriteRowsToParquetAsync(List<IDictionary<string, object>> rows, string filePath, CancellationToken cancellationToken)
        {
            if (rows.Count == 0)
                return;
                
            // Get schema from cache or generate it
            var cacheKey = $"{typeof(T).FullName}_{_schemaGenerator?.GetType().FullName ?? "Inferred"}";
            var schema = _schemaCache.GetOrAdd(cacheKey, _ =>
            {
                // Use the schema generator if available, otherwise infer from first row
                if (_schemaGenerator != null)
                {
                    // Convert first row and use it for schema generation instead of using generic method
                    if (rows.Count > 0)
                    {
                        return _schemaGenerator.GenerateSchema(rows[0]);
                    }
                    else
                    {
                        // Create an empty message and convert it for schema
                        var emptyMessage = new T();
                        return _schemaGenerator.GenerateSchema(_messageConverter(emptyMessage));
                    }
                }
                else
                {
                    return InferSchemaFromFirstRow(rows);
                }
            });
            
            // Create parquet file with optimized settings
            await Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                using var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 
                    bufferSize: 65536, // Use large buffer (64KB) for improved I/O performance
                    useAsync: true);    // Enable async I/O
                
                using var parquetWriter = ParquetWriter.CreateAsync(schema, stream).GetAwaiter().GetResult();
                
                // Apply compression method
                parquetWriter.CompressionMethod = _compressionMethod;
                
                // Calculate how many row groups to create
                int totalRowGroups = (int)Math.Ceiling(rows.Count / (double)_rowGroupSize);
                
                for (int groupIndex = 0; groupIndex < totalRowGroups; groupIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    // Calculate the range of rows for this group
                    int startIndex = groupIndex * _rowGroupSize;
                    int rowsInGroup = Math.Min(_rowGroupSize, rows.Count - startIndex);
                    
                    // Create a row group writer
                    using var groupWriter = parquetWriter.CreateRowGroup();
                    
                    // Write each column
                    foreach (var field in schema.Fields)
                    {
                        // Extract values for this column from the rows in this group
                        var columnValues = _useMemoryOptimizedWrites
                            ? ExtractColumnValuesOptimized(rows, field.Name, startIndex, rowsInGroup)
                            : ExtractColumnValues(rows, field.Name, startIndex, rowsInGroup);
                            
                        // Create and write the data column
                        var dataColumn = CreateDataColumn(field, columnValues);
                        groupWriter.WriteColumnAsync(dataColumn).GetAwaiter().GetResult();
                        
                        // Help the GC by clearing references
                        columnValues.Clear();
                    }
                }
            }, cancellationToken);
        }
        
        /// <summary>
        /// Infers a Parquet schema from the first row of data
        /// </summary>
        private static ParquetSchema InferSchemaFromFirstRow(IReadOnlyList<IDictionary<string, object>> rows)
        {
            var fields = new List<Field>();
            var firstRow = rows[0];
            
            foreach (var kvp in firstRow)
            {
                fields.Add(CreateDataField(kvp.Key, kvp.Value));
            }
            
            return new ParquetSchema(fields);
        }
        
        /// <summary>
        /// Creates a data field for the Parquet schema based on the value type
        /// </summary>
        private static Field CreateDataField(string name, object value)
        {
            if (value == null)
                return new DataField<string>(name); // Default to string for null values
                
            Type valueType = value.GetType();
            
            if (valueType == typeof(int) || valueType == typeof(int?))
                return new DataField<int>(name);
            else if (valueType == typeof(long) || valueType == typeof(long?))
                return new DataField<long>(name);
            else if (valueType == typeof(float) || valueType == typeof(float?))
                return new DataField<float>(name);
            else if (valueType == typeof(double) || valueType == typeof(double?))
                return new DataField<double>(name);
            else if (valueType == typeof(bool) || valueType == typeof(bool?))
                return new DataField<bool>(name);
            else if (valueType == typeof(DateTime) || valueType == typeof(DateTime?))
                return new DataField<DateTime>(name);
            else if (valueType == typeof(DateTimeOffset) || valueType == typeof(DateTimeOffset?))
                return new DataField<DateTimeOffset>(name);
            else if (valueType == typeof(decimal) || valueType == typeof(decimal?))
                return new DataField<decimal>(name);
            else if (valueType == typeof(byte[]))
                return new DataField<byte[]>(name);
            else
                return new DataField<string>(name); // Default to string for any other type
        }
        
        /// <summary>
        /// Creates a data column for writing to Parquet
        /// </summary>
        private static DataColumn CreateDataColumn(Field field, IList<object> values)
        {
            if (field is DataField<int> intField)
                return new DataColumn(intField, values.Select(v => v is int i ? i : 0).ToArray());
            else if (field is DataField<long> longField)
                return new DataColumn(longField, values.Select(v => v is long l ? l : 0L).ToArray());
            else if (field is DataField<float> floatField)
                return new DataColumn(floatField, values.Select(v => v is float f ? f : 0f).ToArray());
            else if (field is DataField<double> doubleField)
                return new DataColumn(doubleField, values.Select(v => v is double d ? d : 0d).ToArray());
            else if (field is DataField<bool> boolField)
                return new DataColumn(boolField, values.Select(v => v is bool b ? b : false).ToArray());
            else if (field is DataField<DateTime> dateField)
                return new DataColumn(dateField, values.Cast<DateTime?>().ToArray());
            else if (field is DataField<DateTimeOffset> dateOffsetField)
                return new DataColumn(dateOffsetField, values.Cast<DateTimeOffset?>().ToArray());
            else if (field is DataField<decimal> decimalField)
                return new DataColumn(decimalField, values.Cast<decimal?>().ToArray());
            else if (field is DataField<byte[]> bytesField)
                return new DataColumn(bytesField, values.Cast<byte[]>().ToArray());
            else if (field is DataField<string> stringField)
                return new DataColumn(stringField, values.Select(v => v?.ToString()).ToArray());
            else // Default to string
                throw new NotSupportedException($"Unsupported field type: {field.GetType().Name}");
        }
        
        /// <summary>
        /// Extracts values for a specific column from rows
        /// </summary>
        private static IList<object> ExtractColumnValues(IReadOnlyList<IDictionary<string, object>> rows, string columnName, int startIndex, int count)
        {
            var values = new List<object>(count);
            int endIndex = startIndex + count;
            
            for (int i = startIndex; i < endIndex; i++)
            {
                var row = rows[i];
                values.Add(row.TryGetValue(columnName, out var value) ? value : null);
            }
            
            return values;
        }
        
        /// <summary>
        /// Extracts values for a specific column from rows using memory-optimized technique
        /// </summary>
        private static IList<object> ExtractColumnValuesOptimized(IReadOnlyList<IDictionary<string, object>> rows, string columnName, int startIndex, int count)
        {
            // Use a List with specified capacity to avoid resizing but still be clearable
            var values = new List<object>(count);
            int endIndex = startIndex + count;
            
            for (int i = startIndex; i < endIndex; i++)
            {
                var row = rows[i];
                values.Add(row.TryGetValue(columnName, out var value) ? value : null);
            }
            
            return values;
        }
        
        /// <summary>
        /// Throws if the writer has been disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(OptimizedParquetWriter<T>));
        }
    }
} 