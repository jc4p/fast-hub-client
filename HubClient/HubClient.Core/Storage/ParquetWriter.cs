using System;
using System.Collections.Generic;
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

namespace HubClient.Core.Storage
{
    /// <summary>
    /// Writes messages to Parquet files for persistent storage
    /// </summary>
    /// <typeparam name="T">Type of message to write, must implement IMessage</typeparam>
    public class ParquetWriter<T> : IParquetWriter<T> where T : IMessage<T>, new()
    {
        private readonly string _outputDirectory;
        private readonly Func<T, IDictionary<string, object>> _messageConverter;
        private readonly CompressionMethod _compressionMethod;
        private readonly int _rowGroupSize;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly Stopwatch _writeStopwatch = new();
        private BatchWriteInfo _lastBatchInfo = new();
        private bool _isDisposed;
        private readonly ILogger<ParquetWriter<T>> _logger;
        private readonly ISchemaGenerator _schemaGenerator;
        
        /// <summary>
        /// Creates a new instance of the <see cref="ParquetWriter{T}"/> class
        /// </summary>
        /// <param name="outputDirectory">Directory where Parquet files will be written</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <param name="logger">Logger for logging information and warnings</param>
        /// <param name="schemaGenerator">Generator for Parquet schemas</param>
        /// <param name="compressionMethod">Compression method to use for Parquet files</param>
        /// <param name="rowGroupSize">Size of row groups in the Parquet file</param>
        public ParquetWriter(
            string outputDirectory,
            Func<T, IDictionary<string, object>> messageConverter,
            ILogger<ParquetWriter<T>> logger,
            ISchemaGenerator schemaGenerator,
            CompressionMethod compressionMethod = CompressionMethod.Snappy,
            int rowGroupSize = 5000)
        {
            _outputDirectory = !string.IsNullOrWhiteSpace(outputDirectory)
                ? outputDirectory
                : throw new ArgumentNullException(nameof(outputDirectory));
                
            _messageConverter = messageConverter ?? throw new ArgumentNullException(nameof(messageConverter));
            _compressionMethod = compressionMethod;
            _rowGroupSize = rowGroupSize;
            
            // Create the output directory if it doesn't exist
            Directory.CreateDirectory(_outputDirectory);
            
            _logger = logger;
            _schemaGenerator = schemaGenerator;
        }
        
        /// <summary>
        /// Writes a batch of messages to a Parquet file
        /// </summary>
        /// <param name="messages">Messages to write</param>
        /// <param name="batchId">Unique identifier for this batch of messages</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task that completes when the write is finished</returns>
        public async Task WriteMessagesAsync(IReadOnlyList<T> messages, string batchId, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            
            if (messages == null || messages.Count == 0)
                return;
                
            // Acquire the write lock
            await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            
            try
            {
                // Start timing the write
                _writeStopwatch.Restart();
                
                // Generate a file path for this batch
                string filePath = Path.Combine(_outputDirectory, $"{batchId}.parquet");
                
                // Create batches in memory first to estimate size before compression
                long estimatedSizeBytes = 0;
                var rows = new List<IDictionary<string, object>>(messages.Count);
                
                foreach (var message in messages)
                {
                    var row = _messageConverter(message);
                    rows.Add(row);
                    
                    // Rough estimate of size
                    estimatedSizeBytes += message.CalculateSize();
                }
                
                // Create schema based on the first row if available
                var schema = InferSchemaFromFirstRow(rows);
                
                // Write to Parquet file
                await WriteRowsToParquet(rows, filePath);
                
                // Stop timing the write
                _writeStopwatch.Stop();
                double writeTimeMs = _writeStopwatch.Elapsed.TotalMilliseconds;
                
                // Get file size
                var fileInfo = new FileInfo(filePath);
                long fileSizeBytes = fileInfo.Length;
                
                // Calculate compression ratio
                double compressionRatio = estimatedSizeBytes > 0 
                    ? (double)estimatedSizeBytes / fileSizeBytes 
                    : 1.0;
                
                // Update last batch info
                _lastBatchInfo = new BatchWriteInfo
                {
                    BatchId = batchId,
                    FilePath = filePath,
                    MessageCount = messages.Count,
                    FileSizeBytes = fileSizeBytes,
                    WriteTimeMs = writeTimeMs,
                    CompressionRatio = compressionRatio
                };
            }
            finally
            {
                // Release the write lock
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
        public ValueTask DisposeAsync()
        {
            if (_isDisposed)
                return ValueTask.CompletedTask;
                
            _isDisposed = true;
            _writeLock.Dispose();
            
            return ValueTask.CompletedTask;
        }
        
        /// <summary>
        /// Infers a Parquet schema from the first row in a dataset
        /// </summary>
        /// <param name="rows">Collection of rows</param>
        /// <returns>Inferred schema</returns>
        private static ParquetSchema InferSchemaFromFirstRow(IReadOnlyList<IDictionary<string, object>> rows)
        {
            if (rows.Count == 0)
                throw new ArgumentException("Cannot infer schema from empty dataset", nameof(rows));
                
            var firstRow = rows[0];
            var fields = new List<Field>();
            
            foreach (var kvp in firstRow)
            {
                var field = CreateDataField(kvp.Key, kvp.Value);
                if (field != null)
                {
                    fields.Add(field);
                }
            }
            
            return new ParquetSchema(fields);
        }
        
        /// <summary>
        /// Creates a data field for a schema based on a key and value
        /// </summary>
        /// <param name="name">Field name</param>
        /// <param name="value">Field value</param>
        /// <returns>Data field or null if type is not supported</returns>
        private static Field CreateDataField(string name, object value)
        {
            return value switch
            {
                null => new DataField<string>(name),
                string => new DataField<string>(name),
                int => new DataField<int>(name),
                long => new DataField<long>(name),
                float => new DataField<float>(name),
                double => new DataField<double>(name),
                bool => new DataField<bool>(name),
                DateTime => new DataField<DateTimeOffset>(name),
                DateTimeOffset => new DataField<DateTimeOffset>(name),
                byte[] => new DataField<byte[]>(name),
                _ => new DataField<string>(name) // Convert other types to string
            };
        }
        
        /// <summary>
        /// Writes rows to a Parquet file
        /// </summary>
        /// <param name="rows">Rows to write</param>
        /// <param name="filePath">Path to write the file</param>
        private async Task WriteRowsToParquet(List<IDictionary<string, object>> rows, string filePath)
        {
            if (rows.Count == 0)
            {
                _logger.LogWarning("No messages to write to Parquet file.");
                return;
            }

            // Create schema based on the first row
            var schema = InferSchemaFromFirstRow(rows);
            
            using (var fileStream = new FileStream(filePath, FileMode.Create))
            {
                using (var writer = await ParquetWriter.CreateAsync(schema, fileStream))
                {
                    // Set compression method after creating the writer
                    writer.CompressionMethod = _compressionMethod;
                    writer.CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;
                    
                    using (var rowGroup = writer.CreateRowGroup())
                    {
                        // Get column names from schema
                        var columnNames = schema.Fields.Select(f => f.Name).ToList();
                        
                        // Process data by column
                        foreach (var columnName in columnNames)
                        {
                            var field = schema.Fields.First(f => f.Name == columnName);
                            var values = ExtractColumnValues(rows, columnName);
                            
                            // Create DataColumn and write it
                            var dataColumn = CreateDataColumn(field, values);
                            await rowGroup.WriteColumnAsync(dataColumn);
                        }
                    }
                }
            }
            
            _logger.LogInformation($"Successfully wrote {rows.Count} messages to Parquet file: {filePath}");
        }
        
        /// <summary>
        /// Creates a DataColumn for a field with the provided values
        /// </summary>
        /// <param name="field">Field definition</param>
        /// <param name="values">Values for the column</param>
        /// <returns>Data column ready to write</returns>
        private static DataColumn CreateDataColumn(Field field, IList<object> values)
        {
            return field switch
            {
                DataField<string> stringField => new DataColumn(
                    stringField, 
                    values.Select(v => v as string).ToArray()),
                    
                DataField<int> intField => new DataColumn(
                    intField, 
                    values.Select(v => v is int i ? i : default).ToArray()),
                    
                DataField<long> longField => new DataColumn(
                    longField, 
                    values.Select(v => v is long l ? l : default).ToArray()),
                    
                DataField<float> floatField => new DataColumn(
                    floatField, 
                    values.Select(v => v is float f ? f : default).ToArray()),
                    
                DataField<double> doubleField => new DataColumn(
                    doubleField, 
                    values.Select(v => v is double d ? d : default).ToArray()),
                    
                DataField<bool> boolField => new DataColumn(
                    boolField, 
                    values.Select(v => v is bool b ? b : default).ToArray()),
                    
                DataField<DateTimeOffset> dateField => new DataColumn(
                    dateField, 
                    values.Select(v => v is DateTimeOffset dto ? dto : 
                                  v is DateTime dt ? new DateTimeOffset(dt) : default).ToArray()),
                                  
                DataField<byte[]> bytesField => new DataColumn(
                    bytesField, 
                    values.Select(v => v as byte[] ?? Array.Empty<byte>()).ToArray()),
                    
                _ => throw new NotSupportedException($"Unsupported field type: {field.GetType().Name}")
            };
        }
        
        /// <summary>
        /// Extracts values for a specific column from all rows
        /// </summary>
        private static IList<object> ExtractColumnValues(IReadOnlyList<IDictionary<string, object>> rows, string columnName)
        {
            var values = new List<object>(rows.Count);
            
            foreach (var row in rows)
            {
                if (row.TryGetValue(columnName, out var value))
                {
                    values.Add(value);
                }
                else
                {
                    values.Add(null);
                }
            }
            
            return values;
        }
        
        /// <summary>
        /// Throws an ObjectDisposedException if the writer is disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(ParquetWriter<T>));
            }
        }
    }
} 