using System;
using System.Collections.Generic;
using System.IO;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using HubClient.Core.Storage;
using Parquet;

namespace HubClient.Production.Storage
{
    /// <summary>
    /// Factory for creating optimized storage components based on benchmarking results.
    /// Provides different storage strategies tailored to specific workloads.
    /// </summary>
    public class OptimizedStorageFactory : IStorageFactory
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ISchemaGenerator _schemaGenerator;
        private readonly string _baseDirectory;
        private bool _isDisposed;
        
        /// <summary>
        /// Creates a new instance of the <see cref="OptimizedStorageFactory"/> class
        /// </summary>
        /// <param name="loggerFactory">Factory for creating loggers</param>
        /// <param name="schemaGenerator">Generator for Parquet schemas</param>
        /// <param name="baseDirectory">Base directory for storage output</param>
        public OptimizedStorageFactory(
            ILoggerFactory loggerFactory,
            ISchemaGenerator schemaGenerator,
            string baseDirectory)
        {
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _schemaGenerator = schemaGenerator ?? throw new ArgumentNullException(nameof(schemaGenerator));
            _baseDirectory = !string.IsNullOrWhiteSpace(baseDirectory)
                ? baseDirectory
                : throw new ArgumentNullException(nameof(baseDirectory));
                
            // Create the base directory if it doesn't exist
            Directory.CreateDirectory(_baseDirectory);
        }
        
        /// <summary>
        /// Creates an optimized storage solution for general-purpose workloads.
        /// This is the recommended default option based on benchmarking results.
        /// Uses OptimizedParquetWriter with Snappy compression and 50,000 row groups.
        /// </summary>
        /// <typeparam name="T">Type of message to store</typeparam>
        /// <param name="outputDirectoryName">Name of subdirectory for output files</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <returns>An intermediate storage implementation optimized for general-purpose use</returns>
        public IIntermediateStorage<T> CreateOptimizedStorage<T>(
            string outputDirectoryName, 
            Func<T, System.Collections.Generic.IDictionary<string, object>> messageConverter) 
            where T : IMessage<T>, new()
        {
            ThrowIfDisposed();
            
            var outputDirectory = Path.Combine(_baseDirectory, outputDirectoryName);
            Directory.CreateDirectory(outputDirectory);
            
            var logger = _loggerFactory.CreateLogger<OptimizedMessageBuffer<T>>();
            var writerLogger = _loggerFactory.CreateLogger<OptimizedParquetWriter<T>>();
            
            var parquetWriter = new OptimizedParquetWriter<T>(
                outputDirectory,
                messageConverter,
                writerLogger,
                _schemaGenerator,
                CompressionMethod.Snappy,  // Balanced performance/compression
                50000,                     // Optimal row group size from benchmarks
                8192,                      // Optimal page size from benchmarks
                true,                      // Enable dictionary encoding
                true);                     // Use memory-optimized writes
                
            return new OptimizedMessageBuffer<T>(
                parquetWriter,
                logger,
                25000,                     // Optimal batch size from benchmarks
                true);                     // Use background flushing
        }
        
        /// <summary>
        /// Creates a high-throughput storage solution for large workloads.
        /// Uses MultiFileParallelWriter for maximum throughput on multi-core systems.
        /// </summary>
        /// <typeparam name="T">Type of message to store</typeparam>
        /// <param name="outputDirectoryName">Name of subdirectory for output files</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <param name="workerCount">Number of parallel writers (defaults to CPU core count, max 8)</param>
        /// <returns>An intermediate storage implementation optimized for high-throughput</returns>
        public IIntermediateStorage<T> CreateHighThroughputStorage<T>(
            string outputDirectoryName, 
            Func<T, System.Collections.Generic.IDictionary<string, object>> messageConverter,
            int workerCount = 0) 
            where T : IMessage<T>, new()
        {
            ThrowIfDisposed();
            
            var outputDirectory = Path.Combine(_baseDirectory, outputDirectoryName);
            Directory.CreateDirectory(outputDirectory);
            
            var logger = _loggerFactory.CreateLogger<OptimizedMessageBuffer<T>>();
            var writerLogger = _loggerFactory.CreateLogger<MultiFileParallelWriter<T>>();
            
            var parallelWriter = new MultiFileParallelWriter<T>(
                outputDirectory,
                messageConverter,
                writerLogger,
                _schemaGenerator,
                CompressionMethod.Snappy,  // Balanced performance/compression
                100000,                    // Larger row groups for high-volume data
                8192,                      // Optimal page size from benchmarks
                true,                      // Enable dictionary encoding
                workerCount);              // Use provided worker count or default
                
            return new OptimizedMessageBuffer<T>(
                parallelWriter,
                logger,
                25000,                     // Optimal batch size from benchmarks
                true);                     // Use background flushing
        }
        
        /// <summary>
        /// Creates a storage solution optimized for small, frequent messages.
        /// Uses smaller row groups and batch sizes for more frequent writes.
        /// </summary>
        /// <typeparam name="T">Type of message to store</typeparam>
        /// <param name="outputDirectoryName">Name of subdirectory for output files</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <returns>An intermediate storage implementation optimized for small, frequent messages</returns>
        public IIntermediateStorage<T> CreateSmallMessageStorage<T>(
            string outputDirectoryName, 
            Func<T, System.Collections.Generic.IDictionary<string, object>> messageConverter) 
            where T : IMessage<T>, new()
        {
            ThrowIfDisposed();
            
            var outputDirectory = Path.Combine(_baseDirectory, outputDirectoryName);
            Directory.CreateDirectory(outputDirectory);
            
            var logger = _loggerFactory.CreateLogger<OptimizedMessageBuffer<T>>();
            var writerLogger = _loggerFactory.CreateLogger<OptimizedParquetWriter<T>>();
            
            var parquetWriter = new OptimizedParquetWriter<T>(
                outputDirectory,
                messageConverter,
                writerLogger,
                _schemaGenerator,
                CompressionMethod.Snappy,  // Balanced performance/compression
                10000,                     // Smaller row groups for more frequent writes
                4096,                      // Smaller page size for smaller messages
                true,                      // Enable dictionary encoding
                true);                     // Use memory-optimized writes
                
            return new OptimizedMessageBuffer<T>(
                parquetWriter,
                logger,
                5000,                      // Smaller batch size for more frequent commits
                true);                     // Use background flushing
        }
        
        /// <summary>
        /// Creates a direct writer for Parquet files without buffering.
        /// Use this when you want direct control over when files are written.
        /// </summary>
        /// <typeparam name="T">Type of message to write</typeparam>
        /// <param name="outputDirectoryName">Name of subdirectory for output files</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <returns>A Parquet writer optimized based on benchmarking results</returns>
        public IParquetWriter<T> CreateOptimizedParquetWriter<T>(
            string outputDirectoryName, 
            Func<T, System.Collections.Generic.IDictionary<string, object>> messageConverter) 
            where T : IMessage<T>, new()
        {
            ThrowIfDisposed();
            
            var outputDirectory = Path.Combine(_baseDirectory, outputDirectoryName);
            Directory.CreateDirectory(outputDirectory);
            
            var writerLogger = _loggerFactory.CreateLogger<OptimizedParquetWriter<T>>();
            
            return new OptimizedParquetWriter<T>(
                outputDirectory,
                messageConverter,
                writerLogger,
                _schemaGenerator,
                CompressionMethod.Snappy,  // Balanced performance/compression
                50000,                     // Optimal row group size from benchmarks
                8192,                      // Optimal page size from benchmarks
                true,                      // Enable dictionary encoding
                true);                     // Use memory-optimized writes
        }
        
        /// <summary>
        /// Creates an intermediate storage instance for the specified message type.
        /// Implements the IStorageFactory interface method.
        /// </summary>
        /// <typeparam name="T">Type of message to store</typeparam>
        /// <param name="outputDirectoryName">Name of subdirectory for output</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <returns>An intermediate storage instance</returns>
        public IIntermediateStorage<T> CreateStorage<T>(
            string outputDirectoryName,
            Func<T, IDictionary<string, object>> messageConverter)
            where T : IMessage<T>, new()
        {
            // By default, use the optimized storage implementation
            return CreateOptimizedStorage<T>(outputDirectoryName, messageConverter);
        }
        
        /// <summary>
        /// Disposes resources used by the factory
        /// </summary>
        public void Dispose()
        {
            if (_isDisposed)
                return;
                
            _isDisposed = true;
        }
        
        /// <summary>
        /// Throws if the factory has been disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(OptimizedStorageFactory));
        }
    }
} 