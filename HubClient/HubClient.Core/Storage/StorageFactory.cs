using System;
using System.IO;
using Google.Protobuf;
using HubClient.Core.Grpc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Parquet;

namespace HubClient.Core.Storage
{
    /// <summary>
    /// Factory for creating storage components with pre-configured settings
    /// </summary>
    public class StorageFactory
    {
        private readonly IServiceProvider _serviceProvider;
        
        /// <summary>
        /// Creates a new instance of the StorageFactory
        /// </summary>
        /// <param name="serviceProvider">Service provider to resolve dependencies</param>
        public StorageFactory(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        }
        
        /// <summary>
        /// Creates an intermediate storage system for Message objects
        /// </summary>
        /// <param name="outputDirectory">Directory to store Parquet files</param>
        /// <param name="batchSize">Number of messages to buffer before writing to disk</param>
        /// <param name="compressionMethod">Compression method for Parquet files</param>
        /// <param name="rowGroupSize">Size of row groups in Parquet files</param>
        /// <returns>Configured intermediate storage</returns>
        public IIntermediateStorage<Message> CreateMessageStorage(
            string outputDirectory,
            int batchSize = 10000,
            CompressionMethod compressionMethod = CompressionMethod.Snappy,
            int rowGroupSize = 5000)
        {
            // Ensure output directory exists and is properly formatted
            if (string.IsNullOrWhiteSpace(outputDirectory))
                throw new ArgumentNullException(nameof(outputDirectory));
                
            outputDirectory = Path.GetFullPath(outputDirectory);
            Directory.CreateDirectory(outputDirectory);
            
            // Create a timestamp-based subdirectory to prevent overwriting
            var timestampedDir = Path.Combine(
                outputDirectory,
                $"messages_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            
            Directory.CreateDirectory(timestampedDir);
            
            // Get required dependencies
            var logger = _serviceProvider.GetRequiredService<ILogger<ParquetWriter<Message>>>();
            var schemaGenerator = _serviceProvider.GetRequiredService<ISchemaGenerator>();
            
            // Create a Parquet writer
            var parquetWriter = new ParquetWriter<Message>(
                timestampedDir,
                MessageParquetConverter.CreateConverter(),
                logger,
                schemaGenerator,
                compressionMethod,
                rowGroupSize);
                
            // Create and return the message buffer
            return new MessageBuffer<Message>(parquetWriter, batchSize);
        }
        
        /// <summary>
        /// Creates an intermediate storage system for a specific message type
        /// </summary>
        /// <typeparam name="T">Message type to store</typeparam>
        /// <param name="outputDirectory">Directory to store Parquet files</param>
        /// <param name="messageConverter">Function to convert messages to rows</param>
        /// <param name="batchSize">Number of messages to buffer before writing to disk</param>
        /// <param name="compressionMethod">Compression method for Parquet files</param>
        /// <param name="rowGroupSize">Size of row groups in Parquet files</param>
        /// <returns>Configured intermediate storage</returns>
        public IIntermediateStorage<T> CreateStorage<T>(
            string outputDirectory,
            Func<T, System.Collections.Generic.IDictionary<string, object>> messageConverter,
            int batchSize = 10000,
            CompressionMethod compressionMethod = CompressionMethod.Snappy,
            int rowGroupSize = 5000) where T : IMessage<T>, new()
        {
            // Ensure output directory exists and is properly formatted
            if (string.IsNullOrWhiteSpace(outputDirectory))
                throw new ArgumentNullException(nameof(outputDirectory));
                
            outputDirectory = Path.GetFullPath(outputDirectory);
            Directory.CreateDirectory(outputDirectory);
            
            // Create a timestamp-based subdirectory to prevent overwriting
            var typeName = typeof(T).Name.ToLowerInvariant();
            var timestampedDir = Path.Combine(
                outputDirectory,
                $"{typeName}_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            
            Directory.CreateDirectory(timestampedDir);
            
            // Get required dependencies
            var logger = _serviceProvider.GetRequiredService<ILogger<ParquetWriter<T>>>();
            var schemaGenerator = _serviceProvider.GetRequiredService<ISchemaGenerator>();
            
            // Create a Parquet writer
            var parquetWriter = new ParquetWriter<T>(
                timestampedDir,
                messageConverter,
                logger,
                schemaGenerator,
                compressionMethod,
                rowGroupSize);
                
            // Create and return the message buffer
            return new MessageBuffer<T>(parquetWriter, batchSize);
        }
    }
} 