using System;
using Google.Protobuf;
using HubClient.Core.Storage;
using System.Collections.Generic;

namespace HubClient.Production.Storage
{
    /// <summary>
    /// Factory for creating storage instances
    /// </summary>
    public interface IStorageFactory
    {
        /// <summary>
        /// Creates an intermediate storage instance for the specified message type
        /// </summary>
        /// <typeparam name="T">Type of message to store</typeparam>
        /// <param name="outputDirectoryName">Name of subdirectory for output</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <returns>An intermediate storage instance</returns>
        IIntermediateStorage<T> CreateStorage<T>(
            string outputDirectoryName,
            Func<T, IDictionary<string, object>> messageConverter)
            where T : IMessage<T>, new();
            
        /// <summary>
        /// Creates a high-throughput storage instance for the specified message type
        /// </summary>
        /// <typeparam name="T">Type of message to store</typeparam>
        /// <param name="outputDirectoryName">Name of subdirectory for output</param>
        /// <param name="messageConverter">Function to convert messages to row dictionaries</param>
        /// <param name="workerCount">Number of parallel workers to use</param>
        /// <returns>An intermediate storage instance optimized for high throughput</returns>
        IIntermediateStorage<T> CreateHighThroughputStorage<T>(
            string outputDirectoryName,
            Func<T, IDictionary<string, object>> messageConverter,
            int workerCount = 0)
            where T : IMessage<T>, new();
    }
} 