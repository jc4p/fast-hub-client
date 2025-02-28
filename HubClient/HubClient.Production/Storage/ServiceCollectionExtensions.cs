using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using HubClient.Core.Storage;
using Google.Protobuf;
using System.Collections.Generic;

namespace HubClient.Production.Storage
{
    /// <summary>
    /// Extension methods for registering optimized storage components with the DI container
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds optimized storage services to the service collection
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="baseOutputDirectory">Base directory for output files</param>
        /// <returns>The service collection for chaining</returns>
        public static IServiceCollection AddOptimizedStorage(
            this IServiceCollection services,
            string baseOutputDirectory)
        {
            // Register the schema generator if not already registered
            services.TryAddSingleton<ISchemaGenerator, SchemaGenerator>();
            
            // Register the optimized storage factory
            services.AddSingleton<OptimizedStorageFactory>(provider => new OptimizedStorageFactory(
                provider.GetRequiredService<ILoggerFactory>(),
                provider.GetRequiredService<ISchemaGenerator>(),
                baseOutputDirectory));
                
            // Register the storage factory interface
            services.AddSingleton<IStorageFactory>(provider => provider.GetRequiredService<OptimizedStorageFactory>());
            
            return services;
        }
        
        /// <summary>
        /// Adds specific storage optimizations for high-throughput workloads
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="baseOutputDirectory">Base directory for output files</param>
        /// <param name="workerCount">Number of parallel workers (defaults to CPU core count, max 8)</param>
        /// <returns>The service collection for chaining</returns>
        public static IServiceCollection AddHighThroughputStorage(
            this IServiceCollection services,
            string baseOutputDirectory,
            int workerCount = 0)
        {
            // Register dependencies
            services.AddOptimizedStorage(baseOutputDirectory);
            
            // Register a factory delegate for creating high-throughput storage
            services.AddSingleton<IStorageFactoryDelegate>(provider => 
            {
                var factory = provider.GetRequiredService<OptimizedStorageFactory>();
                return new HighThroughputStorageFactoryDelegate(factory, workerCount);
            });
            
            return services;
        }
        
        /// <summary>
        /// Extension method to check if a service is already registered and add it if not
        /// </summary>
        private static IServiceCollection TryAddSingleton<TService, TImplementation>(this IServiceCollection services)
            where TService : class
            where TImplementation : class, TService
        {
            var descriptor = services.FirstOrDefault(d => 
                d.ServiceType == typeof(TService) &&
                d.ImplementationType == typeof(TImplementation));
                
            if (descriptor == null)
            {
                services.AddSingleton<TService, TImplementation>();
            }
            
            return services;
        }
    }
    
    /// <summary>
    /// Factory delegate for creating storage instances
    /// </summary>
    public interface IStorageFactoryDelegate
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
            System.Func<T, System.Collections.Generic.IDictionary<string, object>> messageConverter)
            where T : Google.Protobuf.IMessage<T>, new();
    }
    
    /// <summary>
    /// Factory delegate for creating high-throughput storage
    /// </summary>
    internal class HighThroughputStorageFactoryDelegate : IStorageFactoryDelegate
    {
        private readonly OptimizedStorageFactory _factory;
        private readonly int _workerCount;
        
        public HighThroughputStorageFactoryDelegate(OptimizedStorageFactory factory, int workerCount)
        {
            _factory = factory;
            _workerCount = workerCount;
        }
        
        public IIntermediateStorage<T> CreateStorage<T>(
            string outputDirectoryName,
            System.Func<T, System.Collections.Generic.IDictionary<string, object>> messageConverter)
            where T : Google.Protobuf.IMessage<T>, new()
        {
            return _factory.CreateHighThroughputStorage<T>(
                outputDirectoryName,
                messageConverter,
                _workerCount);
        }
    }
} 