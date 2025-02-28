using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HubClient.Production.Extensions
{
    /// <summary>
    /// Extension methods for registering the optimized client with dependency injection
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds the OptimizedHubClient to the service collection with default options
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        /// <returns>The service collection for chaining</returns>
        public static IServiceCollection AddOptimizedHubClient(
            this IServiceCollection services,
            string serverEndpoint)
        {
            return services.AddOptimizedHubClient(serverEndpoint, options => { });
        }

        /// <summary>
        /// Adds the OptimizedHubClient to the service collection with custom options
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        /// <param name="configureOptions">Action to configure client options</param>
        /// <returns>The service collection for chaining</returns>
        public static IServiceCollection AddOptimizedHubClient(
            this IServiceCollection services,
            string serverEndpoint,
            Action<OptimizedHubClientOptions> configureOptions)
        {
            // Register the client as a singleton
            services.AddSingleton<OptimizedHubClient>(sp => {
                var options = new OptimizedHubClientOptions { ServerEndpoint = serverEndpoint };
                configureOptions(options);
                var logger = sp.GetService<ILogger<OptimizedHubClient>>();
                return new OptimizedHubClient(options, logger);
            });
            
            return services;
        }
        
        /// <summary>
        /// Adds the OptimizedHubClient to the service collection with workload-specific options for high throughput
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        /// <returns>The service collection for chaining</returns>
        public static IServiceCollection AddOptimizedHubClientForHighThroughput(
            this IServiceCollection services,
            string serverEndpoint)
        {
            return services.AddOptimizedHubClient(serverEndpoint, options => {
                options.ChannelCount = 8;
                options.BatchSize = 25000;
                options.SerializerType = HubClient.Core.Serialization.MessageSerializerFactory.SerializerType.UnsafeMemory;
                options.MaxConcurrentCalls = 1000;
            });
        }
        
        /// <summary>
        /// Adds the OptimizedHubClient to the service collection with workload-specific options for low latency
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        /// <returns>The service collection for chaining</returns>
        public static IServiceCollection AddOptimizedHubClientForLowLatency(
            this IServiceCollection services,
            string serverEndpoint)
        {
            return services.AddOptimizedHubClient(serverEndpoint, options => {
                options.ChannelCount = 16;
                options.BatchSize = 10000;
                options.SerializerType = HubClient.Core.Serialization.MessageSerializerFactory.SerializerType.UnsafeMemory;
                options.TimeoutMilliseconds = 5000;
            });
        }
        
        /// <summary>
        /// Adds the OptimizedHubClient to the service collection with workload-specific options for resource-constrained environments
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        /// <returns>The service collection for chaining</returns>
        public static IServiceCollection AddOptimizedHubClientForResourceConstrained(
            this IServiceCollection services,
            string serverEndpoint)
        {
            return services.AddOptimizedHubClient(serverEndpoint, options => {
                options.ChannelCount = 4;
                options.BatchSize = 5000;
                options.SerializerType = HubClient.Core.Serialization.MessageSerializerFactory.SerializerType.PooledBuffer;
                options.MaxConcurrentCalls = 250;
            });
        }
    }
} 