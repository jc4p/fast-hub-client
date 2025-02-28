using Microsoft.Extensions.DependencyInjection;

namespace HubClient.Core.Storage
{
    /// <summary>
    /// Extension methods for registering storage-related services
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds core storage services to the service collection
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <returns>The service collection for chaining</returns>
        public static IServiceCollection AddStorageServices(this IServiceCollection services)
        {
            // Register ISchemaGenerator implementation
            services.AddSingleton<ISchemaGenerator, SchemaGenerator>();
            
            return services;
        }
    }
} 