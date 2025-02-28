using Grpc.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Core.Resilience
{
    /// <summary>
    /// Interface for gRPC call resilience policies
    /// </summary>
    public interface IGrpcResiliencePolicy
    {
        /// <summary>
        /// Executes a gRPC operation with resilience (retry, circuit breaking, etc.)
        /// </summary>
        /// <typeparam name="TResponse">The response type</typeparam>
        /// <param name="operation">The operation to execute</param>
        /// <param name="context">Optional context information for the operation</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The result of the operation</returns>
        Task<TResponse> ExecuteAsync<TResponse>(
            Func<CancellationToken, Task<TResponse>> operation,
            string? context = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Executes a gRPC operation that returns no result with resilience
        /// </summary>
        /// <param name="operation">The operation to execute</param>
        /// <param name="context">Optional context information for the operation</param>
        /// <param name="cancellationToken">Cancellation token</param>
        Task ExecuteAsync(
            Func<CancellationToken, Task> operation,
            string? context = null,
            CancellationToken cancellationToken = default);
            
        /// <summary>
        /// Gets or sets whether telemetry should be collected for resilience operations
        /// </summary>
        bool EnableTelemetry { get; set; }
        
        /// <summary>
        /// Gets resilience metrics collected during execution
        /// </summary>
        ResilienceMetrics Metrics { get; }
    }
} 