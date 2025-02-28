using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using HubClient.Core.Resilience;

namespace HubClient.Production.Resilience
{
    /// <summary>
    /// A resilient gRPC client wrapper specifically for the Production library
    /// </summary>
    /// <typeparam name="TClient">The type of gRPC client to wrap</typeparam>
    public class ResilientGrpcClient<TClient> where TClient : ClientBase<TClient>
    {
        private readonly TClient _client;
        private readonly IGrpcResiliencePolicy _resiliencePolicy;
        
        /// <summary>
        /// Creates a new instance of ResilientGrpcClient
        /// </summary>
        /// <param name="client">The gRPC client to wrap</param>
        /// <param name="resiliencePolicy">The resilience policy to apply</param>
        public ResilientGrpcClient(TClient client, IGrpcResiliencePolicy resiliencePolicy)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _resiliencePolicy = resiliencePolicy ?? throw new ArgumentNullException(nameof(resiliencePolicy));
        }
        
        /// <summary>
        /// Gets the underlying client
        /// </summary>
        public TClient Client => _client;
        
        /// <summary>
        /// Gets the resilience policy
        /// </summary>
        public IGrpcResiliencePolicy ResiliencePolicy => _resiliencePolicy;
        
        /// <summary>
        /// Executes a client operation with resilience
        /// </summary>
        /// <typeparam name="TResponse">The response type</typeparam>
        /// <param name="operation">The operation to execute on the client</param>
        /// <param name="methodName">The name of the method being called (for telemetry)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The result of the operation</returns>
        public Task<TResponse> CallAsync<TResponse>(
            Func<TClient, CancellationToken, Task<TResponse>> operation,
            string? methodName = null,
            CancellationToken cancellationToken = default)
        {
            if (operation == null) throw new ArgumentNullException(nameof(operation));
            
            return _resiliencePolicy.ExecuteAsync(
                async (ct) => await operation(_client, ct),
                methodName,
                cancellationToken);
        }
        
        /// <summary>
        /// Executes a client operation with resilience (no result version)
        /// </summary>
        /// <param name="operation">The operation to execute on the client</param>
        /// <param name="methodName">The name of the method being called (for telemetry)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public Task CallAsync(
            Func<TClient, CancellationToken, Task> operation,
            string? methodName = null,
            CancellationToken cancellationToken = default)
        {
            if (operation == null) throw new ArgumentNullException(nameof(operation));
            
            return _resiliencePolicy.ExecuteAsync(
                async (ct) => await operation(_client, ct),
                methodName,
                cancellationToken);
        }
    }
} 