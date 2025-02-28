using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using HubClient.Core.Grpc;
using HubClient.Core.Resilience;
using HubClient.Production.Resilience;

namespace HubClient.Production.Grpc
{
    /// <summary>
    /// Provides resilience patterns for gRPC clients, including circuit breaking, retries, and connection pooling
    /// Optimized based on benchmarking results
    /// </summary>
    /// <typeparam name="TClient">The type of the gRPC client</typeparam>
    public class ResilientGrpcClient<TClient> where TClient : ClientBase<TClient>
    {
        private readonly Func<GrpcChannel> _channelProvider;
        private readonly ConcurrentDictionary<string, IGrpcResiliencePolicy> _policies = new();
        private readonly Func<IGrpcResiliencePolicy> _resiliencePolicyFactory;
        private readonly SemaphoreSlim _semaphore;

        /// <summary>
        /// Create a new resilient gRPC client
        /// </summary>
        /// <param name="channelProvider">Function to provide a channel for this client</param>
        /// <param name="maxConcurrentCalls">Maximum concurrent calls allowed (default: 1000)</param>
        /// <param name="resiliencePolicyFactory">Optional factory for creating resilience policies</param>
        public ResilientGrpcClient(
            Func<GrpcChannel> channelProvider, 
            int maxConcurrentCalls = 1000,
            Func<IGrpcResiliencePolicy> resiliencePolicyFactory = null)
        {
            _channelProvider = channelProvider ?? throw new ArgumentNullException(nameof(channelProvider));
            _semaphore = new SemaphoreSlim(maxConcurrentCalls, maxConcurrentCalls);
            _resiliencePolicyFactory = resiliencePolicyFactory ?? (() => new OptimizedResiliencePolicy());
        }

        /// <summary>
        /// Execute a gRPC call with resilience patterns applied
        /// </summary>
        /// <typeparam name="TResponse">Type of the response</typeparam>
        /// <param name="operationKey">Key to identify this operation type (for policy isolation)</param>
        /// <param name="callFunc">Function to execute the gRPC call</param>
        /// <param name="timeout">Optional timeout</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The response from the gRPC call</returns>
        public async Task<TResponse> ExecuteAsync<TResponse>(
            string operationKey,
            Func<TClient, Task<TResponse>> callFunc,
            TimeSpan? timeout = null,
            CancellationToken cancellationToken = default)
        {
            // Get or create a policy for this operation
            var policy = _policies.GetOrAdd(operationKey, _ => _resiliencePolicyFactory());
            
            // Apply timeout if provided
            using var timeoutCts = timeout.HasValue 
                ? new CancellationTokenSource(timeout.Value) 
                : new CancellationTokenSource();
            
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, timeoutCts.Token);
            
            // Ensure we don't exceed max concurrent calls
            await _semaphore.WaitAsync(linkedCts.Token).ConfigureAwait(false);
            
            try
            {
                // Execute with resilience patterns
                return await policy.ExecuteAsync(async token =>
                {
                    // Create client and channel (channel manager handles pooling)
                    var channel = _channelProvider();
                    var client = (TClient)Activator.CreateInstance(typeof(TClient), channel);
                    
                    // Execute the call
                    return await callFunc(client).ConfigureAwait(false);
                }, operationKey, linkedCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
            {
                double timeoutSeconds = timeout?.TotalSeconds ?? 0;
                throw new TimeoutException($"The operation {operationKey} timed out after {timeoutSeconds} seconds");
            }
            finally
            {
                _semaphore.Release();
            }
        }

        /// <summary>
        /// Execute a server streaming gRPC call with resilience patterns applied
        /// </summary>
        /// <typeparam name="TResponse">Type of the response in the stream</typeparam>
        /// <param name="operationKey">Key to identify this operation type (for policy isolation)</param>
        /// <param name="callFunc">Function to execute the gRPC streaming call</param>
        /// <param name="timeout">Optional timeout</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The server streaming call</returns>
        public async Task<AsyncServerStreamingCall<TResponse>> ExecuteServerStreamingAsync<TResponse>(
            string operationKey,
            Func<TClient, AsyncServerStreamingCall<TResponse>> callFunc,
            TimeSpan? timeout = null,
            CancellationToken cancellationToken = default)
        {
            // Get or create a policy for this operation
            var policy = _policies.GetOrAdd(operationKey, _ => _resiliencePolicyFactory());
            
            // Apply timeout if provided
            using var timeoutCts = timeout.HasValue 
                ? new CancellationTokenSource(timeout.Value) 
                : new CancellationTokenSource();
            
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, timeoutCts.Token);
            
            // Ensure we don't exceed max concurrent calls
            await _semaphore.WaitAsync(linkedCts.Token).ConfigureAwait(false);
            
            try
            {
                // Execute with resilience patterns
                return await policy.ExecuteAsync(async token =>
                {
                    // Create client and channel (channel manager handles pooling)
                    var channel = _channelProvider();
                    var client = (TClient)Activator.CreateInstance(typeof(TClient), channel);
                    
                    // Execute the call - no need to await since it returns directly
                    return callFunc(client);
                }, operationKey, linkedCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
            {
                double timeoutSeconds = timeout?.TotalSeconds ?? 0;
                throw new TimeoutException($"The operation {operationKey} timed out after {timeoutSeconds} seconds");
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
} 