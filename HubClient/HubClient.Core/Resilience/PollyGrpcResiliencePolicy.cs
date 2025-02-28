using Grpc.Core;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace HubClient.Core.Resilience
{
    /// <summary>
    /// Implements a resilience policy for gRPC operations using Polly
    /// </summary>
    public class PollyGrpcResiliencePolicy : IGrpcResiliencePolicy
    {
        private readonly ResilienceOptions _options;
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly AsyncCircuitBreakerPolicy _circuitBreaker;
        private readonly ResilienceMetrics _metrics = new();
        private readonly Random _jitterer = new();

        /// <summary>
        /// Creates a new instance of the PollyGrpcResiliencePolicy with default options
        /// </summary>
        public PollyGrpcResiliencePolicy() 
            : this(new ResilienceOptions())
        {
        }

        /// <summary>
        /// Creates a new instance of the PollyGrpcResiliencePolicy with the specified options
        /// </summary>
        /// <param name="options">Resilience options</param>
        public PollyGrpcResiliencePolicy(ResilienceOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            
            // Create retry policy with exponential backoff and jitter
            _retryPolicy = Policy
                .Handle<RpcException>(IsTransientError)
                .WaitAndRetryAsync(
                    retryCount: options.MaxRetryAttempts,
                    sleepDurationProvider: (retryAttempt, context) =>
                    {
                        // Calculate base delay with exponential backoff
                        var baseDelay = TimeSpan.FromMilliseconds(
                            Math.Pow(options.RetryBackoffFactor, retryAttempt) * options.RetryBackoffBaseMs);
                            
                        // Add jitter to avoid thundering herd
                        var jitterMs = _jitterer.Next(0, (int)(baseDelay.TotalMilliseconds * options.JitterFactor));
                        var delay = baseDelay + TimeSpan.FromMilliseconds(jitterMs);
                        
                        // Cap at max delay
                        return delay > options.MaxRetryDelay ? options.MaxRetryDelay : delay;
                    },
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        if (EnableTelemetry)
                        {
                            _metrics.RecordRetry(retryCount);
                            if (exception is RpcException rpcEx)
                            {
                                string errorType = $"{rpcEx.StatusCode}-Retry{retryCount}";
                                _metrics.RecordFailure(errorType);
                            }
                        }
                    });
            
            // Create circuit breaker policy only if not disabled
            if (!options.DisableCircuitBreaker)
            {
                _circuitBreaker = Policy
                    .Handle<RpcException>(IsTransientError)
                    .CircuitBreakerAsync(
                        exceptionsAllowedBeforeBreaking: options.ExceptionsAllowedBeforeBreaking,
                        durationOfBreak: options.CircuitBreakerDuration,
                        onBreak: (exception, duration) => 
                        {
                            if (EnableTelemetry)
                            {
                                _metrics.RecordCircuitBreakerOpen();
                            }
                        },
                        onReset: () => { /* Optional actions when circuit resets */ });
            }
            else
            {
                // Create a dummy circuit breaker that never breaks
                // Use an actual circuit breaker with threshold values so high they'll never be reached
                _circuitBreaker = Policy
                    .Handle<RpcException>(IsTransientError)
                    .CircuitBreakerAsync(
                        exceptionsAllowedBeforeBreaking: int.MaxValue, // Effectively never breaks
                        durationOfBreak: TimeSpan.FromMilliseconds(1)); // Minimal break time if it somehow did break
            }
        }
        
        /// <summary>
        /// Gets the metrics for this resilience policy
        /// </summary>
        public ResilienceMetrics Metrics => _metrics;
        
        /// <summary>
        /// Gets or sets whether telemetry should be collected
        /// </summary>
        public bool EnableTelemetry { get; set; } = true;

        /// <summary>
        /// Executes a gRPC operation with resilience
        /// </summary>
        /// <typeparam name="TResponse">The type of response</typeparam>
        /// <param name="operation">The operation to execute</param>
        /// <param name="context">Context information for the call</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The result of the operation</returns>
        public async Task<TResponse> ExecuteAsync<TResponse>(
            Func<CancellationToken, Task<TResponse>> operation,
            string? context = null,
            CancellationToken cancellationToken = default)
        {
            if (operation == null) throw new ArgumentNullException(nameof(operation));
            
            if (EnableTelemetry)
            {
                _metrics.RecordCallAttempt(context);
            }
            
            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                // Combine the retry policy with the circuit breaker (if enabled)
                AsyncPolicy resilientOperation;
                
                if (_options.DisableCircuitBreaker)
                {
                    // Use only the retry policy without circuit breaking
                    resilientOperation = _retryPolicy;
                }
                else
                {
                    // Use combined policy with circuit breaker
                    resilientOperation = Policy.WrapAsync(_retryPolicy, _circuitBreaker);
                }
                
                // Execute the operation with resilience
                var result = await resilientOperation.ExecuteAsync(
                    async (ctx) => await operation(cancellationToken),
                    new Dictionary<string, object>
                    {
                        ["Context"] = context ?? string.Empty,
                        ["StartTime"] = DateTime.UtcNow
                    });
                
                if (EnableTelemetry)
                {
                    _metrics.RecordSuccess();
                }
                
                return result;
            }
            catch (RpcException ex)
            {
                if (EnableTelemetry)
                {
                    string errorType = $"{ex.StatusCode}";
                    _metrics.RecordFailure(errorType);
                }
                
                // Rethrow the exception after recording metrics
                throw;
            }
            finally
            {
                stopwatch.Stop();
            }
        }
        
        /// <summary>
        /// Executes a gRPC operation with resilience (no result version)
        /// </summary>
        /// <param name="operation">The operation to execute</param>
        /// <param name="context">Context information for the call</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async Task ExecuteAsync(
            Func<CancellationToken, Task> operation,
            string? context = null,
            CancellationToken cancellationToken = default)
        {
            if (operation == null) throw new ArgumentNullException(nameof(operation));
            
            await ExecuteAsync<object>(
                async (ct) =>
                {
                    await operation(ct);
                    return new object(); // Return a non-null object instead of null
                },
                context,
                cancellationToken);
        }
        
        /// <summary>
        /// Determines if an RPC exception is transient and should be retried
        /// </summary>
        /// <param name="ex">The exception to check</param>
        /// <returns>True if the exception is transient</returns>
        private bool IsTransientError(RpcException ex)
        {
            if (ex == null) return false;
            
            // Check if the status code is in the list of retryable status codes
            return _options.RetryableStatusCodes.Contains(ex.StatusCode);
        }
    }
    
    /// <summary>
    /// Options for configuring the resilience policy
    /// </summary>
    public class ResilienceOptions
    {
        /// <summary>
        /// Gets or sets the maximum number of retry attempts
        /// </summary>
        public int MaxRetryAttempts { get; set; } = 3;
        
        /// <summary>
        /// Gets or sets the base delay for retries in milliseconds
        /// </summary>
        public int RetryBackoffBaseMs { get; set; } = 100;
        
        /// <summary>
        /// Gets or sets the backoff factor for exponential backoff
        /// </summary>
        public double RetryBackoffFactor { get; set; } = 2.0;
        
        /// <summary>
        /// Gets or sets the maximum retry delay
        /// </summary>
        public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);
        
        /// <summary>
        /// Gets or sets the jitter factor (0-1) to apply to retry delays
        /// </summary>
        public double JitterFactor { get; set; } = 0.2;
        
        /// <summary>
        /// Gets or sets the number of exceptions allowed before breaking the circuit
        /// </summary>
        public int ExceptionsAllowedBeforeBreaking { get; set; } = 5;
        
        /// <summary>
        /// Gets or sets the duration to keep the circuit broken
        /// </summary>
        public TimeSpan CircuitBreakerDuration { get; set; } = TimeSpan.FromSeconds(30);
        
        /// <summary>
        /// Gets or sets whether to disable the circuit breaker functionality
        /// </summary>
        /// <remarks>
        /// When set to true, the circuit breaker will not be used, preventing channels from
        /// being disabled when they encounter errors. This is useful in environments with known
        /// error rates where you want to keep trying rather than breaking the circuit.
        /// </remarks>
        public bool DisableCircuitBreaker { get; set; } = false;
        
        /// <summary>
        /// Gets the collection of status codes that should be retried
        /// </summary>
        public HashSet<StatusCode> RetryableStatusCodes { get; } = new HashSet<StatusCode>
        {
            StatusCode.DeadlineExceeded,
            StatusCode.ResourceExhausted,
            StatusCode.Unavailable,
            StatusCode.Internal, // For simulated errors and general server errors
            StatusCode.Unknown,  // Unspecified errors
        };
    }
} 