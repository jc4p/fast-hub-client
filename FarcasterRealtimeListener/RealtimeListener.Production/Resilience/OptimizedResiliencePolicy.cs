using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using HubClient.Core.Resilience;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Wrap;

namespace RealtimeListener.Production.Resilience
{
    /// <summary>
    /// Optimized implementation of the gRPC resilience policy using Polly,
    /// based on benchmark results for optimal settings and patterns
    /// </summary>
    public class OptimizedResiliencePolicy : IGrpcResiliencePolicy
    {
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly AsyncCircuitBreakerPolicy _circuitBreakerPolicy;
        private readonly AsyncPolicyWrap _combinedPolicy;
        private readonly ResilienceMetrics _metrics;
        private bool _enableTelemetry = true;
        
        private static readonly HashSet<StatusCode> _retryableStatusCodes = new()
        {
            StatusCode.Unavailable,
            StatusCode.Internal,
            StatusCode.DeadlineExceeded,
            StatusCode.ResourceExhausted
        };

        /// <summary>
        /// Enable or disable telemetry collection
        /// </summary>
        public bool EnableTelemetry 
        { 
            get => _enableTelemetry; 
            set => _enableTelemetry = value; 
        }

        /// <summary>
        /// Gets resilience metrics collected during execution
        /// </summary>
        public HubClient.Core.Resilience.ResilienceMetrics Metrics
        {
            get
            {
                var metrics = new HubClient.Core.Resilience.ResilienceMetrics();
                
                // Record call metrics
                for (int i = 0; i < _metrics.CallCount; i++)
                {
                    metrics.RecordCallAttempt(null);
                }
                
                // Record failures
                for (int i = 0; i < _metrics.ErrorCount; i++)
                {
                    metrics.RecordFailure("Unknown");
                }
                
                // Record retries
                for (int i = 0; i < _metrics.RetryCount; i++)
                {
                    metrics.RecordRetry(1);
                }
                
                // Record circuit breaker events
                for (int i = 0; i < _metrics.CircuitBreaks; i++)
                {
                    metrics.RecordCircuitBreakerOpen();
                }
                
                return metrics;
            }
        }

        /// <summary>
        /// Creates a new instance of the optimized resilience policy
        /// </summary>
        /// <param name="maxRetries">Maximum number of retries. Default is 3.</param>
        /// <param name="initialBackoffMs">Initial backoff in milliseconds. Default is 100ms.</param>
        /// <param name="maxBackoffMs">Maximum backoff in milliseconds. Default is 3000ms.</param>
        /// <param name="circuitBreakerThreshold">Number of failures before circuit trips. Default is 10.</param>
        /// <param name="circuitBreakerDelay">Time circuit stays open before reset attempt. Default is 30s.</param>
        public OptimizedResiliencePolicy(
            int maxRetries = 3,
            int initialBackoffMs = 100,
            int maxBackoffMs = 3000,
            int circuitBreakerThreshold = 10,
            TimeSpan? circuitBreakerDelay = null)
        {
            _metrics = new ResilienceMetrics();
            var cbDelay = circuitBreakerDelay ?? TimeSpan.FromSeconds(30);
            
            // Create retry policy with exponential backoff
            _retryPolicy = Policy
                .Handle<RpcException>(IsTransientException)
                .Or<HttpRequestException>()
                .WaitAndRetryAsync(
                    maxRetries,
                    retryAttempt => TimeSpan.FromMilliseconds(
                        Math.Min(maxBackoffMs, initialBackoffMs * Math.Pow(2, retryAttempt - 1))),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        if (!_enableTelemetry) return;
                        
                        _metrics.RecordRetry();
                        if (exception is RpcException rpcEx)
                        {
                            _metrics.RecordStatusCode(rpcEx.StatusCode);
                        }
                    });
            
            // Create circuit breaker policy
            _circuitBreakerPolicy = Policy
                .Handle<RpcException>(IsNonTransientException)
                .CircuitBreakerAsync(
                    circuitBreakerThreshold,
                    cbDelay,
                    onBreak: (ex, breakDelay) =>
                    {
                        if (!_enableTelemetry) return;
                        
                        _metrics.RecordCircuitBroken();
                        if (ex is RpcException rpcEx)
                        {
                            _metrics.RecordStatusCode(rpcEx.StatusCode);
                        }
                    },
                    onReset: () => 
                    {
                        if (_enableTelemetry) _metrics.RecordCircuitReset();
                    },
                    onHalfOpen: () => 
                    {
                        if (_enableTelemetry) _metrics.RecordCircuitHalfOpen();
                    });
            
            // Combine policies: retry, then circuit breaker
            _combinedPolicy = _retryPolicy.WrapAsync(_circuitBreakerPolicy);
        }

        /// <summary>
        /// Execute an asynchronous action with resilience
        /// </summary>
        public async Task<TResult> ExecuteAsync<TResult>(
            Func<CancellationToken, Task<TResult>> operation,
            string? context = null,
            CancellationToken cancellationToken = default)
        {
            try
            {
                if (_enableTelemetry) _metrics.RecordCall();
                
                var result = await _combinedPolicy.ExecuteAsync(
                    async ct => await operation(ct), 
                    cancellationToken);
                
                if (_enableTelemetry) _metrics.RecordSuccess();
                return result;
            }
            catch (Exception ex)
            {
                if (_enableTelemetry)
                {
                    _metrics.RecordFailure();
                    
                    if (ex is RpcException rpcEx)
                    {
                        _metrics.RecordStatusCode(rpcEx.StatusCode);
                    }
                }
                
                throw;
            }
        }

        /// <summary>
        /// Execute an asynchronous action with resilience (no result version)
        /// </summary>
        public async Task ExecuteAsync(
            Func<CancellationToken, Task> operation,
            string? context = null,
            CancellationToken cancellationToken = default)
        {
            try
            {
                if (_enableTelemetry) _metrics.RecordCall();
                
                await _combinedPolicy.ExecuteAsync(
                    async ct => await operation(ct), 
                    cancellationToken);
                
                if (_enableTelemetry) _metrics.RecordSuccess();
            }
            catch (Exception ex)
            {
                if (_enableTelemetry)
                {
                    _metrics.RecordFailure();
                    
                    if (ex is RpcException rpcEx)
                    {
                        _metrics.RecordStatusCode(rpcEx.StatusCode);
                    }
                }
                
                throw;
            }
        }

        /// <summary>
        /// Get the metrics for this policy
        /// </summary>
        public ResilienceMetrics GetMetrics() => _metrics;

        /// <summary>
        /// Determine if an exception is transient and should be retried
        /// </summary>
        private static bool IsTransientException(RpcException exception)
        {
            return _retryableStatusCodes.Contains(exception.StatusCode);
        }

        /// <summary>
        /// Determine if an exception is non-transient and should trip the circuit breaker
        /// </summary>
        private static bool IsNonTransientException(RpcException exception)
        {
            // For circuit breaking, we look at failures that are likely server-side issues
            return exception.StatusCode == StatusCode.Internal ||
                   exception.StatusCode == StatusCode.Unimplemented ||
                   exception.StatusCode == StatusCode.Unknown;
        }
    }

    /// <summary>
    /// Metrics for resilience operations
    /// </summary>
    public class ResilienceMetrics
    {
        private long _callCount;
        private long _successCount;
        private long _errorCount;
        private long _retryCount;
        private long _circuitBreaks;
        private long _circuitResets;
        private long _circuitHalfOpens;
        private readonly Dictionary<StatusCode, long> _statusCodes = new();
        
        /// <summary>
        /// Gets the total number of calls
        /// </summary>
        public long CallCount => Interlocked.Read(ref _callCount);
        
        /// <summary>
        /// Gets the number of successful calls
        /// </summary>
        public long SuccessCount => Interlocked.Read(ref _successCount);
        
        /// <summary>
        /// Gets the number of failed calls
        /// </summary>
        public long ErrorCount => Interlocked.Read(ref _errorCount);
        
        /// <summary>
        /// Gets the number of retry attempts
        /// </summary>
        public long RetryCount => Interlocked.Read(ref _retryCount);
        
        /// <summary>
        /// Gets the number of circuit breaker trips
        /// </summary>
        public long CircuitBreaks => Interlocked.Read(ref _circuitBreaks);
        
        /// <summary>
        /// Records a call
        /// </summary>
        public void RecordCall() => Interlocked.Increment(ref _callCount);
        
        /// <summary>
        /// Records a successful call
        /// </summary>
        public void RecordSuccess() => Interlocked.Increment(ref _successCount);
        
        /// <summary>
        /// Records a failed call
        /// </summary>
        public void RecordFailure() => Interlocked.Increment(ref _errorCount);
        
        /// <summary>
        /// Records a retry attempt
        /// </summary>
        public void RecordRetry() => Interlocked.Increment(ref _retryCount);
        
        /// <summary>
        /// Records a circuit breaker trip
        /// </summary>
        public void RecordCircuitBroken() => Interlocked.Increment(ref _circuitBreaks);
        
        /// <summary>
        /// Records a circuit breaker reset
        /// </summary>
        public void RecordCircuitReset() => Interlocked.Increment(ref _circuitResets);
        
        /// <summary>
        /// Records a circuit breaker half-open state
        /// </summary>
        public void RecordCircuitHalfOpen() => Interlocked.Increment(ref _circuitHalfOpens);
        
        /// <summary>
        /// Records a status code
        /// </summary>
        public void RecordStatusCode(StatusCode statusCode)
        {
            lock (_statusCodes)
            {
                if (!_statusCodes.TryGetValue(statusCode, out var count))
                {
                    count = 0;
                }
                
                _statusCodes[statusCode] = count + 1;
            }
        }
        
        /// <summary>
        /// Gets the count of a specific status code
        /// </summary>
        public long GetStatusCodeCount(StatusCode statusCode)
        {
            lock (_statusCodes)
            {
                if (_statusCodes.TryGetValue(statusCode, out var count))
                {
                    return count;
                }
                
                return 0;
            }
        }
        
        /// <summary>
        /// Resets all metrics
        /// </summary>
        public void Reset()
        {
            Interlocked.Exchange(ref _callCount, 0);
            Interlocked.Exchange(ref _successCount, 0);
            Interlocked.Exchange(ref _errorCount, 0);
            Interlocked.Exchange(ref _retryCount, 0);
            Interlocked.Exchange(ref _circuitBreaks, 0);
            Interlocked.Exchange(ref _circuitResets, 0);
            Interlocked.Exchange(ref _circuitHalfOpens, 0);
            
            lock (_statusCodes)
            {
                _statusCodes.Clear();
            }
        }
    }
} 