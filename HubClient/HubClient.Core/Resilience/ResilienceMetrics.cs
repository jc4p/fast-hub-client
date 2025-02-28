using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace HubClient.Core.Resilience
{
    /// <summary>
    /// Collects metrics about resilience operations
    /// </summary>
    public class ResilienceMetrics
    {
        private long _totalCalls;
        private long _successfulCalls;
        private long _failedCalls;
        private long _retriedCalls;
        private long _circuitBreakerOpenCount;
        private long _totalRetries;
        private long _maxRetries;
        private readonly ConcurrentDictionary<string, long> _errorsByType = new();
        private readonly ConcurrentDictionary<string, long> _callsByContext = new();
        private readonly Stopwatch _uptime = Stopwatch.StartNew();

        /// <summary>
        /// Records that a call was attempted
        /// </summary>
        /// <param name="context">The call context</param>
        public void RecordCallAttempt(string? context)
        {
            Interlocked.Increment(ref _totalCalls);
            
            if (!string.IsNullOrEmpty(context))
            {
                _callsByContext.AddOrUpdate(context, 1, (_, count) => count + 1);
            }
        }
        
        /// <summary>
        /// Records that a call succeeded
        /// </summary>
        public void RecordSuccess()
        {
            Interlocked.Increment(ref _successfulCalls);
        }
        
        /// <summary>
        /// Records that a call failed
        /// </summary>
        /// <param name="errorType">The type of error</param>
        public void RecordFailure(string errorType)
        {
            Interlocked.Increment(ref _failedCalls);
            
            if (!string.IsNullOrEmpty(errorType))
            {
                _errorsByType.AddOrUpdate(errorType, 1, (_, count) => count + 1);
            }
        }
        
        /// <summary>
        /// Records that a call was retried
        /// </summary>
        /// <param name="retryCount">The current retry count for the call</param>
        public void RecordRetry(int retryCount)
        {
            Interlocked.Increment(ref _retriedCalls);
            Interlocked.Increment(ref _totalRetries);
            
            // Update max retries if this is higher
            while (true)
            {
                long currentMax = _maxRetries;
                if (retryCount <= currentMax)
                    break;
                
                if (Interlocked.CompareExchange(ref _maxRetries, retryCount, currentMax) == currentMax)
                    break;
            }
        }
        
        /// <summary>
        /// Records that the circuit breaker opened
        /// </summary>
        public void RecordCircuitBreakerOpen()
        {
            Interlocked.Increment(ref _circuitBreakerOpenCount);
        }
        
        /// <summary>
        /// Gets the total number of calls attempted
        /// </summary>
        public long TotalCalls => _totalCalls;
        
        /// <summary>
        /// Gets the total number of successful calls
        /// </summary>
        public long SuccessfulCalls => _successfulCalls;
        
        /// <summary>
        /// Gets the total number of failed calls
        /// </summary>
        public long FailedCalls => _failedCalls;
        
        /// <summary>
        /// Gets the total number of calls that were retried
        /// </summary>
        public long RetriedCalls => _retriedCalls;
        
        /// <summary>
        /// Gets the total number of retries performed
        /// </summary>
        public long TotalRetries => _totalRetries;
        
        /// <summary>
        /// Gets the maximum number of retries performed for any single call
        /// </summary>
        public long MaxRetries => _maxRetries;
        
        /// <summary>
        /// Gets the number of times the circuit breaker opened
        /// </summary>
        public long CircuitBreakerOpenCount => _circuitBreakerOpenCount;
        
        /// <summary>
        /// Gets the success rate as a percentage
        /// </summary>
        public double SuccessRate => _totalCalls > 0 ? (double)_successfulCalls / _totalCalls * 100 : 0;
        
        /// <summary>
        /// Gets the error counts by error type
        /// </summary>
        public IDictionary<string, long> ErrorsByType => new Dictionary<string, long>(_errorsByType);
        
        /// <summary>
        /// Gets the call counts by context
        /// </summary>
        public IDictionary<string, long> CallsByContext => new Dictionary<string, long>(_callsByContext);
        
        /// <summary>
        /// Gets the uptime of the resilience metrics
        /// </summary>
        public TimeSpan Uptime => _uptime.Elapsed;
        
        /// <summary>
        /// Resets all metrics
        /// </summary>
        public void Reset()
        {
            Interlocked.Exchange(ref _totalCalls, 0);
            Interlocked.Exchange(ref _successfulCalls, 0);
            Interlocked.Exchange(ref _failedCalls, 0);
            Interlocked.Exchange(ref _retriedCalls, 0);
            Interlocked.Exchange(ref _circuitBreakerOpenCount, 0);
            Interlocked.Exchange(ref _totalRetries, 0);
            Interlocked.Exchange(ref _maxRetries, 0);
            
            _errorsByType.Clear();
            _callsByContext.Clear();
            _uptime.Restart();
        }
    }
} 