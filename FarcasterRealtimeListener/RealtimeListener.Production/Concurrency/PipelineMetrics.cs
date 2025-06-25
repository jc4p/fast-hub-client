using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace RealtimeListener.Production.Concurrency
{
    /// <summary>
    /// Collects metrics about pipeline performance
    /// </summary>
    public class PipelineMetrics
    {
        private readonly ConcurrentQueue<TimeSpan> _latencies = new(new TimeSpan[1000]);
        private readonly ConcurrentQueue<TimeSpan> _throughputTimestamps = new();
        private readonly ConcurrentQueue<Exception> _exceptions = new();
        private long _successCount;
        private long _failureCount;
        private long _backpressureEventCount;
        private TimeSpan _totalLatency;
        private readonly Stopwatch _uptimeStopwatch = Stopwatch.StartNew();
        private readonly object _latencyLock = new();
        
        /// <summary>
        /// Gets the total number of successfully processed items
        /// </summary>
        public long SuccessCount => Interlocked.Read(ref _successCount);
        
        /// <summary>
        /// Gets the total number of failures
        /// </summary>
        public long FailureCount => Interlocked.Read(ref _failureCount);
        
        /// <summary>
        /// Alias for SuccessCount for compatibility with benchmarks
        /// </summary>
        public long ProcessedCount => SuccessCount;

        /// <summary>
        /// Gets the number of backpressure events that have occurred
        /// </summary>
        public long BackpressureEventCount => Interlocked.Read(ref _backpressureEventCount);
        
        /// <summary>
        /// Gets the total number of backpressure events for compatibility with benchmarks
        /// </summary>
        public long TotalBackpressureEvents => BackpressureEventCount;
        
        /// <summary>
        /// Gets the average latency of processing
        /// </summary>
        public TimeSpan AverageLatency
        {
            get
            {
                long successCount = Interlocked.Read(ref _successCount);
                return successCount > 0 
                    ? TimeSpan.FromTicks(_totalLatency.Ticks / Math.Max(1, successCount)) 
                    : TimeSpan.Zero;
            }
        }
        
        /// <summary>
        /// Gets the 99th percentile latency
        /// </summary>
        public TimeSpan P99Latency
        {
            get
            {
                var latencies = _latencies.ToArray();
                if (latencies.Length == 0) return TimeSpan.Zero;
                
                Array.Sort(latencies);
                int p99Index = Math.Max(0, (int)(latencies.Length * 0.99) - 1);
                return latencies[p99Index];
            }
        }
        
        /// <summary>
        /// Gets the average throughput in items per second
        /// </summary>
        public double AverageThroughput
        {
            get
            {
                long totalCount = SuccessCount + FailureCount;
                TimeSpan uptime = _uptimeStopwatch.Elapsed;
                return uptime.TotalSeconds > 0 ? totalCount / uptime.TotalSeconds : 0;
            }
        }
        
        /// <summary>
        /// Gets the recent throughput in items per second (last minute)
        /// </summary>
        public double RecentThroughput
        {
            get
            {
                // Calculate throughput based on items processed in the last minute
                TimeSpan cutoff = TimeSpan.FromMinutes(1);
                TimeSpan now = _uptimeStopwatch.Elapsed;
                TimeSpan oldest = now.Subtract(cutoff);
                
                // Clean up old timestamps
                while (_throughputTimestamps.TryPeek(out var timestamp) && timestamp < oldest)
                {
                    _throughputTimestamps.TryDequeue(out _);
                }
                
                // Count items in the window
                int recentCount = _throughputTimestamps.Count;
                return recentCount / Math.Min(cutoff.TotalSeconds, now.TotalSeconds);
            }
        }
        
        /// <summary>
        /// Gets the most recent exceptions (up to 100)
        /// </summary>
        public IEnumerable<Exception> RecentExceptions => _exceptions.ToArray();
        
        /// <summary>
        /// Records the latency of a processing operation
        /// </summary>
        public void RecordLatency(TimeSpan latency)
        {
            lock (_latencyLock)
            {
                // Update total latency
                _totalLatency = _totalLatency.Add(latency);
                
                // Add to rolling latency queue (limit to 1000 samples)
                _latencies.Enqueue(latency);
                while (_latencies.Count > 1000 && _latencies.TryDequeue(out _)) { }
            }
        }
        
        /// <summary>
        /// Increments the success counter
        /// </summary>
        public void IncrementSuccessCount()
        {
            Interlocked.Increment(ref _successCount);
            _throughputTimestamps.Enqueue(_uptimeStopwatch.Elapsed);
        }
        
        /// <summary>
        /// Increments the failure counter
        /// </summary>
        public void IncrementFailureCount()
        {
            Interlocked.Increment(ref _failureCount);
            _throughputTimestamps.Enqueue(_uptimeStopwatch.Elapsed);
        }
        
        /// <summary>
        /// Records an exception that occurred during processing
        /// </summary>
        public void RecordException(Exception exception)
        {
            if (exception == null) return;
            
            // Add to rolling exception queue (limit to 100 exceptions)
            _exceptions.Enqueue(exception);
            while (_exceptions.Count > 100 && _exceptions.TryDequeue(out _)) { }
        }
        
        /// <summary>
        /// Records that a backpressure event occurred
        /// </summary>
        public void RecordBackpressureEvent()
        {
            Interlocked.Increment(ref _backpressureEventCount);
        }
        
        /// <summary>
        /// Resets all metrics
        /// </summary>
        public void Reset()
        {
            while (_latencies.TryDequeue(out _)) { }
            while (_throughputTimestamps.TryDequeue(out _)) { }
            while (_exceptions.TryDequeue(out _)) { }
            
            Interlocked.Exchange(ref _successCount, 0);
            Interlocked.Exchange(ref _failureCount, 0);
            Interlocked.Exchange(ref _backpressureEventCount, 0);
            _totalLatency = TimeSpan.Zero;
            _uptimeStopwatch.Restart();
        }
    }
} 