using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace HubClient.Core.Concurrency
{
    /// <summary>
    /// Contains performance metrics for a concurrency pipeline
    /// </summary>
    public class PipelineMetrics
    {
        private readonly Stopwatch _uptime = Stopwatch.StartNew();
        private readonly object _syncLock = new();
        private long _totalItemsEnqueued;
        private long _totalItemsProcessed;
        private long _totalItemsDequeued;
        private long _maxQueuedItems;
        private long _processingLatencyTicks;
        private long _queuedLatencyTicks;
        private long _processingLatencyCount;
        private long _queuedLatencyCount;
        private long _processingErrorCount;
        private long _backpressureEventCount;
        private long _totalQueueWaitTimeTicks;
        private long _queueWaitTimeCount;
        private long _totalProcessingTimeTicks;
        private long _processingTimeCount;
        
        // Latency tracking
        private long _minLatencyTicks = long.MaxValue;
        private long _maxLatencyTicks;
        private long _totalLatencyTicks;
        private long _latencySampleCount;
        
        // P99 tracking - we'll keep track of the last 1000 latencies
        private readonly Queue<long> _recentLatencies = new(1000);
        
        /// <summary>
        /// Time since the pipeline was created
        /// </summary>
        public TimeSpan Uptime => _uptime.Elapsed;
        
        /// <summary>
        /// Total number of items that have been enqueued to the pipeline
        /// </summary>
        public long TotalItemsEnqueued => Interlocked.Read(ref _totalItemsEnqueued);
        
        /// <summary>
        /// Total number of items that have been processed by the pipeline
        /// </summary>
        public long TotalItemsProcessed => Interlocked.Read(ref _totalItemsProcessed);
        
        /// <summary>
        /// Alias for TotalItemsProcessed for backward compatibility
        /// </summary>
        public long ProcessedCount => TotalItemsProcessed;
        
        /// <summary>
        /// Total number of items that have been dequeued from the output of the pipeline
        /// </summary>
        public long TotalItemsDequeued => Interlocked.Read(ref _totalItemsDequeued);
        
        /// <summary>
        /// Maximum number of items that have been queued simultaneously
        /// </summary>
        public long MaxQueuedItems => Interlocked.Read(ref _maxQueuedItems);
        
        /// <summary>
        /// Average time (in milliseconds) an item spends being processed by the pipeline
        /// </summary>
        public double AverageProcessingLatencyMs => 
            _processingLatencyCount == 0 
                ? 0 
                : TimeSpan.FromTicks(Interlocked.Read(ref _processingLatencyTicks) / Interlocked.Read(ref _processingLatencyCount)).TotalMilliseconds;
        
        /// <summary>
        /// Average time (in milliseconds) an item spends in the queue before processing begins
        /// </summary>
        public double AverageQueuedLatencyMs => 
            _queuedLatencyCount == 0 
                ? 0 
                : TimeSpan.FromTicks(Interlocked.Read(ref _queuedLatencyTicks) / Interlocked.Read(ref _queuedLatencyCount)).TotalMilliseconds;
        
        /// <summary>
        /// Number of errors that have occurred during processing
        /// </summary>
        public long ProcessingErrorCount => Interlocked.Read(ref _processingErrorCount);
        
        /// <summary>
        /// Number of backpressure events that have occurred
        /// </summary>
        public long BackpressureEventCount => Interlocked.Read(ref _backpressureEventCount);
        
        /// <summary>
        /// Alias for BackpressureEventCount for backward compatibility
        /// </summary>
        public long TotalBackpressureEvents => BackpressureEventCount;
        
        /// <summary>
        /// Average time (in milliseconds) an item spends waiting in the queue
        /// </summary>
        public double AverageQueueWaitTimeMs => 
            _queueWaitTimeCount == 0 
                ? 0 
                : TimeSpan.FromTicks(Interlocked.Read(ref _totalQueueWaitTimeTicks) / Interlocked.Read(ref _queueWaitTimeCount)).TotalMilliseconds;
        
        /// <summary>
        /// Average time (in milliseconds) spent processing an item
        /// </summary>
        public double AverageProcessingTimeMs => 
            _processingTimeCount == 0 
                ? 0 
                : TimeSpan.FromTicks(Interlocked.Read(ref _totalProcessingTimeTicks) / Interlocked.Read(ref _processingTimeCount)).TotalMilliseconds;
        
        /// <summary>
        /// Minimum latency observed
        /// </summary>
        public TimeSpan MinLatency => TimeSpan.FromTicks(Interlocked.Read(ref _minLatencyTicks));
        
        /// <summary>
        /// Maximum latency observed
        /// </summary>
        public TimeSpan MaxLatency => TimeSpan.FromTicks(Interlocked.Read(ref _maxLatencyTicks));
        
        /// <summary>
        /// Average latency observed
        /// </summary>
        public TimeSpan AverageLatency
        {
            get
            {
                long samples = Interlocked.Read(ref _latencySampleCount);
                if (samples == 0) return TimeSpan.Zero;
                return TimeSpan.FromTicks(Interlocked.Read(ref _totalLatencyTicks) / samples);
            }
        }
        
        /// <summary>
        /// Gets the average throughput in items per second
        /// </summary>
        public double AverageThroughput
        {
            get
            {
                long total = TotalItemsProcessed;
                TimeSpan uptime = Uptime;
                return uptime.TotalSeconds > 0 ? total / uptime.TotalSeconds : 0;
            }
        }
        
        /// <summary>
        /// 99th percentile latency (approximate, based on last 1000 samples)
        /// </summary>
        public TimeSpan P99Latency
        {
            get
            {
                lock (_syncLock)
                {
                    if (_recentLatencies.Count == 0) return TimeSpan.Zero;
                    
                    // Sort the latencies
                    var sortedLatencies = new List<long>(_recentLatencies);
                    sortedLatencies.Sort();
                    
                    // Get the 99th percentile
                    int p99Index = (int)Math.Ceiling(sortedLatencies.Count * 0.99) - 1;
                    if (p99Index < 0) p99Index = 0;
                    return TimeSpan.FromTicks(sortedLatencies[p99Index]);
                }
            }
        }
        
        /// <summary>
        /// Records that an item has been enqueued
        /// </summary>
        public void RecordItemEnqueued()
        {
            Interlocked.Increment(ref _totalItemsEnqueued);
        }
        
        /// <summary>
        /// Records that a batch of items has been enqueued
        /// </summary>
        /// <param name="count">Number of items enqueued</param>
        public void RecordItemsEnqueued(int count)
        {
            Interlocked.Add(ref _totalItemsEnqueued, count);
        }
        
        /// <summary>
        /// Records that an item has been processed
        /// </summary>
        public void RecordItemProcessed()
        {
            Interlocked.Increment(ref _totalItemsProcessed);
        }
        
        /// <summary>
        /// Records that a batch of items has been processed
        /// </summary>
        /// <param name="count">Number of items processed</param>
        public void RecordItemsProcessed(int count)
        {
            Interlocked.Add(ref _totalItemsProcessed, count);
        }
        
        /// <summary>
        /// Records that an item has been dequeued from the output
        /// </summary>
        public void RecordItemDequeued()
        {
            Interlocked.Increment(ref _totalItemsDequeued);
        }
        
        /// <summary>
        /// Updates the maximum number of queued items if the current count is higher
        /// </summary>
        /// <param name="currentQueueCount">Current count of queued items</param>
        public void UpdateMaxQueuedItems(int currentQueueCount)
        {
            long currentMax = _maxQueuedItems;
            while (currentQueueCount > currentMax)
            {
                long newMax = Interlocked.CompareExchange(ref _maxQueuedItems, currentQueueCount, currentMax);
                if (newMax == currentMax) break;
                currentMax = newMax;
            }
        }
        
        /// <summary>
        /// Records a processing latency measurement
        /// </summary>
        /// <param name="latency">Time taken to process an item</param>
        public void RecordProcessingLatency(TimeSpan latency)
        {
            Interlocked.Add(ref _processingLatencyTicks, latency.Ticks);
            Interlocked.Increment(ref _processingLatencyCount);
        }
        
        /// <summary>
        /// Records a queuing latency measurement
        /// </summary>
        /// <param name="latency">Time an item spent in the queue</param>
        public void RecordQueuedLatency(TimeSpan latency)
        {
            Interlocked.Add(ref _queuedLatencyTicks, latency.Ticks);
            Interlocked.Increment(ref _queuedLatencyCount);
        }
        
        /// <summary>
        /// Records that an error occurred during processing
        /// </summary>
        public void RecordProcessingError()
        {
            Interlocked.Increment(ref _processingErrorCount);
        }
        
        /// <summary>
        /// Records that a backpressure event occurred (e.g. queue full, semaphore wait)
        /// </summary>
        public void RecordBackpressureEvent()
        {
            Interlocked.Increment(ref _backpressureEventCount);
        }
        
        /// <summary>
        /// Records the time an item spent waiting in the queue
        /// </summary>
        /// <param name="waitTime">Time the item spent in the queue</param>
        public void RecordQueueWaitTime(TimeSpan waitTime)
        {
            Interlocked.Add(ref _totalQueueWaitTimeTicks, waitTime.Ticks);
            Interlocked.Increment(ref _queueWaitTimeCount);
        }
        
        /// <summary>
        /// Records the time spent processing an item
        /// </summary>
        /// <param name="processingTime">Time spent processing the item</param>
        public void RecordProcessingTime(TimeSpan processingTime)
        {
            Interlocked.Add(ref _totalProcessingTimeTicks, processingTime.Ticks);
            Interlocked.Increment(ref _processingTimeCount);
        }
        
        /// <summary>
        /// Resets all metrics to zero
        /// </summary>
        public void Reset()
        {
            lock (_syncLock)
            {
                _uptime.Restart();
                Interlocked.Exchange(ref _totalItemsEnqueued, 0);
                Interlocked.Exchange(ref _totalItemsProcessed, 0);
                Interlocked.Exchange(ref _totalItemsDequeued, 0);
                Interlocked.Exchange(ref _maxQueuedItems, 0);
                Interlocked.Exchange(ref _processingLatencyTicks, 0);
                Interlocked.Exchange(ref _queuedLatencyTicks, 0);
                Interlocked.Exchange(ref _processingLatencyCount, 0);
                Interlocked.Exchange(ref _queuedLatencyCount, 0);
                Interlocked.Exchange(ref _processingErrorCount, 0);
                Interlocked.Exchange(ref _backpressureEventCount, 0);
                Interlocked.Exchange(ref _totalQueueWaitTimeTicks, 0);
                Interlocked.Exchange(ref _queueWaitTimeCount, 0);
                Interlocked.Exchange(ref _totalProcessingTimeTicks, 0);
                Interlocked.Exchange(ref _processingTimeCount, 0);
                Interlocked.Exchange(ref _minLatencyTicks, long.MaxValue);
                Interlocked.Exchange(ref _maxLatencyTicks, 0);
                Interlocked.Exchange(ref _totalLatencyTicks, 0);
                Interlocked.Exchange(ref _latencySampleCount, 0);
                _recentLatencies.Clear();
            }
        }
        
        /// <summary>
        /// Records end-to-end latency for an item
        /// </summary>
        /// <param name="latency">Latency</param>
        public void RecordLatency(TimeSpan latency)
        {
            long latencyTicks = latency.Ticks;
            
            // Update min/max/total/count
            InterlockedMin(ref _minLatencyTicks, latencyTicks);
            InterlockedMax(ref _maxLatencyTicks, latencyTicks);
            Interlocked.Add(ref _totalLatencyTicks, latencyTicks);
            Interlocked.Increment(ref _latencySampleCount);
            
            // Update P99 tracking
            lock (_syncLock)
            {
                _recentLatencies.Enqueue(latencyTicks);
                if (_recentLatencies.Count > 1000)
                {
                    _recentLatencies.Dequeue();
                }
            }
        }
        
        /// <summary>
        /// Performs an atomic min operation
        /// </summary>
        private static void InterlockedMin(ref long target, long value)
        {
            long currentMin = Interlocked.Read(ref target);
            while (value < currentMin)
            {
                long newMin = Interlocked.CompareExchange(ref target, value, currentMin);
                if (newMin == currentMin) break;
                currentMin = newMin;
            }
        }
        
        /// <summary>
        /// Performs an atomic max operation
        /// </summary>
        private static void InterlockedMax(ref long target, long value)
        {
            long currentMax = Interlocked.Read(ref target);
            while (value > currentMax)
            {
                long newMax = Interlocked.CompareExchange(ref target, value, currentMax);
                if (newMax == currentMax) break;
                currentMax = newMax;
            }
        }
    }
} 