using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Google.Protobuf;
using System.Diagnostics;

namespace RealtimeListener.Production.Storage
{
    /// <summary>
    /// Configuration for Redis batch writer
    /// </summary>
    public class RedisBatchWriterOptions
    {
        /// <summary>
        /// Maximum number of items to batch before flushing
        /// </summary>
        public int MaxBatchSize { get; set; } = 100;
        
        /// <summary>
        /// Maximum time to wait before flushing a partial batch
        /// </summary>
        public TimeSpan MaxBatchWait { get; set; } = TimeSpan.FromMilliseconds(100);
        
        /// <summary>
        /// Redis key for cast add events
        /// </summary>
        public string CastAddQueueKey { get; set; } = "farcaster:casts:add";
        
        /// <summary>
        /// Redis key for cast remove events
        /// </summary>
        public string CastRemoveQueueKey { get; set; } = "farcaster:casts:remove";
        
        /// <summary>
        /// Redis key for storing last processed event ID
        /// </summary>
        public string LastEventIdKey { get; set; } = "farcaster:last_event_id";
        
        /// <summary>
        /// Whether to enable Redis pipelining
        /// </summary>
        public bool EnablePipelining { get; set; } = true;
    }
    
    /// <summary>
    /// High-performance Redis writer with batching and pipelining
    /// </summary>
    public class RedisBatchWriter : IAsyncDisposable
    {
        private readonly IConnectionMultiplexer _redis;
        private readonly RedisBatchWriterOptions _options;
        private readonly ILogger<RedisBatchWriter> _logger;
        private readonly List<(string key, byte[] value, ulong eventId)> _batch;
        private readonly SemaphoreSlim _batchLock;
        private readonly PeriodicTimer _flushTimer;
        private readonly CancellationTokenSource _internalCts;
        private Task? _flushTask;
        private ulong _lastFlushedEventId;
        private long _totalItemsWritten;
        private long _totalBatchesWritten;
        private readonly Stopwatch _timeSinceLastFlush;
        
        public RedisBatchWriter(
            IConnectionMultiplexer redis,
            RedisBatchWriterOptions options,
            ILogger<RedisBatchWriter> logger)
        {
            _redis = redis ?? throw new ArgumentNullException(nameof(redis));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            _batch = new List<(string, byte[], ulong)>(_options.MaxBatchSize);
            _batchLock = new SemaphoreSlim(1, 1);
            _flushTimer = new PeriodicTimer(_options.MaxBatchWait);
            _internalCts = new CancellationTokenSource();
            _timeSinceLastFlush = new Stopwatch();
            _timeSinceLastFlush.Start();
        }
        
        /// <summary>
        /// Starts the background flush task
        /// </summary>
        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (_flushTask != null)
                throw new InvalidOperationException("Writer already started");
                
            _flushTask = Task.Run(() => FlushLoopAsync(cancellationToken), cancellationToken);
            
            _logger.LogInformation("Redis batch writer started");
            return Task.CompletedTask;
        }
        
        /// <summary>
        /// Adds an event to the batch
        /// </summary>
        public async ValueTask AddAsync(FilteredHubEvent hubEvent, CancellationToken cancellationToken = default)
        {
            var key = hubEvent.MessageType == MessageType.CastAdd 
                ? _options.CastAddQueueKey 
                : _options.CastRemoveQueueKey;
                
            await _batchLock.WaitAsync(cancellationToken);
            try
            {
                _batch.Add((key, hubEvent.RawData, hubEvent.EventId));
                
                // Flush immediately if batch is full
                if (_batch.Count >= _options.MaxBatchSize)
                {
                    await FlushBatchInternalAsync();
                }
            }
            finally
            {
                _batchLock.Release();
            }
        }
        
        /// <summary>
        /// Gets the last flushed event ID
        /// </summary>
        public ulong GetLastFlushedEventId() => Interlocked.Read(ref _lastFlushedEventId);
        
        /// <summary>
        /// Gets writer statistics
        /// </summary>
        public (long itemsWritten, long batchesWritten, double avgBatchSize) GetStatistics()
        {
            var items = Interlocked.Read(ref _totalItemsWritten);
            var batches = Interlocked.Read(ref _totalBatchesWritten);
            var avgSize = batches > 0 ? (double)items / batches : 0;
            return (items, batches, avgSize);
        }
        
        /// <summary>
        /// Ensures all queues exist in Redis
        /// </summary>
        public async Task EnsureQueuesExistAsync()
        {
            var db = _redis.GetDatabase();
            
            // Touch the queues to ensure they exist
            var tasks = new[]
            {
                db.ListLengthAsync(_options.CastAddQueueKey),
                db.ListLengthAsync(_options.CastRemoveQueueKey)
            };
            
            await Task.WhenAll(tasks);
            
            _logger.LogInformation("Redis queues initialized: {AddQueue}, {RemoveQueue}", 
                _options.CastAddQueueKey, _options.CastRemoveQueueKey);
        }
        
        /// <summary>
        /// Loads the last processed event ID from Redis
        /// </summary>
        public async Task<ulong> LoadLastEventIdAsync()
        {
            var db = _redis.GetDatabase();
            var value = await db.StringGetAsync(_options.LastEventIdKey);
            
            if (value.HasValue && ulong.TryParse(value, out var eventId))
            {
                _lastFlushedEventId = eventId;
                _logger.LogInformation("Loaded last event ID from Redis: {EventId}", eventId);
                return eventId;
            }
            
            return 0;
        }
        
        private async Task FlushLoopAsync(CancellationToken externalCancellationToken)
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _internalCts.Token, externalCancellationToken);
            var cancellationToken = linkedCts.Token;
            
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await _flushTimer.WaitForNextTickAsync(cancellationToken);
                    
                    await _batchLock.WaitAsync(cancellationToken);
                    try
                    {
                        if (_batch.Count > 0)
                        {
                            await FlushBatchInternalAsync();
                        }
                    }
                    finally
                    {
                        _batchLock.Release();
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in flush loop");
                }
            }
        }
        
        private async Task FlushBatchInternalAsync()
        {
            if (_batch.Count == 0)
                return;
                
            var db = _redis.GetDatabase();
            var batchCopy = _batch.ToList();
            var maxEventId = batchCopy.Max(x => x.eventId);
            
            _batch.Clear();
            _timeSinceLastFlush.Restart();
            
            try
            {
                if (_options.EnablePipelining)
                {
                    // Use pipelining for maximum performance
                    var tasks = new List<Task>(batchCopy.Count + 1);
                    
                    // Group by key for more efficient Redis operations
                    var groups = batchCopy.GroupBy(x => x.key);
                    
                    foreach (var group in groups)
                    {
                        var values = group.Select(x => (RedisValue)x.value).ToArray();
                        tasks.Add(db.ListRightPushAsync(group.Key, values, CommandFlags.FireAndForget));
                    }
                    
                    // Update last event ID
                    tasks.Add(db.StringSetAsync(_options.LastEventIdKey, maxEventId.ToString(), flags: CommandFlags.FireAndForget));
                    
                    await Task.WhenAll(tasks);
                }
                else
                {
                    // Non-pipelined version
                    foreach (var group in batchCopy.GroupBy(x => x.key))
                    {
                        var values = group.Select(x => (RedisValue)x.value).ToArray();
                        await db.ListRightPushAsync(group.Key, values);
                    }
                    
                    await db.StringSetAsync(_options.LastEventIdKey, maxEventId.ToString());
                }
                
                _lastFlushedEventId = maxEventId;
                Interlocked.Add(ref _totalItemsWritten, batchCopy.Count);
                Interlocked.Increment(ref _totalBatchesWritten);
                
                _logger.LogDebug("Flushed batch of {Count} items, last event ID: {EventId}", 
                    batchCopy.Count, maxEventId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to flush batch to Redis");
                throw;
            }
        }
        
        public async ValueTask DisposeAsync()
        {
            _internalCts.Cancel();
            
            // Final flush
            await _batchLock.WaitAsync();
            try
            {
                if (_batch.Count > 0)
                {
                    await FlushBatchInternalAsync();
                }
            }
            finally
            {
                _batchLock.Release();
            }
            
            if (_flushTask != null)
            {
                try
                {
                    await _flushTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }
            
            _batchLock.Dispose();
            _flushTimer.Dispose();
            _internalCts.Dispose();
            
            _logger.LogInformation("Redis batch writer stopped. Total items written: {Items}, Total batches: {Batches}",
                _totalItemsWritten, _totalBatchesWritten);
        }
    }
}