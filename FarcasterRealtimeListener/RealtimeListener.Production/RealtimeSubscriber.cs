using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using HubClient.Core;
using HubClient.Core.Resilience;
using Microsoft.Extensions.Logging;

namespace RealtimeListener.Production
{
    /// <summary>
    /// Configuration for the realtime subscriber
    /// </summary>
    public class RealtimeSubscriberOptions
    {
        /// <summary>
        /// List of FIDs to filter events for. Empty means all FIDs.
        /// </summary>
        public HashSet<ulong> FilteredFids { get; set; } = new();
        
        /// <summary>
        /// Types of messages to process
        /// </summary>
        public HashSet<MessageType> MessageTypes { get; set; } = new() 
        { 
            MessageType.CastAdd,
            MessageType.CastRemove 
        };
        
        /// <summary>
        /// Event ID to start subscribing from
        /// </summary>
        public ulong? FromEventId { get; set; }
        
        /// <summary>
        /// Maximum number of events to buffer before applying backpressure
        /// </summary>
        public int BufferCapacity { get; set; } = 10000;
    }
    
    /// <summary>
    /// Filtered event data for processing
    /// </summary>
    public class FilteredHubEvent
    {
        public ulong EventId { get; set; }
        public ulong Fid { get; set; }
        public MessageType MessageType { get; set; }
        public Message Message { get; set; } = null!;
        public ulong Timestamp { get; set; }
        public byte[] RawData { get; set; } = Array.Empty<byte>();
    }
    
    /// <summary>
    /// Subscribes to Farcaster hub events and filters them based on configuration
    /// </summary>
    public class RealtimeSubscriber : IAsyncDisposable
    {
        private readonly IGrpcConnectionManager _connectionManager;
        private readonly RealtimeSubscriberOptions _options;
        private readonly ILogger<RealtimeSubscriber> _logger;
        private readonly Channel<FilteredHubEvent> _outputChannel;
        private readonly CancellationTokenSource _internalCts;
        private Task? _subscriptionTask;
        private ulong _lastProcessedEventId;
        private long _totalEventsReceived;
        private long _filteredEventsCount;
        
        public RealtimeSubscriber(
            IGrpcConnectionManager connectionManager,
            RealtimeSubscriberOptions options,
            ILogger<RealtimeSubscriber> logger)
        {
            _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            _outputChannel = Channel.CreateBounded<FilteredHubEvent>(new BoundedChannelOptions(_options.BufferCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = true
            });
            
            _internalCts = new CancellationTokenSource();
            _lastProcessedEventId = options.FromEventId ?? 0;
        }
        
        /// <summary>
        /// Starts the subscription
        /// </summary>
        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (_subscriptionTask != null)
                throw new InvalidOperationException("Subscription already started");
                
            _subscriptionTask = Task.Run(() => SubscribeLoopAsync(cancellationToken), cancellationToken);
            return Task.CompletedTask;
        }
        
        /// <summary>
        /// Gets a reader for processed events
        /// </summary>
        public ChannelReader<FilteredHubEvent> GetReader() => _outputChannel.Reader;
        
        /// <summary>
        /// Gets the last processed event ID
        /// </summary>
        public ulong LastProcessedEventId => Interlocked.Read(ref _lastProcessedEventId);
        
        /// <summary>
        /// Gets statistics about event processing
        /// </summary>
        public (long totalReceived, long filtered, double filterRate) GetStatistics()
        {
            var total = Interlocked.Read(ref _totalEventsReceived);
            var filtered = Interlocked.Read(ref _filteredEventsCount);
            var rate = total > 0 ? (double)filtered / total : 0;
            return (total, filtered, rate);
        }
        
        private async Task SubscribeLoopAsync(CancellationToken externalCancellationToken)
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _internalCts.Token, externalCancellationToken);
            var cancellationToken = linkedCts.Token;
            
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await SubscribeWithRetryAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Subscription cancelled");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Fatal error in subscription loop");
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
            }
            
            _outputChannel.Writer.TryComplete();
        }
        
        private async Task SubscribeWithRetryAsync(CancellationToken cancellationToken)
        {
            var retryCount = 0;
            var maxRetryDelay = TimeSpan.FromSeconds(30);
            
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var client = _connectionManager.CreateClient<HubService.HubServiceClient>();
                    
                    var request = new SubscribeRequest();
                    request.EventTypes.Add(HubEventType.MergeMessage);
                    request.FromId = _lastProcessedEventId;
                    
                    _logger.LogInformation("Starting subscription from event ID {EventId} (attempt {Attempt})", 
                        _lastProcessedEventId, retryCount + 1);
                    
                    using var call = client.Subscribe(request, cancellationToken: cancellationToken);
                    
                    // Reset retry count on successful connection
                    retryCount = 0;
                    
                    await foreach (var hubEvent in call.ResponseStream.ReadAllAsync(cancellationToken))
                    {
                        Interlocked.Increment(ref _totalEventsReceived);
                        
                        // Update last processed ID
                        _lastProcessedEventId = hubEvent.Id;
                        
                        // Apply filtering
                        if (ShouldProcessEvent(hubEvent, out var filteredEvent))
                        {
                            Interlocked.Increment(ref _filteredEventsCount);
                            
                            // Write to output channel (will apply backpressure if full)
                            await _outputChannel.Writer.WriteAsync(filteredEvent!, cancellationToken);
                        }
                    }
                    
                    // If we get here, the stream ended normally
                    _logger.LogWarning("Stream ended unexpectedly, will reconnect");
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
                {
                    _logger.LogDebug("Stream cancelled");
                    throw new OperationCanceledException();
                }
                catch (RpcException ex)
                {
                    retryCount++;
                    var delay = TimeSpan.FromSeconds(Math.Min(Math.Pow(2, retryCount - 1), maxRetryDelay.TotalSeconds));
                    
                    _logger.LogWarning(ex, "gRPC error in subscription (attempt {Attempt}), retrying in {Delay}s", 
                        retryCount, delay.TotalSeconds);
                    
                    await Task.Delay(delay, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error in subscription");
                    throw;
                }
            }
        }
        
        private bool ShouldProcessEvent(HubEvent hubEvent, out FilteredHubEvent? filteredEvent)
        {
            filteredEvent = null;
            
            // Only process merge messages
            if (hubEvent.Type != HubEventType.MergeMessage)
                return false;
                
            var message = hubEvent.MergeMessageBody?.Message;
            if (message?.Data == null)
                return false;
                
            // Check message type filter
            if (!_options.MessageTypes.Contains(message.Data.Type))
                return false;
                
            // Check FID filter (empty set means all FIDs)
            if (_options.FilteredFids.Count > 0 && !_options.FilteredFids.Contains(message.Data.Fid))
                return false;
                
            // Create filtered event
            filteredEvent = new FilteredHubEvent
            {
                EventId = hubEvent.Id,
                Fid = message.Data.Fid,
                MessageType = message.Data.Type,
                Message = message,
                Timestamp = hubEvent.Timestamp,
                RawData = hubEvent.ToByteArray()
            };
            
            return true;
        }
        
        public async ValueTask DisposeAsync()
        {
            _internalCts.Cancel();
            
            if (_subscriptionTask != null)
            {
                try
                {
                    await _subscriptionTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }
            
            _outputChannel.Writer.TryComplete();
            _internalCts.Dispose();
        }
    }
}