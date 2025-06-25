using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RealtimeListener.Production.Concurrency;
using RealtimeListener.Production.Grpc;
using RealtimeListener.Production.Storage;
using RealtimeListener.Production.Serialization;
using StackExchange.Redis;
using Jil;

namespace RealtimeListener.Production;

class Program
{
    static async Task Main(string[] args)
    {
        var host = CreateHostBuilder(args).Build();
        
        var logger = host.Services.GetRequiredService<ILogger<Program>>();
        
        try
        {
            logger.LogInformation("Starting Farcaster Realtime Listener...");
            await host.RunAsync();
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Application terminated unexpectedly");
        }
    }
    
    static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                      .AddJsonFile($"appsettings.{context.HostingEnvironment.EnvironmentName}.json", optional: true, reloadOnChange: true)
                      .AddEnvironmentVariables()
                      .AddCommandLine(args);
            })
            .ConfigureServices((context, services) =>
            {
                var configuration = context.Configuration;
                
                // Configure Redis
                services.AddSingleton<IConnectionMultiplexer>(sp =>
                {
                    var connectionString = configuration["Redis:ConnectionString"] ?? "localhost:6379";
                    var config = ConfigurationOptions.Parse(connectionString);
                    config.AbortOnConnectFail = false;
                    return ConnectionMultiplexer.Connect(config);
                });
                
                // Configure services
                services.AddSingleton<MultiplexedChannelManager>();
                services.AddSingleton<RedisBatchWriter>();
                services.AddHostedService<RealtimeListenerService>();
                
                // Configure options
                services.Configure<RealtimeListenerOptions>(configuration);
            })
            .ConfigureLogging((context, logging) =>
            {
                logging.ClearProviders();
                logging.AddConfiguration(context.Configuration.GetSection("Logging"));
                logging.AddConsole();
                logging.AddDebug();
            });
}

/// <summary>
/// Configuration options for the realtime listener
/// </summary>
public class RealtimeListenerOptions
{
    public FarcasterHubOptions FarcasterHub { get; set; } = new();
    public SubscriptionOptions Subscription { get; set; } = new();
    public RedisOptions Redis { get; set; } = new();
    public PipelineOptions Pipeline { get; set; } = new();
}

public class FarcasterHubOptions
{
    public string Url { get; set; } = Environment.GetEnvironmentVariable("HUB_URL") ?? "http://localhost:2283";
    public string? ApiKey { get; set; } = Environment.GetEnvironmentVariable("HUB_API_KEY");
    public int ChannelCount { get; set; } = 2;
    public int MaxConcurrentCallsPerChannel { get; set; } = 500;
}

public class SubscriptionOptions
{
    public ulong? FromEventId { get; set; }
    public int BufferCapacity { get; set; } = 10000;
    public List<ulong> FilteredFids { get; set; } = new();
    public List<string> MessageTypes { get; set; } = new() { "CastAdd", "CastRemove" };
}

public class RedisOptions
{
    public string ConnectionString { get; set; } = "localhost:6379";
    public string CastAddQueueKey { get; set; } = "farcaster:casts:add";
    public string CastRemoveQueueKey { get; set; } = "farcaster:casts:remove";
    public string LastEventIdKey { get; set; } = "farcaster:last_event_id";
    public int MaxBatchSize { get; set; } = 100;
    public int MaxBatchWaitMs { get; set; } = 100;
    public bool EnablePipelining { get; set; } = true;
}

public class PipelineOptions
{
    public int MaxConcurrency { get; set; } = 8;
    public int BoundedCapacity { get; set; } = 10000;
    public bool PreserveOrder { get; set; } = false;
}

/// <summary>
/// The main hosted service that runs the realtime listener
/// </summary>
public class RealtimeListenerService : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RealtimeListenerService> _logger;
    private readonly IHostApplicationLifetime _lifetime;
    
    private MultiplexedChannelManager? _channelManager;
    private RealtimeSubscriber? _subscriber;
    private RedisBatchWriter? _batchWriter;
    private ChannelPipeline<FilteredHubEvent, FilteredHubEvent>? _pipeline;
    
    private static readonly Options JilOptions = new Options(
        excludeNulls: true,
        includeInherited: false,
        dateFormat: DateTimeFormat.MillisecondsSinceUnixEpoch,
        serializationNameFormat: SerializationNameFormat.CamelCase
    );
    
    public RealtimeListenerService(
        IConfiguration configuration,
        IConnectionMultiplexer redis,
        ILogger<RealtimeListenerService> logger,
        ILoggerFactory loggerFactory,
        IHostApplicationLifetime lifetime)
    {
        _configuration = configuration;
        _redis = redis;
        _logger = logger;
        _loggerFactory = loggerFactory;
        _lifetime = lifetime;
    }
    
    private readonly ILoggerFactory _loggerFactory;
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // Load configuration
            var options = _configuration.Get<RealtimeListenerOptions>() ?? new RealtimeListenerOptions();
            
            // Create channel manager with optional API key
            _channelManager = new MultiplexedChannelManager(
                serverEndpoint: options.FarcasterHub.Url,
                channelCount: options.FarcasterHub.ChannelCount,
                maxConcurrentCallsPerChannel: options.FarcasterHub.MaxConcurrentCallsPerChannel,
                apiKey: options.FarcasterHub.ApiKey
            );
            
            // Test connection
            var client = _channelManager.CreateClient<HubService.HubServiceClient>();
            var info = await client.GetInfoAsync(new GetInfoRequest());
            _logger.LogInformation("Connected to Hub. Version: {Version}", info.Version);
            
            // Create Redis batch writer
            var batchWriterOptions = new RedisBatchWriterOptions
            {
                MaxBatchSize = options.Redis.MaxBatchSize,
                MaxBatchWait = TimeSpan.FromMilliseconds(options.Redis.MaxBatchWaitMs),
                CastAddQueueKey = options.Redis.CastAddQueueKey,
                CastRemoveQueueKey = options.Redis.CastRemoveQueueKey,
                LastEventIdKey = options.Redis.LastEventIdKey,
                EnablePipelining = options.Redis.EnablePipelining
            };
            
            _batchWriter = new RedisBatchWriter(_redis, batchWriterOptions, 
                _loggerFactory.CreateLogger<RedisBatchWriter>());
            
            // Ensure Redis queues exist
            await _batchWriter.EnsureQueuesExistAsync();
            
            // Load last event ID from Redis
            var lastEventId = await _batchWriter.LoadLastEventIdAsync();
            
            // If no last event ID, get the latest events from the hub
            if (lastEventId == 0)
            {
                lastEventId = await GetLatestEventIdAsync(client);
                _logger.LogInformation("No saved event ID, starting from latest: {EventId}", lastEventId);
            }
            
            // Configure subscriber
            var subscriberOptions = new RealtimeSubscriberOptions
            {
                FromEventId = options.Subscription.FromEventId ?? lastEventId,
                BufferCapacity = options.Subscription.BufferCapacity,
                FilteredFids = options.Subscription.FilteredFids.ToHashSet(),
                MessageTypes = ParseMessageTypes(options.Subscription.MessageTypes)
            };
            
            _logger.LogInformation("Starting subscription from event ID {EventId}", subscriberOptions.FromEventId);
            
            if (subscriberOptions.FilteredFids.Count > 0)
            {
                _logger.LogInformation("Filtering to FIDs: {Fids}", string.Join(", ", subscriberOptions.FilteredFids));
            }
            else
            {
                _logger.LogInformation("Processing events from all FIDs");
            }
            
            _subscriber = new RealtimeSubscriber(_channelManager, subscriberOptions, 
                _loggerFactory.CreateLogger<RealtimeSubscriber>());
            
            // Create pipeline for processing
            var pipelineOptions = new PipelineCreationOptions
            {
                MaxConcurrency = options.Pipeline.MaxConcurrency,
                BoundedCapacity = options.Pipeline.BoundedCapacity,
                PreserveOrderInBatch = options.Pipeline.PreserveOrder
            };
            
            _pipeline = new ChannelPipeline<FilteredHubEvent, FilteredHubEvent>(
                ProcessEventAsync,
                pipelineOptions
            );
            
            // Start all components
            await _subscriber.StartAsync(stoppingToken);
            await _batchWriter.StartAsync(stoppingToken);
            
            _logger.LogInformation("All components started successfully");
            
            // Main processing loop
            var reader = _subscriber.GetReader();
            var processedCount = 0;
            var lastLogTime = DateTime.UtcNow;
            var lastStatsTime = DateTime.UtcNow;
            
            await foreach (var filteredEvent in reader.ReadAllAsync(stoppingToken))
            {
                // Send to pipeline for processing
                await _pipeline.EnqueueAsync(filteredEvent, stoppingToken);
                processedCount++;
                
                // Log progress periodically
                var now = DateTime.UtcNow;
                if ((now - lastLogTime).TotalSeconds >= 5)
                {
                    var subscriberStats = _subscriber.GetStatistics();
                    var batchStats = _batchWriter.GetStatistics();
                    var pipelineMetrics = _pipeline.Metrics;
                    
                    _logger.LogInformation(
                        "Processed {Processed} | Subscriber: {Total} total, {Filtered} filtered ({FilterRate:P2}) | " +
                        "Redis: {Items} items, {Batches} batches (avg {AvgBatch:F1}) | " +
                        "Pipeline: {PipelineProcessed} processed",
                        processedCount, 
                        subscriberStats.totalReceived, subscriberStats.filtered, subscriberStats.filterRate,
                        batchStats.itemsWritten, batchStats.batchesWritten, batchStats.avgBatchSize,
                        pipelineMetrics.ProcessedCount);
                    
                    lastLogTime = now;
                }
                
                // Log detailed stats every minute
                if ((now - lastStatsTime).TotalMinutes >= 1)
                {
                    LogDetailedStatistics();
                    lastStatsTime = now;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Service shutdown requested");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Fatal error in realtime listener service");
            _lifetime.StopApplication();
        }
    }
    
    private async ValueTask<FilteredHubEvent> ProcessEventAsync(FilteredHubEvent hubEvent, CancellationToken cancellationToken)
    {
        // Normalize the message to JSON
        var normalized = MessageNormalizer.NormalizeMessage(hubEvent);
        if (normalized != null)
        {
            // Add processing timestamp (milliseconds since Unix epoch)
            var processedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            normalized.ProcessedAt = processedAt;
            
            // Calculate latency from Farcaster timestamp
            // Farcaster epoch is 2021-01-01 00:00:00 UTC (1609459200000 ms)
            const long FARCASTER_EPOCH_MS = 1609459200000L;
            var messageTimestampMs = FARCASTER_EPOCH_MS + ((long)hubEvent.Timestamp * 1000L);
            var latencyMs = processedAt - messageTimestampMs;
            
            // Skip messages older than 1 minute (60000ms) to avoid processing old backlog
            if (latencyMs > 60000)
            {
                _logger.LogDebug("Skipping old message - FID: {Fid}, Hash: {Hash}, Age: {Age}ms", 
                    normalized.Fid, normalized.Hash, latencyMs);
                return hubEvent;
            }
            
            // Log latency if it's reasonable (not negative and not too large)
            if (latencyMs >= 0 && latencyMs < 3600000) // Less than 1 hour
            {
                _logger.LogInformation("Cast processed - FID: {Fid}, Hash: {Hash}, Latency: {Latency}ms", 
                    normalized.Fid, normalized.Hash, latencyMs);
            }
            
            // Serialize to JSON using Jil
            var json = JSON.Serialize(normalized, JilOptions);
            var jsonBytes = System.Text.Encoding.UTF8.GetBytes(json);
            
            // Create a modified event with JSON data instead of protobuf
            var jsonEvent = new FilteredHubEvent
            {
                EventId = hubEvent.EventId,
                Fid = hubEvent.Fid,
                MessageType = hubEvent.MessageType,
                Message = hubEvent.Message,
                Timestamp = hubEvent.Timestamp,
                RawData = jsonBytes // Now contains JSON instead of protobuf
            };
            
            // Add to Redis batch writer
            await _batchWriter!.AddAsync(jsonEvent, cancellationToken);
        }
        
        return hubEvent;
    }
    
    private async Task<ulong> GetLatestEventIdAsync(HubService.HubServiceClient client)
    {
        try
        {
            // Get recent events to find the latest event ID
            // We'll use reverse order to get the most recent events
            var request = new EventsRequest
            {
                PageSize = 1,
                Reverse = true,
                StartId = 0
            };
            
            var response = await client.GetEventsAsync(request);
            
            if (response.Events.Count > 0)
            {
                var latestEvent = response.Events[0];
                _logger.LogInformation("Found latest event ID from hub: {EventId}", latestEvent.Id);
                return latestEvent.Id;
            }
            
            _logger.LogWarning("No events found, starting from event ID 0");
            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get latest event ID, starting from 0");
            return 0;
        }
    }
    
    private HashSet<MessageType> ParseMessageTypes(List<string> messageTypeStrings)
    {
        var result = new HashSet<MessageType>();
        
        foreach (var typeStr in messageTypeStrings)
        {
            if (Enum.TryParse<MessageType>(typeStr, out var messageType))
            {
                result.Add(messageType);
            }
            else
            {
                _logger.LogWarning("Unknown message type in configuration: {Type}", typeStr);
            }
        }
        
        return result;
    }
    
    private void LogDetailedStatistics()
    {
        if (_pipeline != null)
        {
            var metrics = _pipeline.Metrics;
            var successRate = metrics.ProcessedCount > 0 
                ? (double)(metrics.ProcessedCount - metrics.FailureCount) / metrics.ProcessedCount 
                : 0;
            _logger.LogInformation(
                "Pipeline Statistics - Processed: {Processed}, " +
                "Success rate: {SuccessRate:P2}, Failures: {Failures}",
                metrics.ProcessedCount,
                successRate,
                metrics.FailureCount);
        }
    }
    
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping realtime listener service...");
        
        // Dispose in reverse order
        if (_pipeline != null)
        {
            await _pipeline.CompleteAsync(cancellationToken);
            await _pipeline.DisposeAsync();
        }
        
        if (_subscriber != null)
        {
            await _subscriber.DisposeAsync();
        }
        
        if (_batchWriter != null)
        {
            await _batchWriter.DisposeAsync();
        }
        
        _channelManager?.Dispose();
        
        await base.StopAsync(cancellationToken);
        
        _logger.LogInformation("Realtime listener service stopped");
    }
}