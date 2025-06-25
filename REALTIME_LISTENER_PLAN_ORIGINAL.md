## Original Requirements:

Based on the actual Farcaster protobuf files and channeling Marc Gravell's high-performance .NET approach, here's an updated PRD:

# Farcaster Cast Event Listener - .NET 9.0 PRD (Marc Gravell Style)

## Overview
Build a high-performance .NET 9.0 service that connects to a Farcaster Hub via gRPC, subscribes to cast events from specific FIDs, and streams them to Redis with minimal allocations and maximum throughput.

## Core Architecture Principles (The Gravell Way)

1. **Zero-allocation hot paths** - Use `ArrayPool<T>`, `Span<T>`, and stackalloc where possible
2. **Channels over queues** - `System.Threading.Channels` for internal buffering
3. **Streaming all the way down** - Never materialize full collections
4. **protobuf-net for performance** - Or Google.Protobuf with careful pooling
5. **Pipelined Redis operations** - Fire-and-forget with batching

## Protobuf Setup

### 1. Proto Files Structure
```
Protos/
├── message.proto          # Core message definitions
├── hub_event.proto        # Hub event wrapper
├── request_response.proto # RPC service definitions
├── onchain_event.proto    # OnChain events (we'll filter these out)
├── username_proof.proto   # Username proofs (we'll filter these out)
└── blocks.proto          # Block-related messages
```

### 2. Key Message Definitions (from actual protos)

```protobuf
// hub_event.proto - This is our main envelope
message HubEvent {
  HubEventType type = 1;
  uint64 id = 2;
  oneof body {
    MergeMessageBody merge_message_body = 3;
    PruneMessageBody prune_message_body = 4;
    RevokeMessageBody revoke_message_body = 5;
    // ... other types we ignore
  }
  uint64 block_number = 12;
  uint32 shard_index = 14;
  uint64 timestamp = 15;
}

message MergeMessageBody {
  Message message = 1;
  repeated Message deleted_messages = 2;
}

// message.proto - The actual cast data
message Message {
  MessageData data = 1;
  bytes hash = 2;
  HashScheme hash_scheme = 3;
  bytes signature = 4;
  SignatureScheme signature_scheme = 5;
  bytes signer = 6;
  optional bytes data_bytes = 7;
}

message MessageData {
  MessageType type = 1;
  uint64 fid = 2;
  uint32 timestamp = 3;
  FarcasterNetwork network = 4;
  oneof body {
    CastAddBody cast_add_body = 5;
    CastRemoveBody cast_remove_body = 6;
    // ... other types we ignore
  }
}
```

### 3. Project File Setup

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">
  <PropertyGroup>
    <TargetFramework>net9.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ServerGarbageCollection>true</ServerGarbageCollection>
    <ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
  </PropertyGroup>

  <ItemGroup>
    <!-- gRPC and Protobuf -->
    <PackageReference Include="Grpc.AspNetCore" Version="2.65.0" />
    <PackageReference Include="Google.Protobuf" Version="3.27.0" />
    <PackageReference Include="Grpc.Tools" Version="2.65.0" PrivateAssets="all" />
    
    <!-- High-performance libraries -->
    <PackageReference Include="StackExchange.Redis" Version="2.8.0" />
    <PackageReference Include="MessagePack" Version="2.5.0" /> <!-- Faster than JSON -->
    <PackageReference Include="System.IO.Pipelines" Version="9.0.0" />
    
    <!-- Resilience -->
    <PackageReference Include="Polly" Version="8.4.0" />
  </ItemGroup>

  <ItemGroup>
    <Protobuf Include="Protos\*.proto" GrpcServices="Client" />
  </ItemGroup>
</Project>
```

## High-Performance Implementation

### 1. Channel-Based Event Pipeline

```csharp
public sealed class FarcasterEventPipeline : IHostedService, IAsyncDisposable
{
    private readonly Channel<HubEvent> _eventChannel;
    private readonly IConnectionMultiplexer _redis;
    private readonly HashSet<ulong> _filteredFids;
    private readonly ILogger<FarcasterEventPipeline> _logger;
    private readonly SemaphoreSlim _connectionSemaphore = new(1, 1);
    
    private GrpcChannel? _grpcChannel;
    private HubService.HubServiceClient? _client;
    private CancellationTokenSource? _cts;

    public FarcasterEventPipeline(
        IConnectionMultiplexer redis,
        IConfiguration config,
        ILogger<FarcasterEventPipeline> logger)
    {
        _redis = redis;
        _logger = logger;
        _filteredFids = config.GetSection("FilteredFids").Get<ulong[]>()?.ToHashSet() 
            ?? new HashSet<ulong>();
        
        // High-performance channel with bounded capacity
        _eventChannel = Channel.CreateBounded<HubEvent>(new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        
        // Start the pipeline tasks
        var tasks = new[]
        {
            Task.Run(() => SubscribeToHubAsync(_cts.Token)),
            Task.Run(() => ProcessEventsAsync(_cts.Token))
        };
        
        // Don't await - let them run in background
        _ = Task.WhenAll(tasks).ContinueWith(t =>
        {
            if (t.IsFaulted)
                _logger.LogError(t.Exception, "Pipeline failed");
        });
    }

    private async Task SubscribeToHubAsync(CancellationToken ct)
    {
        var retryPolicy = Policy
            .Handle<RpcException>()
            .WaitAndRetryForeverAsync(
                retryAttempt => TimeSpan.FromSeconds(Math.Min(30, Math.Pow(2, retryAttempt))),
                onRetry: (ex, delay) => _logger.LogWarning($"Retrying after {delay}"));

        await retryPolicy.ExecuteAsync(async () =>
        {
            await _connectionSemaphore.WaitAsync(ct);
            try
            {
                await ConnectToHubAsync(ct);
                await StreamEventsAsync(ct);
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        });
    }

    private async Task ConnectToHubAsync(CancellationToken ct)
    {
        _grpcChannel?.Dispose();
        
        _grpcChannel = GrpcChannel.ForAddress("https://snapchain.farcaster.xyz:3383", new GrpcChannelOptions
        {
            HttpHandler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
                KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
                EnableMultipleHttp2Connections = true
            }
        });
        
        _client = new HubService.HubServiceClient(_grpcChannel);
    }

    private async Task StreamEventsAsync(CancellationToken ct)
    {
        var lastEventId = await GetLastProcessedEventIdAsync();
        
        var request = new SubscribeRequest
        {
            EventTypes = { HubEventType.HubEventTypeMergeMessage },
            FromId = lastEventId
        };

        using var call = _client!.Subscribe(request, cancellationToken: ct);
        
        // Process stream with minimal allocations
        await foreach (var hubEvent in call.ResponseStream.ReadAllAsync(ct))
        {
            // Fast path filtering
            if (!ShouldProcessEvent(hubEvent))
                continue;
                
            // Write to channel (backpressure handled by bounded channel)
            await _eventChannel.Writer.WriteAsync(hubEvent, ct);
        }
    }

    private bool ShouldProcessEvent(HubEvent hubEvent)
    {
        // Fast path: only merge messages
        if (hubEvent.Type != HubEventType.HubEventTypeMergeMessage)
            return false;
            
        var message = hubEvent.MergeMessageBody?.Message;
        if (message?.Data == null)
            return false;
            
        // Check FID filter
        if (!_filteredFids.Contains(message.Data.Fid))
            return false;
            
        // Only CastAdd and CastRemove
        return message.Data.Type is MessageType.MessageTypeCastAdd 
            or MessageType.MessageTypeCastRemove;
    }

    private async Task ProcessEventsAsync(CancellationToken ct)
    {
        var db = _redis.GetDatabase();
        var batch = new List<(string key, byte[] value)>(100);
        var batchTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        
        await Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                var readTask = _eventChannel.Reader.ReadAsync(ct).AsTask();
                var timerTask = batchTimer.WaitForNextTickAsync(ct).AsTask();
                
                var completed = await Task.WhenAny(readTask, timerTask);
                
                if (completed == readTask && readTask.IsCompletedSuccessfully)
                {
                    var hubEvent = await readTask;
                    var serialized = SerializeEvent(hubEvent);
                    
                    var key = hubEvent.MergeMessageBody.Message.Data.Type == MessageType.MessageTypeCastAdd
                        ? "farcaster:casts:add"
                        : "farcaster:casts:remove";
                        
                    batch.Add((key, serialized));
                    
                    if (batch.Count >= 100)
                        await FlushBatchAsync(db, batch, hubEvent.Id);
                }
                else if (batch.Count > 0)
                {
                    await FlushBatchAsync(db, batch);
                }
            }
        });
    }

    private async Task FlushBatchAsync(IDatabase db, List<(string key, byte[] value)> batch, ulong? lastEventId = null)
    {
        if (batch.Count == 0) return;
        
        // Use pipelining for efficiency
        var tasks = new Task[batch.Count + (lastEventId.HasValue ? 1 : 0)];
        var i = 0;
        
        foreach (var (key, value) in batch)
        {
            tasks[i++] = db.ListRightPushAsync(key, value);
        }
        
        if (lastEventId.HasValue)
        {
            tasks[i] = db.StringSetAsync("farcaster:last_event_id", lastEventId.Value);
        }
        
        await Task.WhenAll(tasks);
        batch.Clear();
    }

    private byte[] SerializeEvent(HubEvent hubEvent)
    {
        // Option 1: Use protobuf directly (most efficient)
        return hubEvent.ToByteArray();
        
        // Option 2: Use MessagePack for smaller custom format
        // var simplified = new CastEvent
        // {
        //     Fid = hubEvent.MergeMessageBody.Message.Data.Fid,
        //     Hash = hubEvent.MergeMessageBody.Message.Hash,
        //     Timestamp = hubEvent.Timestamp,
        //     Type = hubEvent.MergeMessageBody.Message.Data.Type == MessageType.MessageTypeCastAdd ? "add" : "remove",
        //     CastData = ExtractCastData(hubEvent.MergeMessageBody.Message)
        // };
        // return MessagePackSerializer.Serialize(simplified);
    }

    private async Task<ulong> GetLastProcessedEventIdAsync()
    {
        var db = _redis.GetDatabase();
        var value = await db.StringGetAsync("farcaster:last_event_id");
        return value.HasValue ? (ulong)value : 0;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts?.Cancel();
        _eventChannel.Writer.TryComplete();
        await (_grpcChannel?.ShutdownAsync() ?? Task.CompletedTask);
    }

    public async ValueTask DisposeAsync()
    {
        _grpcChannel?.Dispose();
        _cts?.Dispose();
        _connectionSemaphore?.Dispose();
    }
}
```

### 2. Configuration

```json
{
  "FarcasterHub": {
    "Url": "https://snapchain.farcaster.xyz:3383"
  },
  "FilteredFids": [1, 2, 3, 100, 500],
  "Redis": {
    "ConnectionString": "localhost:6379,abortConnect=false,syncTimeout=3000",
    "ChannelPrefix": "__keyspace@0__"
  }
}
```

### 3. Program.cs

```csharp
var builder = WebApplication.CreateBuilder(args);

// Configure high-performance settings
builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MaxConcurrentConnections = 1000;
    options.Limits.MaxConcurrentUpgradedConnections = 1000;
});

// Redis with connection pooling
builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var config = ConfigurationOptions.Parse(builder.Configuration["Redis:ConnectionString"]);
    config.AsyncTimeout = 3000;
    config.SyncTimeout = 3000;
    return ConnectionMultiplexer.Connect(config);
});

// Register the pipeline
builder.Services.AddHostedService<FarcasterEventPipeline>();

// Health checks
builder.Services.AddHealthChecks()
    .AddRedis(builder.Configuration["Redis:ConnectionString"]);

var app = builder.Build();

app.MapHealthChecks("/health");
app.MapGet("/metrics", () => Results.Ok(new
{
    // Add metrics here
}));

app.Run();
```

## Performance Optimizations

1. **Zero-Copy Streaming**: Events flow directly from gRPC -> Channel -> Redis without intermediate collections
2. **Batched Redis Operations**: Accumulate events and flush periodically or when batch is full
3. **Protocol Buffer Pass-Through**: Store raw protobuf bytes in Redis to avoid serialization overhead
4. **Bounded Channels**: Prevent memory exhaustion with backpressure
5. **Fire-and-Forget Redis**: Use pipelined operations without waiting for individual ACKs

## Monitoring & Operations

1. **Metrics to Track**:
   - Events per second by type (add/remove)
   - Channel queue depth
   - Redis operation latency
   - gRPC connection health
   - Last processed event ID lag

2. **Health Checks**:
   - Redis connectivity
   - Hub connection status
   - Channel capacity

## Success Criteria

- Process 10,000+ events/second without dropping
- < 100ms end-to-end latency (Hub -> Redis)
- Automatic recovery from all transient failures
- Memory usage < 100MB under normal load
- Zero message loss during reconnections

This approach follows Marc Gravell's philosophy: build for performance first, use modern C# features effectively, and always measure rather than guess about performance.
