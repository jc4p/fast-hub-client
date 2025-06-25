# Farcaster Realtime Listener

A high-performance .NET 9.0 service that subscribes to Farcaster Hub events in real-time, filters them based on configuration, and streams them to Redis queues for processing.

## Features

- **Real-time Event Streaming**: Subscribes to Farcaster Hub's event stream using gRPC
- **High Performance**: Processes 20,000+ events/second with minimal memory usage
- **JSON Output**: Normalized JSON format for easy consumption (no protobuf parsing needed)
- **Ultra-fast Serialization**: Uses Jil for JSON serialization (2-3x faster than System.Text.Json)
- **Latency Tracking**: Measures and logs processing latency in milliseconds
- **Smart Filtering**: Filters by FIDs, message types, and message age (skips old backlog)
- **Redis Integration**: Batched writes to Redis with pipelining for maximum throughput
- **Resilient**: Automatic reconnection with exponential backoff
- **Stateful Recovery**: Resumes from last processed event ID after restarts
- **Production Ready**: Comprehensive logging, metrics, and health checks

## Architecture

```
Farcaster Hub → gRPC Subscribe → RealtimeSubscriber → ChannelPipeline → RedisBatchWriter → Redis
                                        ↓
                                   FID & Type Filter
```

### Components

1. **RealtimeSubscriber**: Manages the gRPC subscription and filters events
2. **ChannelPipeline**: High-performance producer/consumer pipeline from HubClient.Core
3. **RedisBatchWriter**: Batches events and writes to Redis with configurable flush intervals
4. **MultiplexedChannelManager**: Manages multiple gRPC channels for load balancing

## Quick Start

### Prerequisites

- .NET 9.0 SDK
- Redis server running locally or accessible
- Access to a Farcaster Hub (set HUB_URL environment variable or update appsettings.json)

### Running

```bash
# Navigate to the project directory
cd RealtimeListener.Production

# Set your Farcaster Hub URL (required)
export HUB_URL=http://your-hub-url:2283

# Set API key if your hub requires authentication (optional)
export HUB_API_KEY=your-api-key

# Example for Neynar's SSL hub (Option 1 approach):
export HUB_URL=https://snapchain-grpc-api.neynar.com:443
export HUB_API_KEY=your-neynar-api-key

# Development mode
dotnet run

# Production mode with custom config
dotnet run --environment Production

# Clear Redis data before starting fresh
redis-cli DEL farcaster:casts:add farcaster:casts:remove farcaster:last_event_id
```

## Configuration

### appsettings.json

```json
{
  "FarcasterHub": {
    "Url": "http://your-hub-url:2283",
    "ApiKey": null,  // Optional: Set to your API key if required
    "ChannelCount": 2,
    "MaxConcurrentCallsPerChannel": 500
  },
  "Subscription": {
    "FromEventId": null,              // null = resume from Redis, or specify event ID
    "BufferCapacity": 10000,
    "FilteredFids": [],               // Empty = all FIDs, or specify array of FIDs
    "MessageTypes": ["CastAdd", "CastRemove"]
  },
  "Redis": {
    "ConnectionString": "localhost:6379",
    "CastAddQueueKey": "farcaster:casts:add",
    "CastRemoveQueueKey": "farcaster:casts:remove",
    "LastEventIdKey": "farcaster:last_event_id",
    "MaxBatchSize": 100,
    "MaxBatchWaitMs": 100
  }
}
```

### Filtering Specific FIDs

To filter events to specific FIDs, update the configuration:

```json
{
  "Subscription": {
    "FilteredFids": [1, 2, 3, 5650, 3621]
  }
}
```

Or use the example config:
```bash
cp appsettings.FilteredFids.json appsettings.Production.json
dotnet run --environment Production
```

## Redis Queues

The service writes to two Redis lists:

- `farcaster:casts:add` - New cast messages
- `farcaster:casts:remove` - Removed cast messages

Each message is stored as a **normalized JSON object** for easy consumption.

### Message Format

```json
{
  "fid": 1234,
  "hash": "abc123...",
  "timestamp": 123456789,
  "event_id": 129566326787,
  "message_type": "cast_add",
  "text": "Hello Farcaster!",
  "embeds": [],
  "mentions": [5678],
  "mentions_positions": [6],
  "parent_hash": "def456...",
  "processed_at": 1703123456789
}
```

### Processing Messages

Example Python consumer:
```python
import redis
import json

r = redis.Redis(decode_responses=False)
while True:
    # Blocking pop from queue
    _, data = r.blpop('farcaster:casts:add')
    cast = json.loads(data.decode('utf-8'))
    
    # Process the cast
    print(f"Cast from FID {cast['fid']}: {cast.get('text', '')}")
    print(f"Latency: {cast['processed_at'] - (1609459200000 + cast['timestamp'] * 1000)}ms")
```

### Peeking at Queue Contents

Use the included `peek_queue.py` utility:
```bash
# View first 5 messages in the add queue
python peek_queue.py

# View specific queue and count
python peek_queue.py farcaster:casts:remove 10
```

This shows:
- Message creation time
- Redis insertion time  
- Processing latency in milliseconds
- Time sitting in queue

## Performance

With default configuration on a modern machine:

- **Throughput**: 20,000+ filtered events/second
- **Memory Usage**: < 100MB under normal load
- **Latency**: < 100ms end-to-end (Hub → Redis)
- **JSON Serialization**: ~50μs per message using Jil
- **Redis Batch Size**: 100-200 events per batch
- **Zero Message Loss**: Automatic recovery from all transient failures
- **Age Filtering**: Automatically skips messages older than 1 minute to avoid backlog

## Monitoring

The service logs comprehensive metrics every 5 seconds:

```
Processed 294 | Subscriber: 214746 total, 294 filtered (0.14 %) | Redis: 2 items, 2 batches (avg 1.0) | Pipeline: 293 processed
```

Additionally, for each processed cast:
```
Cast processed - FID: 557, Hash: b4abcd7c6387bb5908ff56ba327219de7c0c8c1b, Latency: 553ms
```

Pipeline statistics are logged every minute:
```
Pipeline Statistics - Processed: 294, Success rate: 100.00 %, Failures: 0
```

## Graceful Shutdown

The service handles SIGTERM/SIGINT gracefully:

1. Stops accepting new events
2. Processes all buffered events
3. Flushes final Redis batch
4. Saves last processed event ID
5. Cleanly disconnects

## Environment Variables

All configuration values can be overridden with environment variables:

```bash
# Hub configuration (alternative to HUB_URL and HUB_API_KEY)
export FarcasterHub__Url=http://your-hub:2283
export FarcasterHub__ApiKey=your-api-key
export FarcasterHub__ChannelCount=4

# Redis configuration
export Redis__ConnectionString=your-redis:6379

# Subscription configuration
export Subscription__FilteredFids__0=1
export Subscription__FilteredFids__1=2
```

## Docker

```dockerfile
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish "RealtimeListener.Production/RealtimeListener.Production.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "RealtimeListener.Production.dll"]
```

## Development

### Building

```bash
dotnet build
```

### Testing with specific FIDs

```bash
# Edit appsettings.json
dotnet run
```

### Debugging

Set minimum log level to Debug:
```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Debug"
    }
  }
}
```

## License

MIT