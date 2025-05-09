# HubClient.Production

A high-performance gRPC client implementation for Hub services, optimized based on extensive benchmarking results.

## Key Features

- **Highly Optimized**: Uses the fastest approaches identified through rigorous benchmarking
- **Memory Efficient**: Minimizes allocations and GC pressure with pooling and unsafe operations
- **Resilient**: Built-in retry policies, circuit breakers, and connection management
- **Configurable**: Flexible options to tune performance for different workload patterns
- **Thread-Safe**: Safe for concurrent use from multiple threads
- **Message Crawler**: Built-in support for crawling and exporting cast and reaction messages to Parquet files

## Message Crawler

The HubClient includes a high-performance message crawler that can export both cast messages and reactions to Parquet files. The crawler processes FIDs from 1M down to 1, collecting all messages and storing them in an optimized format.

### Usage

```bash
# Crawl cast messages (default)
dotnet run

# Explicitly specify cast messages
dotnet run --type casts

# Crawl reaction messages
dotnet run --type reactions

# Crawl messages for a specific FID
dotnet run --fid 977233

# Crawl profile data for a specific FID
dotnet run --profiles --fid 977233
```

### Output Structure

The crawler creates the following directory structure:

```
output/
├── casts/
│   └── casts_messages/      # Parquet files containing cast messages
└── reactions/
    └── reactions_messages/  # Parquet files containing reaction messages
```

### Data Schema

#### Cast Messages
Fields stored for each cast message:
- `Fid`: The Farcaster ID of the message creator
- `MessageType`: Type of the message (CastAdd/CastRemove)
- `Timestamp`: When the message was created
- `Hash`: Unique hash of the message
- `SignatureScheme`: Signature scheme used
- `Signature`: Message signature
- `Signer`: Public key of the signer
- `Text`: Content of the cast (for CastAdd)
- `Mentions`: Comma-separated list of mentioned FIDs
- `ParentCastId`: ID of the parent cast if this is a reply
- `ParentUrl`: URL being replied to (if any)
- `Embeds`: Pipe-separated list of embedded content
- `TargetHash`: Hash of the cast being removed (for CastRemove)

#### Reaction Messages
Fields stored for each reaction:
- `Fid`: The Farcaster ID of the reactor
- `MessageType`: Type of the message (ReactionAdd/ReactionRemove)
- `Timestamp`: When the reaction was created
- `Hash`: Unique hash of the message
- `SignatureScheme`: Signature scheme used
- `Signature`: Message signature
- `Signer`: Public key of the signer
- `ReactionType`: Type of reaction (like/recast)
- `TargetCastId`: ID of the cast being reacted to
- `TargetUrl`: URL being reacted to (if any)

### Performance Characteristics

The crawler is optimized for high-throughput processing:
- Uses 8 managed channels with 500 concurrent calls per channel
- Implements efficient pagination with 100 messages per page
- Periodically flushes data to disk (every 1000 FIDs)
- Provides detailed progress tracking and time estimates
- Handles errors gracefully with automatic retries

### Example Output

```
# For full crawl:
Starting HubClient cast message crawler - processing from 1,050,000 down to 1...
Progress: 25.50% complete | Current: FID 750000 | Stats: 1234 active FIDs, 56789 total cast messages | Time: 45.2 minutes elapsed, ~131.8 minutes remaining

# For specific FID:
Starting HubClient cast message crawler - processing for FID 977233...
FID 977233: Retrieved 412 cast messages in 1523ms
```

## Performance Optimizations

This client incorporates the following optimizations based on benchmark results:

1. **Connection Management**: 
   - Uses `MultiplexedChannelManager` with 8 managed channels (identified as optimal in benchmarks)
   - Implements connection pooling with 1000 concurrent calls per channel
   - Includes health monitoring with circuit breaker pattern

2. **Memory Management**:
   - Utilizes `UnsafeMemoryAccess` for zero-copy message handling
   - Implements object pooling for message instances
   - Uses `RecyclableMemoryStream` for I/O operations
   - Pre-allocates buffers based on expected data volume

3. **Concurrency Pipeline**:
   - Implements `Channel`-based pipeline for balanced performance
   - Configures optimal batch sizes (25000 for most workloads)
   - Uses controlled parallelism (capped at 16 cores)
   - Implements backpressure handling with bounded queues

4. **Serialization**:
   - Implements direct memory manipulation for fastest performance
   - Uses pooled buffers for serialization operations
   - Provides span-based APIs for zero-allocation operations
   - Optimizes for both small and large messages

5. **Error Handling**:
   - Implements Polly-based retry policies with exponential backoff
   - Adds circuit breaker for failing endpoints
   - Includes detailed error tracking and metrics

## Usage Examples

### Basic Usage

```csharp
// Create client with default optimized options
using var client = new OptimizedHubClient("http://localhost:5293");

// Get casts for a specific FID
var response = await client.GetCastsByFidAsync(1234);

// Process the results
foreach (var message in response.Messages)
{
    Console.WriteLine($"Cast: {message.Data.CastAddBody.Text}");
}
```

### Advanced Configuration

```csharp
// Create custom options for specific workloads
var options = new OptimizedHubClientOptions
{
    // Tune connection pooling
    ChannelCount = 16,                                  // More channels for high concurrency
    MaxConcurrentCalls = 2000,                          // Higher limits for busy servers
    
    // Configure pipeline behavior
    BatchSize = 10000,                                  // Smaller batches for lower latency
    PipelineStrategy = PipelineStrategy.Channel,        // Balanced performance
    
    // Set memory optimization strategy
    SerializerType = OptimizedHubClientOptions.SerializerType.UnsafeMemoryAccess,
    
    // Configure resilience
    MaxRetryAttempts = 5,                              // More retries for unstable networks
    TimeoutMilliseconds = 60000                        // Longer timeout for slow responses
};

// Create client with custom options
using var client = new OptimizedHubClient("http://localhost:5293", options);

// Use with pagination
var response = await client.GetCastsByFidAsync(
    fid: 1234,
    pageSize: 50,
    pageToken: null,
    reverse: true);
    
// Get subsequent pages
while (response.NextPageToken != null)
{
    response = await client.GetCastsByFidAsync(
        fid: 1234,
        pageSize: 50,
        pageToken: response.NextPageToken,
        reverse: true);
        
    // Process results...
}
```

## Performance Metrics

The client automatically collects performance metrics during operation:

```csharp
// Access metrics
var metrics = client.Metrics;

// Log performance information
Console.WriteLine($"Requests: {metrics.SuccessCount + metrics.FailureCount}");
Console.WriteLine($"Successful: {metrics.SuccessCount}");
Console.WriteLine($"Failed: {metrics.FailureCount}");
Console.WriteLine($"Average latency: {metrics.AverageLatency.TotalMilliseconds:F2}ms");
Console.WriteLine($"P99 latency: {metrics.P99Latency.TotalMilliseconds:F2}ms");
Console.WriteLine($"Throughput: {metrics.AverageThroughput:F2} msgs/sec");
```

## Workload-Specific Recommendations

Based on benchmark findings, these configurations are recommended for specific workloads:

### High-Throughput Applications

```csharp
var options = new OptimizedHubClientOptions
{
    ChannelCount = 8,
    BatchSize = 25000,
    SerializerType = OptimizedHubClientOptions.SerializerType.UnsafeMemoryAccess
};
```

### Low-Latency Applications

```csharp
var options = new OptimizedHubClientOptions
{
    ChannelCount = 16,
    BatchSize = 10000,
    SerializerType = OptimizedHubClientOptions.SerializerType.UnsafeMemoryAccess,
    TimeoutMilliseconds = 5000
};
```

### Resource-Constrained Environments

```csharp
var options = new OptimizedHubClientOptions
{
    ChannelCount = 4,
    BatchSize = 5000,
    SerializerType = OptimizedHubClientOptions.SerializerType.PooledBuffer
};
``` 