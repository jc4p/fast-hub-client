# HubClient.Production

A high-performance gRPC client implementation for Hub services, optimized based on extensive benchmarking results.

## Key Features

- **Highly Optimized**: Uses the fastest approaches identified through rigorous benchmarking
- **Memory Efficient**: Minimizes allocations and GC pressure with pooling and unsafe operations
- **Resilient**: Built-in retry policies, circuit breakers, and connection management
- **Configurable**: Flexible options to tune performance for different workload patterns
- **Thread-Safe**: Safe for concurrent use from multiple threads

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