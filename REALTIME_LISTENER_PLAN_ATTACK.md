# 7-Day Attack Plan: Farcaster Cast Event Listener

## Day 1: Proto Foundation & Minimal gRPC Connection
**Goal: Establish working gRPC connection with compiled protos and basic event streaming**

### Morning (4 hours)
- Create solution structure with .NET 9.0 project
- Add all proto files from Farcaster
- Configure Grpc.Tools and compile protos successfully
- Verify generated C# classes are correct

### Afternoon (4 hours)
- Build minimal gRPC client that connects to `snapchain.farcaster.xyz:3383`
- Implement `GetInfo` RPC call to verify connection
- Create basic `Subscribe` call that prints raw event count to console
- Profile: Memory allocations during connection, connection establishment time

### Deliverables
- Working gRPC connection
- Console app that streams events and prints count every second
- Benchmark results: events/second received, memory baseline

### Profile Points
```csharp
// Add these from day 1
BenchmarkDotNet for micro-benchmarks
dotMemory Unit for allocation testing
Simple console metrics: events/sec, MB allocated
```

---

## Day 2: High-Performance Event Pipeline
**Goal: Build zero-allocation streaming pipeline with channels**

### Morning (4 hours)
- Implement `System.Threading.Channels` pipeline
- Build bounded channel with backpressure (10k capacity)
- Create producer task (gRPC reader) and consumer task structure
- Add filtered FID configuration and hot-path filtering

### Afternoon (4 hours)
- Optimize `ShouldProcessEvent` to be allocation-free
- Implement event batching logic with `PeriodicTimer`
- Add reconnection logic with exponential backoff
- Profile: Allocations per event, channel throughput

### Deliverables
- Full channel pipeline processing 10k+ events/second
- Zero allocations in hot path (verified with dotMemory)
- Benchmark showing channel overhead < 1μs per event

### Key Optimizations
```csharp
// Pre-allocate and reuse
private readonly HashSet<ulong> _filteredFids; // Pre-computed
private readonly byte[] _reuseBuffer = new byte[4096]; // For serialization

// Struct for zero-allocation passing
readonly record struct EventMetadata(ulong Fid, MessageType Type, ulong EventId);
```

---

## Day 3: Redis Integration & Batching
**Goal: Implement high-throughput Redis pipeline with batching**

### Morning (4 hours)
- Setup StackExchange.Redis with optimal configuration
- Implement pipelined operations with fire-and-forget
- Build batch accumulator with efficient data structures
- Test pure Redis throughput (benchmark just Redis ops)

### Afternoon (4 hours)
- Connect pipeline to Redis with batching logic
- Implement last event ID persistence/recovery
- Add Redis operation metrics and latency tracking
- Profile: Redis latency, throughput with various batch sizes

### Deliverables
- Redis achieving 50k+ ops/second with batching
- Optimal batch size determined (likely 100-500)
- P99 latency < 5ms for batch flushes

### Profiling Code
```csharp
public class RedisBenchmark
{
    // Benchmark different batch sizes
    [Params(10, 50, 100, 200, 500)]
    public int BatchSize { get; set; }
    
    [Benchmark]
    public async Task BatchedWrites()
    {
        // Measure throughput and latency
    }
}
```

---

## Day 4: Serialization Optimization
**Goal: Minimize serialization overhead and memory usage**

### Morning (4 hours)
- Benchmark protobuf vs MessagePack vs raw binary
- Implement custom minimal serialization for cast events
- Use `ArrayPool<byte>` for serialization buffers
- Test memory pressure under load

### Afternoon (4 hours)
- Implement zero-copy protobuf passthrough option
- Add compression experiments (LZ4, Brotli)
- Optimize for Redis memory usage
- Profile: Serialization time, memory allocations, Redis storage size

### Deliverables
- Serialization taking < 100ns per event
- Memory usage < 50MB for 10k events/second
- Decision on optimal serialization strategy

### Options to Test
```csharp
// Option 1: Raw protobuf passthrough
hubEvent.ToByteArray() // Simple but larger

// Option 2: Custom binary format
Span<byte> buffer = stackalloc byte[256];
var written = WriteCastEvent(buffer, hubEvent);

// Option 3: MessagePack with custom resolver
MessagePackSerializer.Serialize(simplified, _resolverWithArrayPool);
```

---

## Day 5: Resilience & Production Hardening
**Goal: Handle all failure modes gracefully**

### Morning (4 hours)
- Implement comprehensive Polly policies
- Add circuit breakers for Hub and Redis
- Build connection health monitoring
- Test disconnection scenarios

### Afternoon (4 hours)
- Add graceful shutdown with event draining
- Implement startup recovery from last event ID
- Add comprehensive error logging and metrics
- Profile: Recovery time, message loss during failures

### Deliverables
- Zero message loss during reconnections
- < 5 second recovery from any failure
- Comprehensive health check endpoints

### Test Scenarios
```csharp
// Chaos testing
- Kill gRPC connection mid-stream
- Redis connection timeout
- Channel full scenarios
- Graceful shutdown under load
```

---

## Day 6: Performance Tuning & Optimization
**Goal: Achieve target performance metrics**

### Morning (4 hours)
- Run full load tests (10k+ events/second)
- Profile with PerfView for CPU hotspots
- Optimize any allocation hotspots found
- Tune GC settings (Server GC, concurrent)

### Afternoon (4 hours)
- Implement performance counters and ETW events
- Add real-time metrics endpoint
- Optimize Redis connection pooling
- Final round of allocation hunting

### Deliverables
- Stable 10k events/second throughput
- < 100MB memory usage under load
- < 100ms end-to-end latency (P99)

### Performance Dashboard
```csharp
app.MapGet("/metrics", () => new
{
    EventsPerSecond = _metrics.EventRate,
    ChannelDepth = _channel.Reader.Count,
    RedisLatencyMs = _metrics.RedisLatencyP99,
    MemoryMB = GC.GetTotalMemory(false) / 1024 / 1024,
    Gen0Collections = GC.CollectionCount(0),
    LastEventId = _lastProcessedEventId
});
```

---

## Day 7: Production Deployment & Monitoring
**Goal: Production-ready deployment with full observability**

### Morning (4 hours)
- Create production Dockerfile with optimal settings
- Setup Kubernetes manifests with resource limits
- Add Prometheus metrics export
- Configure structured logging with Serilog

### Afternoon (4 hours)
- Load test in production-like environment
- Create runbook for operations
- Document all configuration options
- Final performance validation

### Deliverables
- Docker image < 100MB
- Kubernetes deployment with autoscaling
- Grafana dashboard for monitoring
- Complete operations documentation

### Production Config
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: farcaster-listener
data:
  appsettings.Production.json: |
    {
      "FarcasterHub": {
        "Url": "https://snapchain.farcaster.xyz:3383"
      },
      "FilteredFids": [1, 2, 3, 100, 500],
      "Redis": {
        "ConnectionString": "redis-cluster:6379,abortConnect=false"
      }
    }
```

---

## Daily Profiling Checklist

Every day, run these benchmarks and track progress:

1. **Throughput Test**: Max events/second processed
2. **Allocation Test**: Allocations per 1000 events
3. **Latency Test**: P50/P95/P99 end-to-end latency
4. **Memory Test**: Memory usage at 10k events/second
5. **CPU Test**: CPU usage at max throughput

```csharp
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net90)]
public class DailyBenchmark
{
    [Benchmark]
    public async Task ProcessThousandEvents()
    {
        // Your daily benchmark
    }
}
```

Track results in a spreadsheet:
| Day | Events/sec | Allocations/1k | P99 Latency | Memory MB | CPU % |
|-----|------------|----------------|-------------|-----------|-------|
| 1   | 1,000      | 50,000         | 500ms       | 200       | 80    |
| 2   | 5,000      | 1,000          | 200ms       | 150       | 60    |
| ... | Target: 10k| Target: 0      | Target: 100ms| Target: 100| <50  |

This plan front-loads the hard performance work and saves production hardening for the end. Each day builds on the previous, with constant profiling to ensure you're hitting performance targets.
