# A Fast Farcaster Hub Client

## Usage
```bash
# Download all casts (default)
dotnet run --project HubClient.Production/HubClient.Production.csproj -c Release

# Explicitly specify cast messages
dotnet run --project HubClient.Production/HubClient.Production.csproj -c Release --type casts

# Download all reactions
dotnet run --project HubClient.Production/HubClient.Production.csproj -c Release --type reactions

# Download all links
dotnet run --project HubClient.Production/HubClient.Production.csproj -c Release --type links

# Only download messages for a specific FID (977233)
dotnet run --project HubClient.Production/HubClient.Production.csproj -c Release --type links --mine
```

This will start downloading all messages to:
- Casts: `outputs/casts/cast_messages/`
- Reactions: `outputs/reactions/reaction_messages/`
- Links: `outputs/links/link_messages/`

If you want to update it to only pull a specific person's data, or to call a different gRPC call like get profile information or identity proofs, please update HubClient.Production/Program.cs

## Intro

Hi, I needed every cast on Farcaster on my computer for analysis, well, need is a strong word, I wanted.

Then I tried using the clients I could find but they were painfully slow, so I said "i wonder if .net is a good solution for this cause it has a good concurrency story" -- that was the end of my coding contributions to this repo, the rest was done by my good friend Claude with some shoulder watching and occasional help from me.

As of 2/28/2025, this repo can pull:
- All casts from every user FID 1 million and below (157 mil casts excluding deleted casts) in 69.4 minutes
- All reactions (likes and recasts) with similar performance characteristics
- All links between users with the same high-performance design

How we approached this can be found in IMPLEMENTATION_STEPS.md which we used as the roadmap for the project. The main approach was to try different parts of the pipeline, profile them, pick the best one and move onto the next part.

It was actually quite fun! The highlights are:

- When connected to a Hub on the same machine: can pull data at 30k gRPC messages a second.
- Configurable to optimize whatever your machine has the most of, RAM, disk space, etc.
- Writes directly to parquet files making storage tiny and compatible with a whole ecosystem of tools
- Supports cast messages, reactions, and links with the same high-performance characteristics

If you're interested in the process that went into putting this together, I (mostly Claude) first made [minimal-hub-server](https://github.com/jc4p/minimal-hub-server) to have a local testing environment that wouldn't need the storage space a real hub needs.

Then I condensed the instructions for how to connect to the server into SERVER_USAGE.md which is copy pasted into this repo.

LEARNINGS.md covers all of the daily reports from the various Claude agents that worked on the project, and their thoughts on the optimal strategy to use. BENCHMARKS_*.md contain the benchmarks for each phase.

Alright, that's enough from me, I'll pass it over to my buddy Claude:

## Project Rationale and Documentation

It was our goal to explore the design and implementation of a high-performance .NET client for the Farcaster protocol. Our strategy focused on three key pillars:

1. **Performance at Scale**: Designing systems to efficiently handle millions of messages with minimal resource usage
2. **Reliability**: Implementing robust error handling and resilience patterns for network operations
3. **Developer Experience**: Creating an intuitive API that's both powerful and approachable

This repository represents our attack plan and experimental implementation toward these objectives, documenting both successful approaches and lessons learned along the way.

---

A high-performance .NET 9.0 client for the Farcaster Hub, with optimizations for handling large-scale message processing and storage.

## Features

- **High-throughput Message Processing**: Optimized for crawling and processing millions of messages efficiently.
- **Scalable Data Storage**: Parquet file storage with configurable compression and optimized I/O patterns.
- **Resilient gRPC Communication**: Robust error handling and retry logic for reliable Hub API interactions.
- **Parallel Processing**: Multi-threaded processing that scales with available CPU cores.
- **Comprehensive Benchmarking**: Detailed performance metrics for various client configurations.

## Components

### Production-Ready .NET Client

The `HubClient.Production` namespace contains a complete, production-ready client that can:

- Connect to any Farcaster Hub implementation
- Efficiently retrieve messages by FID, message type, or other criteria
- Process and store messages in optimized Parquet file format
- Handle pagination and large datasets without memory issues
- Provide detailed logging and error handling

### Benchmark Suite

The benchmarking suite allows for performance testing across different:

- Serialization strategies (standard, pooled, unsafe)
- Concurrency models
- Storage configurations
- Compression algorithms
- Network scenarios

Benchmarks are implemented using BenchmarkDotNet for reliable, repeatable performance measurements.

## Architecture and Design Choices

- **Resilient Client Design**: Implements the Circuit Breaker and Retry patterns for robust API connections.
- **Memory-Efficient Processing**: Uses pooled buffers and minimizes allocations for high-throughput scenarios.
- **Optimized Storage**: Parallel writer implementation that scales linearly with available CPU cores.
- **Extensible Architecture**: Interface-based design that allows for custom implementations of core components.

## Getting Started

```bash
# Build the solution
dotnet build

# Run the client (crawls messages from FID 1,000,000 down to 1)
cd HubClient/HubClient.Production
dotnet run
```

## Production Usage Example

The client can be used to efficiently crawl and store millions of cast messages:

```csharp
var options = new OptimizedHubClientOptions
{
    ServerEndpoint = "http://localhost:2283",
    ChannelCount = 8,
    MaxConcurrentCallsPerChannel = 500
};

using var client = new OptimizedHubClient(options, logger);
var storage = storageFactory.CreateHighThroughputStorage<Message>("cast_messages", messageConverter);

// Process FIDs and store messages
foreach (uint fid in fidRange)
{
    var messages = await client.GetCastMessagesByFidAsync(fid);
    foreach (var message in messages)
    {
        await storage.AddAsync(message);
    }
}
```

## Requirements

- .NET 9.0 SDK or later
- A running Farcaster Hub instance (local or remote)

## Notes

This repository also contains a minimal Node.js client using Bun in the project root, which provides a simplified implementation for testing Hub connectivity. 