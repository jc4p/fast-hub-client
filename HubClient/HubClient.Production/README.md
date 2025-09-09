# HubClient.Production

A high-performance .NET 9.0 client and crawler for Farcaster Hub. It fetches casts, reactions, links, and profile/user-data messages at scale and writes them to fast, compact Parquet files.

## Features

- Real-world optimized: multiplexed channels, tuned concurrency, efficient pagination
- API key auth: sends `x-api-key` when `HUB_API_KEY` is set (TLS for https)
- Message export: casts, reactions, links, and profile messages to Parquet
- Scalable throughput: 8 channels x 500 calls/channel by default
- Progress + resilience: periodic progress, retries, bounded queues

## Quick Start

### Prerequisites
- .NET 9.0 SDK

### Run
```bash
# Go to production client
cd HubClient/HubClient.Production

# Required: hub URL (local insecure example)
export HUB_URL=http://localhost:3383

# Optional: API key (adds x-api-key); useful for managed hubs
export HUB_API_KEY=your-api-key

# Example for Neynar (SSL + API key)
export HUB_URL=https://snapchain-grpc-api.neynar.com:443
export HUB_API_KEY=your-neynar-api-key

# Crawl cast messages (default)
dotnet run

# Crawl reactions / links / profiles
dotnet run --type reactions
dotnet run --type links
dotnet run --profiles

# Only a specific FID
dotnet run --fid 977233            # with default type=casts
dotnet run --profiles --fid 977233

# Only messages from last N days
dotnet run --days 7                # works with any --type

# Sanity check connection
dotnet run --test-connection

# Export active pro members (FID + primary ETH address)
dotnet run --pro-members
```

## Configuration

These environment variables mirror the Realtime Listener setup and are supported here as well:

- HUB_URL: Required. Hub endpoint, e.g. `http://localhost:3383` or `https://snapchain-grpc-api.neynar.com:443`.
- HUB_API_KEY: Optional. When set, adds `x-api-key` to all gRPC calls and enables TLS for https endpoints.
- FARCASTERHUB__CHANNELCOUNT: Optional. Overrides channel count. Default 8 (2 for `--test-connection`).
- FARCASTERHUB__MAXCONCURRENTCALLSPERCHANNEL: Optional. Overrides max concurrent calls per channel. Default 500 (10 for `--test-connection`).

Notes:
- For secure endpoints without an API key, TLS is still enabled when `HUB_URL` starts with `https://`.
- Defaults are tuned for high throughput; lower them on constrained environments.

## Output

Files are written under `output/` by message type:

```
output/
├── casts/
│   └── casts_messages/      # Parquet files with cast messages
├── reactions/
│   └── reactions_messages/  # Parquet files with reaction messages
├── links/
│   └── links_messages/      # Parquet files with link messages
└── profiles/
    └── profiles_messages/   # Parquet files with user-data messages
```

## Data Schema

### Cast messages
- Fid, MessageType (CastAdd/CastRemove), Timestamp, Hash
- SignatureScheme, Signature, Signer
- Text, Mentions, ParentCastId, ParentUrl, Embeds
- TargetHash (for CastRemove)

### Reaction messages
- Fid, MessageType (ReactionAdd/ReactionRemove), Timestamp, Hash
- SignatureScheme, Signature, Signer
- ReactionType (like/recast), TargetCastId, TargetUrl

### Link messages
- Fid, MessageType (LinkAdd/LinkRemove), Timestamp, Hash
- LinkType, TargetFid, TargetUrl

### Profile/User-data messages
- Fid, MessageType (UserDataAdd/Remove), Timestamp, Hash
- UserDataType (pfp/display/bio/url), Value

## Performance

- 8 channels with 500 concurrent calls/channel by default
- 100 messages/page pagination
- Bounded queues and periodic flushes for stability
- Progress logs every 1000 FIDs with ETA

Tip: adjust `FARCASTERHUB__CHANNELCOUNT` and `FARCASTERHUB__MAXCONCURRENTCALLSPERCHANNEL` for your environment.

## Troubleshooting

- 401/permission errors: set `HUB_API_KEY` and ensure `HUB_URL` uses `https://`.
- Slow or throttled hub: lower channel count or per-channel concurrency.
- Large runs: ensure sufficient disk space under `output/`.
