---
license: mit
pretty_name: Farcaster Links
---

# Farcaster Public Links Dataset

This dataset contains public follow relationships from the Farcaster social protocol that have not been deleted by their authors. The dataset includes comprehensive metadata for each link, allowing for detailed analysis of the social graph in the Farcaster ecosystem.

## Dataset Description

The dataset contains the following fields for each active link:
- `Fid`: The Farcaster ID of the user who created the link
- `MessageType`: Type of message (all are 'LinkAdd' as deleted links are excluded)
- `Timestamp`: Seconds since the Farcaster Epoch (January 1, 2021 00:00:00 UTC)
- `LinkType`: The type of link (all 'follow')
- `TargetFid`: The Farcaster ID of the user being followed

## Timestamp Format

Timestamps in this dataset use the Farcaster epoch (seconds since January 1, 2021 00:00:00 UTC).

To convert to a standard Unix timestamp:
1. Add 1609459200 (Unix timestamp for January 1, 2021 00:00:00 UTC) 
2. The result will be a standard Unix timestamp

Example in Python:
```python
# Convert Farcaster timestamp to Unix timestamp
unix_timestamp = farcaster_timestamp + 1609459200

# Convert to datetime
from datetime import datetime
dt = datetime.fromtimestamp(unix_timestamp)
```

## Key Statistics
- Format: Single Parquet file
- Link types: Primarily 'follow' relationships
- Dataset includes all non-deleted links from the Farcaster network as of March 31, 2025

## Intended Uses

This dataset can be useful for:
- Analyzing the social graph of the Farcaster network
- Identifying influential users through follower analysis
- Studying follow/following patterns and community structures
- Tracking growth of the network over time
- Building recommendation algorithms based on social connections
- Researching information diffusion patterns
- Creating social network visualizations
- Analyzing community formation and evolution

## Example Queries

Using Python and pandas:
```python
import pandas as pd
import numpy as np
from datetime import datetime

# Load the dataset
df = pd.read_parquet('farcaster_links.parquet')

# Convert timestamps to datetime
df['datetime'] = df['Timestamp'].astype(int).apply(
    lambda x: datetime.fromtimestamp(x + 1609459200)
)

# Count followers for each user
follower_counts = df.groupby('TargetFid').size().sort_values(ascending=False)

# Top 20 most followed accounts
top_followed = follower_counts.head(20)

# Analyze follow trends over time
monthly_follows = df.groupby(df['datetime'].dt.strftime('%Y-%m')).size()
```

Using DuckDB:
```sql
-- Count followers for each user
SELECT 
  TargetFid,
  COUNT(*) AS follower_count
FROM 'farcaster_links.parquet'
GROUP BY TargetFid
ORDER BY follower_count DESC
LIMIT 20;

-- Find users with most outgoing follows
SELECT 
  Fid,
  COUNT(*) AS following_count
FROM 'farcaster_links.parquet'
GROUP BY Fid
ORDER BY following_count DESC
LIMIT 20;

-- Analyze follow growth by day
SELECT 
  DATE_TRUNC('day', TIMESTAMP '2021-01-01 00:00:00' + (CAST(Timestamp AS BIGINT) * INTERVAL '1 second')) AS follow_date,
  COUNT(*) AS daily_follow_count
FROM 'farcaster_links.parquet'
GROUP BY follow_date
ORDER BY follow_date;
```

## Limitations
- Does not include user profile information
- Only includes LinkAdd relationships (not other link types)
- Does not include the content of users' casts
- Does not include information about user interactions

## Ethics & Privacy
- This dataset contains only public follow relationships that were publicly accessible on the Farcaster protocol
- Deleted relationships have been excluded to respect user content choices
- Personal information beyond public FIDs has been excluded
- Users should respect Farcaster's terms of service when using this data
- Researchers should be mindful of potential biases in social media datasets

## License
MIT License

## Updates
This dataset represents a snapshot of Farcaster follow relationships as of March 31, 2025. For live data, please refer to the Farcaster protocol directly or check for updated versions of this dataset.

