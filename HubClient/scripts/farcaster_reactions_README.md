---
license: mit
pretty_name: Farcaster Reactions
---

# Farcaster Public Reactions Dataset

This dataset contains 295,162,048 public reactions from the Farcaster social protocol that have not been deleted by their authors. The dataset includes comprehensive metadata for each reaction, allowing for detailed analysis of user engagement in the Farcaster ecosystem.

## Dataset Description

The dataset contains the following fields for each reaction:
- `Fid`: The Farcaster ID of the user who created the reaction
- `MessageType`: Type of message (all are 'ReactionAdd' as deleted reactions are excluded)
- `Timestamp`: Seconds since the Farcaster Epoch (January 1, 2021 00:00:00 UTC)
- `Hash`: The unique hash identifier of the reaction
- `SignatureScheme`: The cryptographic signature scheme used
- `Signature`: Cryptographic signature of the reaction
- `Signer`: The public key of the signer
- `ReactionType`: The type of reaction (Like or Recast)
- `TargetCastId`: ID of the cast being reacted to
- `TargetUrl`: URL of external content if this reaction refers to external content

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
- Total number of reactions: 295,162,048
- Format: Single Parquet file
- Reaction types: Like and Recast
- Dataset includes all non-deleted reactions from the Farcaster network as of March 31, 2025

## Intended Uses

This dataset can be useful for:
- Analyzing engagement patterns on content within the Farcaster network
- Identifying influential users and viral content through reaction analysis
- Studying the correlation between different reaction types
- Tracking user engagement over time
- Building recommendation algorithms based on user preferences
- Researching social amplification patterns through Recasts
- Creating engagement prediction models
- Analyzing the relationship between content type and engagement metrics

## Example Queries

Using Python and pandas:
```python
import pandas as pd
import numpy as np
from datetime import datetime

# Load the dataset
df = pd.read_parquet('farcaster_reactions.parquet')

# Convert timestamps to datetime
df['datetime'] = df['Timestamp'].astype(int).apply(
    lambda x: datetime.fromtimestamp(x + 1609459200)
)

# Count reactions by type
reaction_counts = df.groupby('ReactionType').size()

# Find most engaged with content
top_content = df.groupby('TargetCastId').size().sort_values(ascending=False).head(10)

# Analyze reaction trends over time
monthly_reactions = df.groupby([df['datetime'].dt.strftime('%Y-%m'), 'ReactionType']).size().unstack()
```

Using DuckDB:
```sql
-- Count reactions by type
SELECT 
  ReactionType,
  COUNT(*) AS reaction_count
FROM 'farcaster_reactions.parquet'
GROUP BY ReactionType;

-- Find most active users giving reactions
SELECT 
  Fid,
  COUNT(*) AS reaction_count
FROM 'farcaster_reactions.parquet'
GROUP BY Fid
ORDER BY reaction_count DESC
LIMIT 20;

-- Analyze reactions by day
SELECT 
  DATE_TRUNC('day', TIMESTAMP '2021-01-01 00:00:00' + (CAST(Timestamp AS BIGINT) * INTERVAL '1 second')) AS reaction_date,
  ReactionType,
  COUNT(*) AS daily_reaction_count
FROM 'farcaster_reactions.parquet'
GROUP BY reaction_date, ReactionType
ORDER BY reaction_date;
```

## Limitations
- Does not include user profile information
- Only includes Like and Recast reactions (not newer reaction types that may be added)
- Does not include the content of the cast being reacted to

## Ethics & Privacy
- This dataset contains only public reactions that were publicly accessible on the Farcaster protocol
- Deleted reactions have been excluded to respect user content choices
- Personal information beyond public FIDs has been excluded
- Users should respect Farcaster's terms of service when using this data
- Researchers should be mindful of potential biases in social media datasets

## License
MIT License

## Updates
This dataset represents a snapshot of Farcaster reactions as of March 31, 2025. For live data, please refer to the Farcaster protocol directly or check for updated versions of this dataset.

