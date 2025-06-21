---
license: mit
pretty_name: Farcaster Casts
---

# Farcaster Public Casts Dataset

This dataset contains 175,067,847 public casts (posts) from the Farcaster social protocol that have not been deleted by their authors. The dataset includes comprehensive metadata for each cast, allowing for detailed analysis of the Farcaster ecosystem.

## Dataset Description

The dataset contains the following fields for each cast:
- `Fid`: The Farcaster ID of the user who created the cast
- `MessageType`: Type of message (all are 'CastAdd' as deleted casts are excluded)
- `Timestamp`: Seconds since the Farcaster Epoch (January 1, 2021 00:00:00 UTC)
- `Hash`: The unique hash identifier of the cast
- `SignatureScheme`: The cryptographic signature scheme used
- `Signature`: Cryptographic signature of the cast
- `Signer`: The public key of the signer
- `Text`: The text content of the cast
- `Mentions`: User mentions included in the cast
- `ParentCastId`: ID of the parent cast if this is a reply
- `ParentUrl`: URL of external content if this cast is a reply to external content
- `Embeds`: JSON object containing any embedded content (URLs, images, etc.)

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
- Total number of casts: 175,067,847
- Format: Single Parquet file
- Dataset includes all non-deleted casts from the Farcaster network as of June 20, 2025

## Intended Uses

This dataset can be useful for:
- Analyzing conversation patterns and social interactions on Farcaster
- Studying reply chains and community formation through mentions and replies
- Tracking the evolution of discussions and trends
- Training language models on social media discourse
- Analyzing embedded content sharing patterns
- Researching decentralized social network growth and user behavior
- Creating classifiers of likelihood of a cast going viral
- Building network graphs of Farcaster users based on interactions

## Example Queries

Using Python and pandas:
```python
import pandas as pd
import numpy as np
from datetime import datetime

# Load the dataset
df = pd.read_parquet('casts.parquet')

# Convert timestamps to datetime
df['datetime'] = df['Timestamp'].astype(int).apply(
    lambda x: datetime.fromtimestamp(x + 1609459200)
)

# Count casts per month
monthly_counts = df.groupby(df['datetime'].dt.strftime('%Y-%m')).size()

# Find most active users
top_users = df.groupby('Fid').size().sort_values(ascending=False).head(10)

# Analyze reply chains
reply_percentage = (df['ParentCastId'].notna().sum() / len(df)) * 100
```

Using DuckDB:
```sql
-- Count casts by day
SELECT 
  DATE_TRUNC('day', TIMESTAMP '2021-01-01 00:00:00' + (CAST(Timestamp AS BIGINT) * INTERVAL '1 second')) AS cast_date,
  COUNT(*) AS daily_cast_count
FROM 'casts.parquet'
GROUP BY cast_date
ORDER BY cast_date;

-- Find casts with external links
SELECT * FROM 'casts.parquet'
WHERE Embeds LIKE '%url%'
LIMIT 100;
```

## Limitations
- Does not include user profile information
- Does not include reactions or other engagement metrics

## Ethics & Privacy
- This dataset contains only public casts that were publicly accessible on the Farcaster protocol
- Deleted casts have been excluded to respect user content choices
- Personal information beyond public FIDs has been excluded
- Users should respect Farcaster's terms of service when using this data
- Researchers should be mindful of potential biases in social media datasets

## License
MIT License

## Updates
This dataset represents a snapshot of Farcaster casts as of June 20, 2025. For live data, please refer to the Farcaster protocol directly or check for updated versions of this dataset.
