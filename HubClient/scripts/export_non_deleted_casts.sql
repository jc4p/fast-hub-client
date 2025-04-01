-- Create a temporary table with all the hashes that have been removed
CREATE OR REPLACE TEMP TABLE removed_casts AS
SELECT TargetHash 
FROM read_parquet('output/casts/casts_messages/*.parquet') 
WHERE MessageType = 'CastRemove';

-- Select all CastAdd messages that don't have a corresponding CastRemove
COPY (
  SELECT c.* 
  FROM read_parquet('output/casts/casts_messages/*.parquet') c
  WHERE c.MessageType = 'CastAdd'
  AND c.Hash NOT IN (SELECT TargetHash FROM removed_casts)
) TO 'farcaster_casts.parquet';

-- Drop the temporary table
DROP TABLE removed_casts;
