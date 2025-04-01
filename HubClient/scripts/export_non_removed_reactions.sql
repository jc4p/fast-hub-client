-- Configure DuckDB to use all available resources
SET memory_limit='190GB';  -- Leave some headroom for system
SET threads=30;

-- Create an optimized temporary table for all reaction events with parallel processing
CREATE OR REPLACE TEMP TABLE all_reaction_events AS 
SELECT 
  Fid, 
  TargetCastId,
  ReactionType,
  MessageType,
  Hash,
  CAST(Timestamp AS BIGINT) AS TimestampInt,
  *  -- Keep all columns for later
FROM read_parquet('output/reactions/reactions_messages/*.parquet')
WHERE MessageType IN ('ReactionAdd', 'ReactionRemove');

-- Create indexes for faster joins
CREATE INDEX idx_reaction_relation ON all_reaction_events(Fid, TargetCastId, ReactionType);

-- Find the latest event for each reaction relationship
CREATE OR REPLACE TEMP TABLE latest_events AS
SELECT 
  Fid, 
  TargetCastId,
  ReactionType,
  MAX(TimestampInt) AS MaxTimestamp
FROM all_reaction_events
GROUP BY Fid, TargetCastId, ReactionType;

-- Join to get final valid reactions (only where the latest event is a ReactionAdd)
COPY (
  SELECT r.*
  FROM all_reaction_events r
  JOIN latest_events e
    ON r.Fid = e.Fid 
    AND r.TargetCastId = e.TargetCastId 
    AND r.ReactionType = e.ReactionType
    AND r.TimestampInt = e.MaxTimestamp
  WHERE r.MessageType = 'ReactionAdd'
) TO 'farcaster_reactions.parquet';

-- Clean up
DROP TABLE all_reaction_events;
DROP TABLE latest_events;
