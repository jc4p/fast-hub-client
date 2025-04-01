-- Configure DuckDB to use all available resources
SET memory_limit='190GB';  -- Leave some headroom for system
SET threads=30;

-- Create an optimized temporary table for all link events
CREATE OR REPLACE TEMP TABLE all_link_events AS 
SELECT 
  Fid, 
  TargetFid, 
  LinkType,
  MessageType,
  Hash,
  CAST(Timestamp AS BIGINT) AS TimestampInt,
  *  -- Keep all columns for later
FROM read_parquet('output/links/links_messages/*.parquet')
WHERE MessageType IN ('LinkAdd', 'LinkRemove');

-- Create indexes for faster joins
CREATE INDEX idx_link_relation ON all_link_events(Fid, TargetFid, LinkType);

-- Find the latest event for each link relationship
CREATE OR REPLACE TEMP TABLE latest_events AS
SELECT 
  Fid, 
  TargetFid, 
  LinkType,
  MAX(TimestampInt) AS MaxTimestamp
FROM all_link_events
GROUP BY Fid, TargetFid, LinkType;

-- Join to get final valid links (only where the latest event is a LinkAdd)
COPY (
  SELECT l.*
  FROM all_link_events l
  JOIN latest_events e
    ON l.Fid = e.Fid 
    AND l.TargetFid = e.TargetFid 
    AND l.LinkType = e.LinkType
    AND l.TimestampInt = e.MaxTimestamp
  WHERE l.MessageType = 'LinkAdd'
) TO 'farcaster_links.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Clean up
DROP TABLE all_link_events;
DROP TABLE latest_events;

