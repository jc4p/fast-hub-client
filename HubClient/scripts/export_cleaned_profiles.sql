-- Set the memory limit to use most of your available RAM
SET memory_limit='180GB';

-- Configure parallelism to use your CPU cores
SET threads=30;

-- Create a wide format table from all parquet files
CREATE TABLE user_profiles AS
SELECT 
    Fid,
    MAX(CASE WHEN UserDataType = 'Username' THEN Value END) AS Username,
    MAX(CASE WHEN UserDataType = 'Display' THEN Value END) AS Display,
    MAX(CASE WHEN UserDataType = 'Location' THEN Value END) AS Location,
    MAX(CASE WHEN UserDataType = 'Pfp' THEN Value END) AS Pfp,
    MAX(CASE WHEN UserDataType = 'Bio' THEN Value END) AS Bio,
    MAX(CASE WHEN UserDataType = 'Twitter' THEN Value END) AS Twitter
FROM read_parquet('output/profiles/profiles_messages/*.parquet')
GROUP BY Fid;

-- Export the result to a compressed parquet file
COPY user_profiles TO 'user_profiles_04_05_2025.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Optional: Display information about the resulting table
SELECT COUNT(*) FROM user_profiles;