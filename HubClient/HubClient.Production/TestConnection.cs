using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Google.Protobuf;
using System.Linq;
using HubClient.Production.Storage;
using HubClient.Core.Storage;

namespace HubClient.Production
{
    /// <summary>
    /// Test class to verify gRPC connection by fetching the last 10 pages of casts for FID 977233
    /// </summary>
    public class TestConnection
    {
        private const uint TEST_FID = 977233;
        private const uint PAGE_SIZE = 100;
        private const int PAGES_TO_FETCH = 10;
        private const long FarcasterEpochOffsetSeconds = 1609459200L;
        
        /// <summary>
        /// Converts a byte array to a hex string with 0x prefix
        /// </summary>
        private static string ToHexString(ByteString bytes)
        {
            if (bytes == null || bytes.Length == 0)
                return "";
            return "0x" + Convert.ToHexString(bytes.ToByteArray()).ToLower();
        }
        
        /// <summary>
        /// Converts a byte array to a hex string with 0x prefix
        /// </summary>
        private static string ToHexString(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
                return "";
            return "0x" + Convert.ToHexString(bytes).ToLower();
        }
        
        public static async Task RunTest()
        {
            Console.WriteLine("=== HubClient gRPC Connection Test ===");
            Console.WriteLine($"Testing connection by fetching last {PAGES_TO_FETCH} pages of casts for FID {TEST_FID}");
            Console.WriteLine();
            
            // Set up logging
            var serviceProvider = new ServiceCollection()
                .AddLogging(builder =>
                {
                    builder.AddConsole();
                    builder.SetMinimumLevel(LogLevel.Information);
                })
                .BuildServiceProvider();
                
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger<TestConnection>();
            
            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                // Get hub configuration from environment or use defaults
                var hubUrl = Environment.GetEnvironmentVariable("HUB_URL") ?? "http://localhost:3383";
                var hubApiKey = Environment.GetEnvironmentVariable("HUB_API_KEY");
                
                logger.LogInformation($"Connecting to hub at: {hubUrl}");
                if (!string.IsNullOrEmpty(hubApiKey))
                {
                    logger.LogInformation("Using API key authentication");
                }
                
                // Create the optimized hub client
                // Allow overrides similar to RealtimeListener env config style
                var channelCountEnv = Environment.GetEnvironmentVariable("FARCASTERHUB__CHANNELCOUNT");
                var maxCallsEnv = Environment.GetEnvironmentVariable("FARCASTERHUB__MAXCONCURRENTCALLSPERCHANNEL");
                int channelCount = 2;  // Small number for test
                int maxConcurrentPerChannel = 10;
                if (int.TryParse(channelCountEnv, out var parsedChannels) && parsedChannels > 0)
                {
                    channelCount = parsedChannels;
                }
                if (int.TryParse(maxCallsEnv, out var parsedMaxCalls) && parsedMaxCalls > 0)
                {
                    maxConcurrentPerChannel = parsedMaxCalls;
                }

                var options = new OptimizedHubClientOptions
                {
                    ServerEndpoint = hubUrl,
                    ChannelCount = channelCount,
                    MaxConcurrentCallsPerChannel = maxConcurrentPerChannel,
                    ApiKey = hubApiKey
                };
                
                using var client = new OptimizedHubClient(options, loggerFactory.CreateLogger<OptimizedHubClient>());
                
                logger.LogInformation("Initializing hub client...");
                await client.InitializeAsync();
                logger.LogInformation("Hub client initialized successfully!");
                
                // Create directory for output
                var outputDir = Path.Combine(Directory.GetCurrentDirectory(), "test_output");
                var dataDir = Path.Combine(outputDir, "casts");
                Directory.CreateDirectory(dataDir);
                
                // Create schema generator and storage factory
                var schemaGenerator = new DefaultSchemaGenerator();
                var storageFactory = new OptimizedStorageFactory(
                    loggerFactory,
                    schemaGenerator,
                    dataDir);
                
                // Define message converter for cast messages
                Func<Message, IDictionary<string, object>> messageConverter = message =>
                {
                    var dict = new Dictionary<string, object>
                    {
                        ["Fid"] = (long)(message.Data?.Fid ?? 0),
                        ["MessageType"] = message.Data?.Type.ToString() ?? "Unknown",
                        ["Timestamp"] = (long)(message.Data?.Timestamp ?? 0),
                        ["Hash"] = ToHexString(message.Hash),
                        ["SignatureScheme"] = message.SignatureScheme.ToString(),
                        ["Signature"] = ToHexString(message.Signature),
                        ["Signer"] = ToHexString(message.Signer),
                        // Initialize cast-specific fields
                        ["Text"] = "",
                        ["Mentions"] = "",
                        ["ParentCastId"] = "",
                        ["ParentUrl"] = "",
                        ["Embeds"] = "",
                        ["TargetHash"] = ""
                    };
                    
                    // Add CastAddBody specific properties if available
                    if (message.Data?.CastAddBody != null)
                    {
                        dict["Text"] = message.Data.CastAddBody.Text;
                        dict["Mentions"] = string.Join(",", message.Data.CastAddBody.Mentions);
                        dict["ParentCastId"] = message.Data.CastAddBody.ParentCastId != null ? 
                            $"{message.Data.CastAddBody.ParentCastId.Fid}:{ToHexString(message.Data.CastAddBody.ParentCastId.Hash)}" : "";
                        dict["ParentUrl"] = message.Data.CastAddBody.ParentUrl;
                        dict["Embeds"] = message.Data.CastAddBody.Embeds.Count > 0 ? 
                            string.Join("|", message.Data.CastAddBody.Embeds) : "";
                    }
                    
                    // Add CastRemoveBody specific properties if available
                    if (message.Data?.CastRemoveBody != null)
                    {
                        dict["TargetHash"] = message.Data.CastRemoveBody.TargetHash != null ? 
                            ToHexString(message.Data.CastRemoveBody.TargetHash) : "";
                    }
                    
                    return dict;
                };
                
                // Create storage for cast messages
                var storage = storageFactory.CreateHighThroughputStorage<Message>(
                    $"test_casts_fid_{TEST_FID}",
                    messageConverter);
                
                // Calculate max allowed timestamp to filter out messages with timestamps in the future
                // Allow 1 day buffer for clock skew
                long currentUnixTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                long maxAllowedFarcasterTimestamp = (currentUnixTime - FarcasterEpochOffsetSeconds) + (24 * 60 * 60); // Current time + 1 day in Farcaster epoch
                logger.LogInformation($"Filtering out messages with timestamps beyond {DateTimeOffset.FromUnixTimeSeconds(maxAllowedFarcasterTimestamp + FarcasterEpochOffsetSeconds).DateTime} UTC (current time + 1 day buffer).");
                
                // Track statistics
                int totalCasts = 0;
                int pagesRetrieved = 0;
                int skippedFutureCasts = 0;
                
                // Create gRPC client
                var hubServiceClient = client.CreateClient<HubService.HubServiceClient>();
                
                // Pagination variables
                byte[]? pageToken = null;
                
                logger.LogInformation($"Starting to fetch casts for FID {TEST_FID}...");
                
                // Fetch pages
                for (int pageNum = 1; pageNum <= PAGES_TO_FETCH; pageNum++)
                {
                    var pageStopwatch = Stopwatch.StartNew();
                    
                    // Create request
                    var request = new FidTimestampRequest 
                    { 
                        Fid = TEST_FID,
                        PageSize = PAGE_SIZE
                    };
                    
                    if (pageToken != null)
                    {
                        request.PageToken = ByteString.CopyFrom(pageToken);
                    }
                    
                    try
                    {
                        // Make the gRPC call
                        var response = await hubServiceClient.CallAsync(
                            async (client, ct) => await client.GetAllCastMessagesByFidAsync(request, cancellationToken: ct).ResponseAsync,
                            $"GetAllCastMessagesByFid-Page{pageNum}");
                        
                        var castsOnPage = response.Messages.Count;
                        totalCasts += castsOnPage;
                        pagesRetrieved++;
                        
                        logger.LogInformation($"Page {pageNum}: Retrieved {castsOnPage} casts in {pageStopwatch.ElapsedMilliseconds}ms");
                        
                        // Process and store cast data
                        foreach (var message in response.Messages)
                        {
                            // Skip messages with timestamps in the future (beyond our max allowed timestamp)
                            if (message.Data != null && message.Data.Timestamp > maxAllowedFarcasterTimestamp)
                            {
                                logger.LogWarning($"Skipping message with future timestamp: FID={message.Data.Fid}, Timestamp={message.Data.Timestamp} (Unix: {message.Data.Timestamp + FarcasterEpochOffsetSeconds}), Hash={ToHexString(message.Hash)}");
                                skippedFutureCasts++;
                                continue;
                            }
                            
                            await storage.AddAsync(message);
                        }
                        
                        // Check if there are more pages
                        if (response.NextPageToken != null && response.NextPageToken.Length > 0)
                        {
                            pageToken = response.NextPageToken.ToByteArray();
                        }
                        else
                        {
                            logger.LogInformation($"No more pages available. Stopped at page {pageNum}.");
                            break;
                        }
                        
                        // If no casts on first page, likely no casts for this FID
                        if (pageNum == 1 && castsOnPage == 0)
                        {
                            logger.LogWarning($"No casts found for FID {TEST_FID}");
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError($"Error fetching page {pageNum}: {ex.Message}");
                        if (pageNum == 1)
                        {
                            throw; // Re-throw if first page fails
                        }
                        break;
                    }
                }
                
                // Flush and close the storage
                await storage.FlushAsync();
                
                // Dispose the storage to clean up resources
                if (storage is IAsyncDisposable asyncDisposable)
                {
                    await asyncDisposable.DisposeAsync();
                }
                else if (storage is IDisposable disposable)
                {
                    disposable.Dispose();
                }
                
                stopwatch.Stop();
                
                // Print summary
                Console.WriteLine();
                Console.WriteLine("=== Test Results ===");
                Console.WriteLine($"✅ Connection successful!");
                Console.WriteLine($"Total casts retrieved: {totalCasts}");
                Console.WriteLine($"Pages retrieved: {pagesRetrieved}/{PAGES_TO_FETCH}");
                if (skippedFutureCasts > 0)
                {
                    Console.WriteLine($"⚠️  Skipped {skippedFutureCasts} casts with future timestamps");
                }
                Console.WriteLine($"Total time: {stopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"Average time per page: {(pagesRetrieved > 0 ? stopwatch.ElapsedMilliseconds / pagesRetrieved : 0)}ms");
                Console.WriteLine();
                Console.WriteLine($"Results saved to: {Path.Combine(dataDir, $"test_casts_fid_{TEST_FID}")}/");
                Console.WriteLine($"Data format: Parquet files");
                
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Test failed with error");
                Console.WriteLine();
                Console.WriteLine("❌ Test FAILED!");
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine();
                Console.WriteLine("Troubleshooting tips:");
                Console.WriteLine("1. Make sure the hub is running and accessible");
                Console.WriteLine("2. Check the HUB_URL environment variable (default: http://localhost:3383)");
                Console.WriteLine("3. If using authentication, ensure HUB_API_KEY is set correctly");
                Console.WriteLine("4. Verify that FID 977233 exists and has cast data");
                
                Environment.Exit(1);
            }
        }
    }
}
