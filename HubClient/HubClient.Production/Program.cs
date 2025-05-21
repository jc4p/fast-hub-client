using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using System.CommandLine;
using System.CommandLine.Invocation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;
using Google.Protobuf;
using HubClient.Production.Grpc;
using HubClient.Production.Storage;
using HubClient.Production.Serialization;
using HubClient.Core.Storage;
using Parquet.Schema;

namespace HubClient.Production
{
    /// <summary>
    /// Default implementation of ISchemaGenerator for Parquet files
    /// </summary>
    public class DefaultSchemaGenerator : ISchemaGenerator
    {
        /// <summary>
        /// Generates a Parquet schema from a dictionary row
        /// </summary>
        public ParquetSchema GenerateSchema(IDictionary<string, object> sample)
        {
            var fields = new List<Field>();
            
            foreach (var kvp in sample)
            {
                var field = CreateField(kvp.Key, kvp.Value);
                if (field != null)
                {
                    fields.Add(field);
                }
            }
            
            return new ParquetSchema(fields);
        }
        
        private static Field CreateField(string name, object value)
        {
            return value switch
            {
                null => new DataField<string>(name),
                string => new DataField<string>(name),
                int => new DataField<int>(name),
                long => new DataField<long>(name),
                float => new DataField<float>(name),
                double => new DataField<double>(name),
                bool => new DataField<bool>(name),
                DateTime => new DataField<DateTimeOffset>(name),
                DateTimeOffset => new DataField<DateTimeOffset>(name),
                byte[] => new DataField<byte[]>(name),
                _ => new DataField<string>(name) // Convert other types to string
            };
        }
    }
    
    public class Program
    {
        
        public static async Task<int> Main(string[] args)
        {
            // Create command line options
            var rootCommand = new RootCommand("HubClient cast, reaction, link, and profile message crawler");
            
            var messageTypeOption = new Option<string>(
                "--type",
                getDefaultValue: () => "casts",
                description: "Type of messages to crawl (casts, reactions, links)"
            );
            rootCommand.AddOption(messageTypeOption);
            
            var fidOption = new Option<uint?>(
                "--fid",
                getDefaultValue: () => null,
                description: "Only get messages for specified FID (e.g. --fid 977233)"
            );
            rootCommand.AddOption(fidOption);
            
            var profilesOption = new Option<bool>(
                "--profiles",
                getDefaultValue: () => false,
                description: "Get user profile data instead of message content"
            );
            rootCommand.AddOption(profilesOption);

            var daysOption = new Option<int?>(
                "--days",
                getDefaultValue: () => null,
                description: "Number of days of messages to retrieve. If not specified, all messages are retrieved."
            );
            rootCommand.AddOption(daysOption);
            
            rootCommand.SetHandler(async (string messageType, uint? fid, bool profiles, int? days) =>
            {
                if (profiles)
                {
                    await RunCrawler("profiles", fid, days);
                }
                else
                {
                    await RunCrawler(messageType, fid, days);
                }
            }, messageTypeOption, fidOption, profilesOption, daysOption);
            
            return await rootCommand.InvokeAsync(args);
        }
        
        private static async Task RunCrawler(string messageType, uint? specificFid, int? days)
        {
            bool isCastMessages = messageType.ToLower() == "casts";
            bool isReactionMessages = messageType.ToLower() == "reactions";
            bool isLinkMessages = messageType.ToLower() == "links";
            bool isProfileMessages = messageType.ToLower() == "profiles";
            
            string messageTypeDisplay = isCastMessages ? "cast" : 
                                       isReactionMessages ? "reaction" : 
                                       isLinkMessages ? "link" :
                                       isProfileMessages ? "profile" : "unknown";
            
            if (!isCastMessages && !isReactionMessages && !isLinkMessages && !isProfileMessages)
            {
                Console.WriteLine($"Unsupported message type: {messageType}. Please use casts, reactions, links, or profiles.");
                return;
            }
            
            bool isSingleFid = specificFid.HasValue;
            uint startFid = isSingleFid ? specificFid.Value : 1_050_000;
            uint endFid = isSingleFid ? specificFid.Value : 1;
            
            string scopeDisplay = isSingleFid ? $"for FID {specificFid.Value}" : "from 1,050,000 down to 1";
            Console.WriteLine($"Starting HubClient {messageTypeDisplay} message crawler - processing {scopeDisplay}...");

            long? cutoffTimestamp = null;
            if (days.HasValue)
            {
                // Ensure 'days' is not negative, or decide how to handle it (e.g., treat as 0 or throw).
                // For now, assume 'days' will be non-negative as per typical use case.
                cutoffTimestamp = DateTimeOffset.UtcNow.AddDays(-days.Value).ToUnixTimeSeconds();
                Console.WriteLine($"Filtering messages newer than {DateTimeOffset.FromUnixTimeSeconds(cutoffTimestamp.Value).DateTime} UTC ({days.Value} days ago).");
            }
            
            // Set up logging
            var serviceProvider = new ServiceCollection()
                .AddLogging(builder =>
                {
                    builder.AddConsole();
                    builder.SetMinimumLevel(LogLevel.Information);
                })
                .BuildServiceProvider();
                
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger<Program>();
            
            // Create a stopwatch to measure total execution time
            var totalStopwatch = new Stopwatch();
            totalStopwatch.Start();
            
            // For statistics
            int totalSuccessfulFids = 0;
            int totalFailedFids = 0;
            long totalMessagesRetrieved = 0;
            
            try
            {
                // 1. Create the client with increased concurrency for faster crawling
                var options = new OptimizedHubClientOptions
                {
                    ServerEndpoint = "http://localhost:3383", // Updated Hub endpoint
                    ChannelCount = 8,
                    MaxConcurrentCallsPerChannel = 500
                };
                
                using var client = new OptimizedHubClient(options, loggerFactory.CreateLogger<OptimizedHubClient>());
                
                // 2. Set up storage to save to parquet file
                var outputDir = Path.Combine(Directory.GetCurrentDirectory(), "output");
                var dataDir = Path.Combine(outputDir, messageType.ToLower());
                Directory.CreateDirectory(dataDir);
                
                // Create schema generator and storage factory
                var schemaGenerator = new DefaultSchemaGenerator();
                var storageFactory = new OptimizedStorageFactory(
                    loggerFactory,
                    schemaGenerator,
                    dataDir);
                
                // Define message converter for the response messages
                Func<Message, IDictionary<string, object>> messageConverter = message =>
                {
                    // Debug logging for message type
                    string messageType = message.Data?.Type.ToString() ?? "Unknown";
                    bool hasCastAddBody = message.Data?.CastAddBody != null;
                    bool hasCastRemoveBody = message.Data?.CastRemoveBody != null;
                    bool hasReactionBody = message.Data?.ReactionBody != null;
                    bool hasLinkBody = message.Data?.LinkBody != null;
                    bool hasUserDataBody = message.Data?.UserDataBody != null;
                    
                    // Critical debugging for MESSAGE_TYPE_CAST_ADD messages
                    if (message.Data?.Type == MessageType.CastAdd)
                    {
                        logger.LogDebug(
                            "CAST_ADD Message: Hash={Hash}, HasCastAddBody={HasCastAddBody}, DataBytes={DataBytesLength}", 
                            Convert.ToBase64String(message.Hash.ToByteArray()),
                            hasCastAddBody,
                            message.DataBytes?.Length ?? 0);
                            
                        // Analyze the data bytes if available
                        if (message.DataBytes != null && message.DataBytes.Length > 0)
                        {
                            logger.LogDebug("DataBytes found for CAST_ADD message, might need to parse manually");
                        }
                    }
                    else if (message.Data?.Type == MessageType.ReactionAdd || message.Data?.Type == MessageType.ReactionRemove)
                    {
                        logger.LogDebug(
                            "REACTION Message: Hash={Hash}, HasReactionBody={HasReactionBody}, Type={Type}", 
                            Convert.ToBase64String(message.Hash.ToByteArray()),
                            hasReactionBody,
                            message.Data.Type);
                    }
                    else if (message.Data?.Type == MessageType.LinkAdd || message.Data?.Type == MessageType.LinkRemove)
                    {
                        logger.LogDebug(
                            "LINK Message: Hash={Hash}, HasLinkBody={HasLinkBody}, Type={Type}", 
                            Convert.ToBase64String(message.Hash.ToByteArray()),
                            hasLinkBody,
                            message.Data.Type);
                    }
                    else if (message.Data?.Type == MessageType.UserDataAdd)
                    {
                        logger.LogDebug(
                            "USER_DATA Message: Hash={Hash}, HasUserDataBody={HasUserDataBody}, Type={Type}", 
                            Convert.ToBase64String(message.Hash.ToByteArray()),
                            hasUserDataBody,
                            message.Data.Type);
                    }
                    
                    var dict = new Dictionary<string, object>
                    {
                        ["Fid"] = message.Data?.Fid ?? 0,
                        ["MessageType"] = messageType,
                        ["Timestamp"] = message.Data?.Timestamp ?? 0,
                        ["Hash"] = Convert.ToBase64String(message.Hash.ToByteArray()),
                        ["SignatureScheme"] = message.SignatureScheme.ToString(),
                        ["Signature"] = Convert.ToBase64String(message.Signature.ToByteArray()),
                        ["Signer"] = Convert.ToBase64String(message.Signer.ToByteArray())
                    };
                    
                    // Initialize all fields to empty strings to ensure consistent schema across all records
                    if (isCastMessages)
                    {
                        // CastAdd specific fields
                        dict["Text"] = "";
                        dict["Mentions"] = "";
                        dict["ParentCastId"] = "";
                        dict["ParentUrl"] = "";
                        dict["Embeds"] = "";
                        
                        // CastRemove specific fields
                        dict["TargetHash"] = "";
                        
                        // Add CastAddBody specific properties if available
                        if (message.Data?.CastAddBody != null)
                        {
                            logger.LogDebug("CastAddBody details: Text length={TextLength}, Mentions={MentionsCount}, ParentUrl={HasParentUrl}",
                                message.Data.CastAddBody.Text?.Length ?? 0,
                                message.Data.CastAddBody.Mentions?.Count ?? 0,
                                !string.IsNullOrEmpty(message.Data.CastAddBody.ParentUrl));
                                
                            dict["Text"] = message.Data.CastAddBody.Text;
                            dict["Mentions"] = string.Join(",", message.Data.CastAddBody.Mentions);
                            dict["ParentCastId"] = message.Data.CastAddBody.ParentCastId != null ? 
                                $"{message.Data.CastAddBody.ParentCastId.Fid}:{Convert.ToBase64String(message.Data.CastAddBody.ParentCastId.Hash.ToByteArray())}" : "";
                            dict["ParentUrl"] = message.Data.CastAddBody.ParentUrl;
                            dict["Embeds"] = message.Data.CastAddBody.Embeds.Count > 0 ? 
                                string.Join("|", message.Data.CastAddBody.Embeds) : "";
                        }
                        
                        // Add CastRemoveBody specific properties if available
                        if (message.Data?.CastRemoveBody != null)
                        {
                            dict["TargetHash"] = message.Data.CastRemoveBody.TargetHash != null ? 
                                Convert.ToBase64String(message.Data.CastRemoveBody.TargetHash.ToByteArray()) : "";
                        }
                    }
                    else if (isReactionMessages)
                    {
                        // Reaction specific fields
                        dict["ReactionType"] = "";
                        dict["TargetCastId"] = "";
                        dict["TargetUrl"] = "";
                        
                        // Add ReactionBody specific properties if available
                        if (message.Data?.ReactionBody != null)
                        {
                            dict["ReactionType"] = message.Data.ReactionBody.Type.ToString();
                            dict["TargetCastId"] = message.Data.ReactionBody.TargetCastId != null ? 
                                $"{message.Data.ReactionBody.TargetCastId.Fid}:{Convert.ToBase64String(message.Data.ReactionBody.TargetCastId.Hash.ToByteArray())}" : "";
                            dict["TargetUrl"] = message.Data.ReactionBody.TargetUrl;
                        }
                    }
                    else if (isLinkMessages)
                    {
                        // Link specific fields
                        dict["LinkType"] = "";
                        dict["TargetFid"] = "";
                        dict["DisplayTimestamp"] = "";
                        
                        // Add LinkBody specific properties if available
                        if (message.Data?.LinkBody != null)
                        {
                            dict["LinkType"] = message.Data.LinkBody.Type;
                            dict["TargetFid"] = message.Data.LinkBody.TargetFid.ToString();
                            dict["DisplayTimestamp"] = message.Data.LinkBody.DisplayTimestamp.ToString();
                        }
                    }
                    else if (isProfileMessages)
                    {
                        // UserData specific fields
                        dict["UserDataType"] = "";
                        dict["Value"] = "";
                        
                        // Add UserDataBody specific properties if available
                        if (message.Data?.UserDataBody != null)
                        {
                            dict["UserDataType"] = message.Data.UserDataBody.Type.ToString();
                            dict["Value"] = message.Data.UserDataBody.Value;
                            
                            // Add human-readable user data type
                            string userDataTypeStr = "";
                            switch (message.Data.UserDataBody.Type)
                            {
                                case UserDataType.Pfp:
                                    userDataTypeStr = "Profile Picture";
                                    break;
                                case UserDataType.Display:
                                    userDataTypeStr = "Display Name";
                                    break;
                                case UserDataType.Bio:
                                    userDataTypeStr = "Bio";
                                    break;
                                case UserDataType.Url:
                                    userDataTypeStr = "URL";
                                    break;
                                default:
                                    userDataTypeStr = "Unknown";
                                    break;
                            }
                            dict["UserDataTypeReadable"] = userDataTypeStr;
                        }
                    }
                    
                    return dict;
                };
                
                // Create a high-throughput storage solution for the massive dataset
                var storage = storageFactory.CreateHighThroughputStorage<Message>(
                    $"{messageType.ToLower()}_messages",
                    messageConverter);
                
                // Define FID range to process (modified by --mine flag)
                const uint progressInterval = 1000; // Report progress every 1000 FIDs
                
                logger.LogInformation($"Beginning to scan FIDs from {startFid} down to {endFid}");
                logger.LogInformation($"All data will be saved to 'output/{messageType.ToLower()}/{messageType.ToLower()}_messages'");
                
                uint currentFid = startFid;
                
                // Process each FID in the range
                while (currentFid >= endFid)
                {
                    var fidStopwatch = new Stopwatch();
                    fidStopwatch.Start();
                    
                    long messagesForCurrentFid = 0;
                    
                    try
                    {
                        // Set up pagination parameters for this FID
                        byte[] pageToken = null;
                        uint pageSize = 100;
                        bool hasMoreMessages = true;
                        int pageCount = 0;
                        
                        // Paginate through all messages for this FID
                        while (hasMoreMessages)
                        {
                            pageCount++;
                            
                            // Create the request with FID and pagination parameters
                            var request = new FidTimestampRequest { 
                                Fid = currentFid,
                                PageSize = pageSize
                            };
                            
                            // Add page token if not the first page
                            if (pageToken != null)
                            {
                                request.PageToken = ByteString.CopyFrom(pageToken);
                            }
                            
                            // Use the strongly typed client
                            var hubServiceClient = client.CreateClient<HubService.HubServiceClient>();
                            
                            // Call the API with proper error handling
                            MessagesResponse response;
                            try
                            {
                                response = await hubServiceClient.CallAsync(
                                    async (client, ct) => {
                                        if (isCastMessages) {
                                            return await client.GetAllCastMessagesByFidAsync(request, cancellationToken: ct).ResponseAsync;
                                        }
                                        else if (isReactionMessages) {
                                            return await client.GetAllReactionMessagesByFidAsync(request, cancellationToken: ct).ResponseAsync;
                                        }
                                        else if (isLinkMessages) {
                                            return await client.GetAllLinkMessagesByFidAsync(request, cancellationToken: ct).ResponseAsync;
                                        }
                                        else { // isProfileMessages
                                            return await client.GetAllUserDataMessagesByFidAsync(request, cancellationToken: ct).ResponseAsync;
                                        }
                                    },
                                    $"GetAll{messageTypeDisplay}MessagesByFid-{currentFid}-Page{pageCount}");
                            }
                            catch (Exception ex)
                            {
                                logger.LogDebug($"Error retrieving FID {currentFid} page {pageCount}: {ex.Message}");
                                break;
                            }
                            
                            int messageCount = response.Messages.Count;
                            messagesForCurrentFid += messageCount;
                            
                            // Write messages to storage
                            foreach (var message in response.Messages)
                            {
                                // Skip messages older than the cutoffTimestamp, if specified
                                if (cutoffTimestamp.HasValue && message.Data != null && message.Data.Timestamp < cutoffTimestamp.Value)
                                {
                                    continue; 
                                }
                                await storage.AddAsync(message);
                            }

                            // Pagination optimization: Stop if a message on this page is older than the cutoff
                            if (hasMoreMessages && cutoffTimestamp.HasValue) // Check hasMoreMessages to avoid re-evaluating if already false
                            {
                                foreach (var message in response.Messages)
                                {
                                    if (message.Data != null && message.Data.Timestamp < cutoffTimestamp.Value)
                                    {
                                        logger.LogInformation($"Stopping pagination for FID {currentFid} on page {pageCount}, message timestamp {message.Data.Timestamp} is older than cutoff {cutoffTimestamp.Value}.");
                                        hasMoreMessages = false;
                                        break; 
                                    }
                                }
                            }
                            
                            // Check if there are more messages (original logic for advancing pageToken)
                            if (hasMoreMessages && response.NextPageToken != null && response.NextPageToken.Length > 0)
                            {
                                pageToken = response.NextPageToken.ToByteArray();
                            }
                            else
                            {
                                hasMoreMessages = false; // Ensure it's set if next page token is null/empty or optimization above set it
                            }
                            
                            // If we didn't get any messages on the first page, no need to continue
                            if (pageCount == 1 && messageCount == 0)
                            {
                                break;
                            }
                        }
                        
                        // Update statistics for successful FID processing
                        if (messagesForCurrentFid > 0)
                        {
                            totalSuccessfulFids++;
                            totalMessagesRetrieved += messagesForCurrentFid;
                            
                            fidStopwatch.Stop();
                            logger.LogInformation($"FID {currentFid}: Retrieved {messagesForCurrentFid} {messageTypeDisplay} messages in {fidStopwatch.ElapsedMilliseconds}ms");
                        }
                    }
                    catch (Exception ex)
                    {
                        totalFailedFids++;
                        logger.LogWarning($"Failed to process FID {currentFid}: {ex.Message}");
                    }
                    
                    // Move to the next FID
                    currentFid--;
                    
                    // Report progress periodically
                    if ((currentFid % progressInterval == 0 || currentFid == endFid) && !isSingleFid)
                    {
                        // Calculate progress percentage
                        double progressPercent = 100.0 * (startFid - currentFid) / (startFid - endFid);
                        
                        // Calculate estimated time remaining
                        TimeSpan elapsed = totalStopwatch.Elapsed;
                        double processingRate = (startFid - currentFid) / elapsed.TotalSeconds;
                        TimeSpan estimatedRemaining = TimeSpan.FromSeconds((currentFid - endFid) / processingRate);
                        
                        logger.LogInformation(
                            $"Progress: {progressPercent:F2}% complete | " +
                            $"Current: FID {currentFid} | " +
                            $"Stats: {totalSuccessfulFids} active FIDs, {totalMessagesRetrieved} total {messageTypeDisplay} messages | " +
                            $"Time: {elapsed.TotalMinutes:F1} minutes elapsed, ~{estimatedRemaining.TotalMinutes:F1} minutes remaining");
                        
                        // Flush storage periodically to ensure data is written
                        await storage.FlushAsync();
                    }
                }
                
                // Make sure to flush and close the storage
                await storage.FlushAsync();
                
                // Stop timing
                totalStopwatch.Stop();
                
                logger.LogInformation($"Crawl complete! Processed {startFid - endFid + 1} FIDs");
                logger.LogInformation($"Found {totalSuccessfulFids} active FIDs with a total of {totalMessagesRetrieved} {messageTypeDisplay} messages");
                logger.LogInformation($"Failed to process {totalFailedFids} FIDs due to errors");
                logger.LogInformation($"Total execution time: {totalStopwatch.Elapsed.TotalHours:F1} hours ({totalStopwatch.Elapsed.TotalMinutes:F1} minutes)");
                logger.LogInformation($"Data has been saved to the 'output/{messageType.ToLower()}/{messageType.ToLower()}_messages' directory");
            }
            catch (Exception ex)
            {
                // Stop timing if exception occurs
                if (totalStopwatch.IsRunning)
                {
                    totalStopwatch.Stop();
                }
                
                logger.LogError(ex, "A critical error occurred during the FID crawling process");
                logger.LogInformation($"Total execution time before error: {totalStopwatch.Elapsed.TotalHours:F1} hours ({totalStopwatch.Elapsed.TotalMinutes:F1} minutes)");
                logger.LogInformation($"Successfully processed {totalSuccessfulFids} FIDs with {totalMessagesRetrieved} total {messageTypeDisplay} messages before failure");
            }
        }
    }
} 
