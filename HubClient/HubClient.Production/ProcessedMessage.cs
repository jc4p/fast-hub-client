using System;
using HubClient.Core.Grpc;

namespace HubClient.Production
{
    /// <summary>
    /// Represents a message that has been processed through the pipeline
    /// </summary>
    public class ProcessedMessage
    {
        /// <summary>
        /// The original message that was processed
        /// </summary>
        public Message? OriginalMessage { get; set; }
        
        /// <summary>
        /// The processed data (serialized form of the message)
        /// </summary>
        public byte[]? ProcessedData { get; set; }
        
        /// <summary>
        /// Whether the processing was successful
        /// </summary>
        public bool IsSuccess { get; set; }
        
        /// <summary>
        /// Error message if the processing failed
        /// </summary>
        public string? Error { get; set; }
        
        /// <summary>
        /// Size of the serialized data in bytes
        /// </summary>
        public int SerializedSize { get; set; }
        
        /// <summary>
        /// Timestamp when the message was processed
        /// </summary>
        public DateTimeOffset ProcessedAt { get; set; } = DateTimeOffset.UtcNow;
    }
} 