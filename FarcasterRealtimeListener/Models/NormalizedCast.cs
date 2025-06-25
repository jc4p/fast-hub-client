using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace FarcasterRealtimeListener.Models
{
    public class NormalizedCast
    {
        [JsonPropertyName("fid")]
        public ulong Fid { get; set; }
        
        [JsonPropertyName("hash")]
        public string Hash { get; set; } = string.Empty;
        
        [JsonPropertyName("timestamp")]
        public uint Timestamp { get; set; }
        
        [JsonPropertyName("text")]
        public string? Text { get; set; }
        
        [JsonPropertyName("embeds")]
        public List<NormalizedEmbed>? Embeds { get; set; }
        
        [JsonPropertyName("mentions")]
        public List<ulong>? Mentions { get; set; }
        
        [JsonPropertyName("mentions_positions")]
        public List<uint>? MentionsPositions { get; set; }
        
        [JsonPropertyName("parent_hash")]
        public string? ParentHash { get; set; }
        
        [JsonPropertyName("parent_url")]
        public string? ParentUrl { get; set; }
        
        [JsonPropertyName("event_id")]
        public ulong EventId { get; set; }
        
        [JsonPropertyName("message_type")]
        public string MessageType { get; set; } = string.Empty;
        
        [JsonPropertyName("target_hash")]
        public string? TargetHash { get; set; }
        
        [JsonPropertyName("processed_at")]
        public long ProcessedAt { get; set; }
    }

    public class NormalizedEmbed
    {
        [JsonPropertyName("url")]
        public string? Url { get; set; }
        
        [JsonPropertyName("cast_id")]
        public NormalizedCastId? CastId { get; set; }
    }

    public class NormalizedCastId
    {
        [JsonPropertyName("fid")]
        public ulong Fid { get; set; }
        
        [JsonPropertyName("hash")]
        public string Hash { get; set; } = string.Empty;
    }
}