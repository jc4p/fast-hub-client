using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using RealtimeListener.Production;
using RealtimeListener.Production.Models;

namespace RealtimeListener.Production.Serialization
{
    public static class MessageNormalizer
    {
        public static NormalizedCast? NormalizeMessage(FilteredHubEvent hubEvent)
        {
            if (hubEvent?.Message?.Data == null)
                return null;

            var message = hubEvent.Message;
            var messageData = message.Data;
            
            var normalized = new NormalizedCast
            {
                Fid = messageData.Fid,
                Hash = BytesToHex(message.Hash),
                Timestamp = messageData.Timestamp,
                EventId = hubEvent.EventId,
                MessageType = hubEvent.MessageType == MessageType.CastAdd ? "cast_add" : "cast_remove"
            };

            switch (hubEvent.MessageType)
            {
                case MessageType.CastAdd:
                    if (messageData.CastAddBody != null)
                    {
                        var castAdd = messageData.CastAddBody;
                        normalized.Text = castAdd.Text;
                        
                        if (castAdd.Mentions.Count > 0)
                        {
                            normalized.Mentions = castAdd.Mentions.ToList();
                        }
                        
                        if (castAdd.MentionsPositions.Count > 0)
                        {
                            normalized.MentionsPositions = castAdd.MentionsPositions.ToList();
                        }
                        
                        if (castAdd.ParentCastId != null)
                        {
                            normalized.ParentHash = BytesToHex(castAdd.ParentCastId.Hash);
                        }
                        else if (!string.IsNullOrEmpty(castAdd.ParentUrl))
                        {
                            normalized.ParentUrl = castAdd.ParentUrl;
                        }
                        
                        if (castAdd.Embeds.Count > 0)
                        {
                            normalized.Embeds = castAdd.Embeds.Select(e => new NormalizedEmbed
                            {
                                Url = e.Url,
                                CastId = e.CastId != null ? new NormalizedCastId
                                {
                                    Fid = e.CastId.Fid,
                                    Hash = BytesToHex(e.CastId.Hash)
                                } : null
                            }).ToList();
                        }
                    }
                    break;
                    
                case MessageType.CastRemove:
                    if (messageData.CastRemoveBody != null)
                    {
                        normalized.TargetHash = BytesToHex(messageData.CastRemoveBody.TargetHash);
                    }
                    break;
            }
            
            return normalized;
        }
        
        private static string BytesToHex(ByteString bytes)
        {
            if (bytes == null || bytes.IsEmpty)
                return string.Empty;
                
            return BitConverter.ToString(bytes.ToByteArray()).Replace("-", "").ToLowerInvariant();
        }
    }
}