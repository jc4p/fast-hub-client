using System;
using System.Collections.Generic;
using Google.Protobuf;
using HubClient.Core.Grpc;

namespace HubClient.Core.Storage
{
    /// <summary>
    /// Provides conversion from Message objects to Parquet-compatible row dictionaries
    /// </summary>
    public static class MessageParquetConverter
    {
        /// <summary>
        /// Converts a Message to a dictionary suitable for Parquet storage
        /// </summary>
        /// <param name="message">The message to convert</param>
        /// <returns>Dictionary representation of the message</returns>
        public static IDictionary<string, object> ConvertMessageToRow(Message message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            
            var row = new Dictionary<string, object>
            {
                // Add primary message fields
                ["hash"] = message.Hash?.ToByteArray() ?? Array.Empty<byte>(),
                ["hash_scheme"] = message.HashScheme.ToString(),
                ["signature"] = message.Signature?.ToByteArray() ?? Array.Empty<byte>(),
                ["signature_scheme"] = message.SignatureScheme.ToString(),
                ["signer"] = message.Signer?.ToByteArray() ?? Array.Empty<byte>()
            };
            
            // Add message data fields if present
            if (message.Data != null)
            {
                row["data_type"] = message.Data.Type.ToString();
                row["data_fid"] = message.Data.Fid;
                row["data_timestamp"] = message.Data.Timestamp;
                row["data_network"] = message.Data.Network.ToString();
                
                // Process the specific body type
                switch (message.Data.BodyCase)
                {
                    case MessageData.BodyOneofCase.CastAddBody:
                        AddCastAddFields(message.Data.CastAddBody, row);
                        break;
                        
                    case MessageData.BodyOneofCase.CastRemoveBody:
                        AddCastRemoveFields(message.Data.CastRemoveBody, row);
                        break;
                        
                    case MessageData.BodyOneofCase.UserDataBody:
                        AddUserDataFields(message.Data.UserDataBody, row);
                        break;
                }
            }
            
            return row;
        }
        
        /// <summary>
        /// Adds CastAdd fields to the row dictionary
        /// </summary>
        private static void AddCastAddFields(CastAddBody castAdd, IDictionary<string, object> row)
        {
            row["body_type"] = "CastAddBody";
            row["cast_text"] = castAdd.Text;
            
            if (castAdd.ParentCastId != null)
            {
                row["parent_cast_fid"] = castAdd.ParentCastId.Fid;
                row["parent_cast_hash"] = castAdd.ParentCastId.Hash?.ToByteArray() ?? Array.Empty<byte>();
            }
            
            if (castAdd.Mentions.Count > 0)
            {
                row["mentions_count"] = castAdd.Mentions.Count;
                // We can't easily store arrays in Parquet, so we'll concatenate the mentions into a string
                row["mentions"] = string.Join(",", castAdd.Mentions);
            }
        }
        
        /// <summary>
        /// Adds CastRemove fields to the row dictionary
        /// </summary>
        private static void AddCastRemoveFields(CastRemoveBody castRemove, IDictionary<string, object> row)
        {
            row["body_type"] = "CastRemoveBody";
            row["target_hash"] = castRemove.TargetHash?.ToByteArray() ?? Array.Empty<byte>();
        }
        
        /// <summary>
        /// Adds UserData fields to the row dictionary
        /// </summary>
        private static void AddUserDataFields(UserDataBody userData, IDictionary<string, object> row)
        {
            row["body_type"] = "UserDataBody";
            row["user_data_type"] = userData.Type.ToString();
            row["user_data_value"] = userData.Value;
        }
        
        /// <summary>
        /// Creates a function to convert messages to rows that can be passed to ParquetWriter
        /// </summary>
        /// <returns>Function converting messages to dictionaries</returns>
        public static Func<Message, IDictionary<string, object>> CreateConverter()
        {
            return ConvertMessageToRow;
        }
    }
} 