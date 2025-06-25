#!/usr/bin/env python3
"""
Simple script to peek at messages in the Redis queue (now JSON format)
Requires: pip install redis
"""

import redis
import sys
import json
from datetime import datetime

def peek_queue(queue_name="farcaster:casts:add", count=5):
    r = redis.Redis(decode_responses=False)
    
    # Get queue length
    queue_len = r.llen(queue_name)
    print(f"Queue '{queue_name}' has {queue_len} items\n")
    
    if queue_len == 0:
        return
    
    # Peek at first few items without removing them
    items = r.lrange(queue_name, 0, min(count-1, queue_len-1))
    
    for i, item in enumerate(items):
        print(f"{'='*60}")
        print(f"Item {i+1}:")
        print(f"  Size: {len(item)} bytes")
        
        try:
            # Parse JSON
            data = json.loads(item.decode('utf-8'))
            
            # Display the parsed data
            print(f"  FID: {data.get('fid')}")
            print(f"  Hash: {data.get('hash')}")
            print(f"  Event ID: {data.get('event_id')}")
            print(f"  Message Type: {data.get('message_type')}")
            print(f"  Timestamp: {data.get('timestamp')}")
            
            # Calculate and display timing information
            if data.get('timestamp') and data.get('processed_at'):
                # Convert Farcaster timestamp to milliseconds (Farcaster epoch is 2021-01-01)
                farcaster_epoch = datetime(2021, 1, 1)
                farcaster_epoch_ms = farcaster_epoch.timestamp() * 1000
                message_timestamp_ms = farcaster_epoch_ms + (data['timestamp'] * 1000)
                message_time = datetime.fromtimestamp(message_timestamp_ms / 1000)
                processed_time = datetime.fromtimestamp(data['processed_at'] / 1000)
                
                # Calculate time from message creation to Redis insertion
                creation_to_redis_ms = data['processed_at'] - message_timestamp_ms
                
                print(f"  📝 Message created at: {message_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
                print(f"  📥 Went into Redis at: {processed_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
                print(f"  ⏱️  Delta (creation → Redis): {creation_to_redis_ms:.0f}ms")
                
                # Show warning if latency is high
                if creation_to_redis_ms > 5000:
                    print(f"  ⚠️  High latency detected!")
                elif creation_to_redis_ms < 1000:
                    print(f"  ✅ Low latency - near real-time!")
                
                # Calculate time since it was put in Redis
                now_ms = datetime.utcnow().timestamp() * 1000
                time_in_queue_ms = now_ms - data['processed_at']
                
                print(f"  📊 Time sitting in Redis: ", end="")
                if time_in_queue_ms < 1000:
                    print(f"{time_in_queue_ms:.0f}ms")
                elif time_in_queue_ms < 60000:
                    print(f"{time_in_queue_ms/1000:.1f}s")
                else:
                    print(f"{time_in_queue_ms/60000:.1f}m")
            
            if data.get('text'):
                text = data['text']
                if len(text) > 100:
                    text = text[:100] + "..."
                print(f"  Text: {text}")
            
            if data.get('parent_hash'):
                print(f"  Parent Hash: {data['parent_hash']}")
            elif data.get('parent_url'):
                print(f"  Parent URL: {data['parent_url']}")
                
            if data.get('embeds'):
                print(f"  Embeds: {len(data['embeds'])} embed(s)")
                for j, embed in enumerate(data['embeds'][:3]):  # Show first 3
                    if embed.get('url'):
                        print(f"    [{j+1}] URL: {embed['url']}")
                    elif embed.get('cast_id'):
                        print(f"    [{j+1}] Cast: fid={embed['cast_id']['fid']}, hash={embed['cast_id']['hash']}")
                        
            if data.get('mentions'):
                print(f"  Mentions: {data['mentions']}")
                
            if data.get('target_hash'):
                print(f"  Target Hash (for removal): {data['target_hash']}")
                
        except json.JSONDecodeError:
            print("  Error: Not valid JSON, showing raw data")
            print(f"  First 100 bytes (hex): {item[:100].hex()}")
        except Exception as e:
            print(f"  Error parsing: {e}")
        
        print()

if __name__ == "__main__":
    queue = sys.argv[1] if len(sys.argv) > 1 else "farcaster:casts:add"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    peek_queue(queue, count)