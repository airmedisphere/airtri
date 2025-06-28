import math, mimetypes
from fastapi.responses import StreamingResponse, Response
from utils.logger import Logger
from utils.streamer.custom_dl import ByteStreamer
from utils.streamer.file_properties import get_name
from utils.clients import (
    get_client,
)
from urllib.parse import quote
import asyncio
import time

logger = Logger(__name__)

class_cache = {}

# Fast streaming configuration - no transcoding delays
FAST_STREAMING_CONFIG = {
    'chunk_sizes': {
        '240p': 32 * 1024,      # 32KB - very fast for slow connections
        '360p': 64 * 1024,      # 64KB - good for 3G
        '480p': 128 * 1024,     # 128KB - standard for 4G
        '720p': 256 * 1024,     # 256KB - good for WiFi
        '1080p': 512 * 1024,    # 512KB - fast WiFi/ethernet
        'original': 256 * 1024   # 256KB default
    },
    'buffer_sizes': {
        '240p': 2,   # 2 second buffer - instant start
        '360p': 3,   # 3 second buffer
        '480p': 4,   # 4 second buffer
        '720p': 5,   # 5 second buffer
        '1080p': 6,  # 6 second buffer
        'original': 4
    }
}

async def media_streamer(channel: int, message_id: int, file_name: str, request):
    global class_cache

    range_header = request.headers.get("Range", 0)
    quality = request.query_params.get("quality", "auto")
    
    faster_client = get_client()

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
    else:
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect

    file_id = await tg_connect.get_file_properties(channel, message_id)
    file_size = file_id.file_size

    # Auto-detect optimal quality based on connection
    if quality == "auto":
        quality = detect_optimal_quality(request)
    
    # Use fast direct streaming - no transcoding
    return await stream_with_adaptive_chunks(tg_connect, file_id, file_name, request, range_header, file_size, quality)

async def stream_with_adaptive_chunks(tg_connect, file_id, file_name, request, range_header, file_size, quality):
    """Fast adaptive streaming with optimized chunk sizes"""
    
    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = 0
        until_bytes = file_size - 1

    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return Response(
            status_code=416,
            content="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    # Get optimized chunk size for the quality
    chunk_size = get_optimized_chunk_size(quality, request)
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    
    # Use optimized streaming with fast delivery
    body = tg_connect.yield_file_fast(
        file_id, offset, first_part_cut, last_part_cut, part_count, chunk_size, quality
    )

    disposition = "attachment"
    mime_type = mimetypes.guess_type(file_name.lower())[0] or "application/octet-stream"

    if (
        "video/" in mime_type
        or "audio/" in mime_type
        or "image/" in mime_type
        or "/html" in mime_type
    ):
        disposition = "inline"

    # Optimized headers for fast streaming
    response_headers = {
        "Content-Type": f"{mime_type}",
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{quote(file_name)}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=31536000, immutable",  # Long cache for speed
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # Disable proxy buffering
        "X-Content-Type-Options": "nosniff",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Range",
    }

    return StreamingResponse(
        status_code=206 if range_header else 200,
        content=body,
        headers=response_headers,
        media_type=mime_type,
    )

def detect_optimal_quality(request):
    """Fast quality detection based on connection hints"""
    
    # Check for connection speed hints from headers
    connection_type = request.headers.get("Connection-Type", "").lower()
    save_data = request.headers.get("Save-Data", "").lower() == "on"
    user_agent = request.headers.get("User-Agent", "").lower()
    
    # Detect mobile devices
    is_mobile = any(mobile in user_agent for mobile in ['mobile', 'android', 'iphone', 'ipad'])
    
    # Fast quality selection - prioritize speed over quality
    if save_data or "slow" in connection_type:
        return "240p"  # Fastest for slow connections
    elif "2g" in connection_type or is_mobile:
        return "360p"  # Good for mobile
    elif "3g" in connection_type:
        return "480p"  # Standard for 3G
    else:
        return "720p"  # Default for good connections

def get_optimized_chunk_size(quality, request):
    """Get optimized chunk size for fast streaming"""
    
    # Base chunk sizes optimized for speed
    base_size = FAST_STREAMING_CONFIG['chunk_sizes'].get(quality, 128 * 1024)
    
    # Check for connection hints
    connection_type = request.headers.get("Connection-Type", "").lower()
    save_data = request.headers.get("Save-Data", "").lower() == "on"
    user_agent = request.headers.get("User-Agent", "").lower()
    
    # Detect mobile devices
    is_mobile = any(mobile in user_agent for mobile in ['mobile', 'android', 'iphone', 'ipad'])
    
    # Optimize chunk size for connection
    if save_data or "slow" in connection_type:
        return max(16 * 1024, base_size // 4)  # Very small chunks for slow connections
    elif "2g" in connection_type or is_mobile:
        return max(32 * 1024, base_size // 2)  # Small chunks for mobile
    elif "fast" in connection_type or "wifi" in connection_type:
        return min(1024 * 1024, base_size * 2)  # Larger chunks for fast connections
    else:
        return base_size

def is_video_file(filename):
    """Check if file is a video"""
    video_extensions = {'.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', 
                       '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg'}
    extension = os.path.splitext(filename.lower())[1]
    return extension in video_extensions