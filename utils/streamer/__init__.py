import math, mimetypes
from fastapi.responses import StreamingResponse, Response
from utils.logger import Logger
from utils.streamer.custom_dl import ByteStreamer
from utils.streamer.file_properties import get_name
from utils.clients import (
    get_client,
)
from urllib.parse import quote

logger = Logger(__name__)

class_cache = {}


async def media_streamer(channel: int, message_id: int, file_name: str, request):
    global class_cache

    range_header = request.headers.get("Range", 0)

    faster_client = get_client()

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
    else:
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect

    file_id = await tg_connect.get_file_properties(channel, message_id)
    file_size = file_id.file_size

    # Check for quality parameter for adaptive streaming
    quality = request.query_params.get("quality", "medium")
    
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

    # Adaptive chunk size based on connection speed and quality
    chunk_size = get_adaptive_chunk_size(request, quality)
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    
    # Use standard streaming for better compatibility
    body = tg_connect.yield_file(
        file_id, offset, first_part_cut, last_part_cut, part_count, chunk_size
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

    # Add optimized caching headers for video streaming
    cache_headers = {
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
        "ETag": f'"{message_id}-{file_size}-{quality}"',
        "Accept-Ranges": "bytes",
        "Connection": "keep-alive",
    }

    response_headers = {
        "Content-Type": f"{mime_type}",
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{quote(file_name)}"',
        **cache_headers
    }

    return StreamingResponse(
        status_code=206 if range_header else 200,
        content=body,
        headers=response_headers,
        media_type=mime_type,
    )


def get_adaptive_chunk_size(request, quality):
    """Determine optimal chunk size based on quality and connection"""
    # Base chunk sizes for different qualities (optimized for streaming)
    chunk_sizes = {
        "low": 128 * 1024,      # 128KB for slow connections
        "medium": 256 * 1024,   # 256KB for medium connections  
        "high": 512 * 1024,     # 512KB for fast connections
        "auto": 256 * 1024      # Default adaptive size
    }
    
    # Check for connection speed hints from headers
    connection_type = request.headers.get("Connection-Type", "").lower()
    save_data = request.headers.get("Save-Data", "").lower() == "on"
    user_agent = request.headers.get("User-Agent", "").lower()
    
    # Detect mobile devices and adjust accordingly
    is_mobile = any(mobile in user_agent for mobile in ['mobile', 'android', 'iphone', 'ipad'])
    
    if save_data or "slow" in connection_type or is_mobile:
        return chunk_sizes["low"]
    elif quality in chunk_sizes:
        return chunk_sizes[quality]
    else:
        return chunk_sizes["medium"]  # Safe default