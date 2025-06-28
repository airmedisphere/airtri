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
import subprocess
import json
import os
from pathlib import Path

logger = Logger(__name__)

class_cache = {}

# Quality presets (YouTube-style)
QUALITY_PRESETS = {
    '240p': {
        'height': 240,
        'bitrate': '400k',
        'audio_bitrate': '64k',
        'fps': 24,
        'profile': 'baseline',
        'level': '3.0'
    },
    '360p': {
        'height': 360,
        'bitrate': '800k',
        'audio_bitrate': '96k',
        'fps': 30,
        'profile': 'baseline',
        'level': '3.1'
    },
    '480p': {
        'height': 480,
        'bitrate': '1200k',
        'audio_bitrate': '128k',
        'fps': 30,
        'profile': 'main',
        'level': '3.1'
    },
    '720p': {
        'height': 720,
        'bitrate': '2500k',
        'audio_bitrate': '192k',
        'fps': 30,
        'profile': 'high',
        'level': '4.0'
    },
    '1080p': {
        'height': 1080,
        'bitrate': '5000k',
        'audio_bitrate': '256k',
        'fps': 30,
        'profile': 'high',
        'level': '4.2'
    }
}

# Cache for transcoded segments
transcode_cache = {}
active_transcodes = {}

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

    # Check if this is a video file that supports quality selection
    if is_video_file(file_name) and quality != "original":
        return await stream_adaptive_quality(tg_connect, file_id, file_name, request, quality)
    
    # Original streaming logic for non-video files or original quality
    return await stream_original_quality(tg_connect, file_id, file_name, request, range_header, file_size)

async def stream_adaptive_quality(tg_connect, file_id, file_name, request, quality):
    """Stream video with adaptive quality using real-time transcoding"""
    
    range_header = request.headers.get("Range", 0)
    
    # Auto-detect quality based on connection if needed
    if quality == "auto":
        quality = detect_optimal_quality(request)
    
    # Validate quality
    if quality not in QUALITY_PRESETS:
        quality = "720p"  # Default fallback
    
    logger.info(f"Streaming {file_name} in {quality} quality")
    
    # Get quality preset
    preset = QUALITY_PRESETS[quality]
    
    # Create cache key
    cache_key = f"{file_id.media_id}_{quality}"
    
    # Check if we have this quality cached or being transcoded
    if cache_key in transcode_cache:
        return await stream_cached_quality(transcode_cache[cache_key], request, range_header)
    
    # Start transcoding if not already in progress
    if cache_key not in active_transcodes:
        active_transcodes[cache_key] = asyncio.create_task(
            transcode_video_stream(tg_connect, file_id, quality, preset, cache_key)
        )
    
    # Wait for initial segments to be ready
    await asyncio.sleep(2)  # Give transcoding a head start
    
    # Stream the transcoded content
    return await stream_transcoded_segments(cache_key, request, range_header, quality)

async def transcode_video_stream(tg_connect, file_id, quality, preset, cache_key):
    """Transcode video stream in real-time using FFmpeg"""
    
    try:
        logger.info(f"Starting transcoding for quality: {quality}")
        
        # Create output directory
        output_dir = Path(f"./cache/transcode/{cache_key}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Download source video in chunks for transcoding
        source_file = output_dir / "source.mp4"
        
        # Download the original file
        await download_for_transcoding(tg_connect, file_id, source_file)
        
        # Start FFmpeg transcoding
        await start_ffmpeg_transcoding(source_file, output_dir, preset, quality)
        
        # Cache the result
        transcode_cache[cache_key] = {
            'output_dir': output_dir,
            'quality': quality,
            'ready': True,
            'segments': []
        }
        
        logger.info(f"Transcoding completed for quality: {quality}")
        
    except Exception as e:
        logger.error(f"Transcoding failed for {quality}: {e}")
        # Remove from active transcodes on failure
        if cache_key in active_transcodes:
            del active_transcodes[cache_key]

async def download_for_transcoding(tg_connect, file_id, output_file):
    """Download the original file for transcoding"""
    
    try:
        # Get file size
        file_size = file_id.file_size
        chunk_size = 1024 * 1024  # 1MB chunks
        
        with open(output_file, 'wb') as f:
            offset = 0
            while offset < file_size:
                # Calculate chunk size for this iteration
                current_chunk_size = min(chunk_size, file_size - offset)
                
                # Download chunk
                async for chunk in tg_connect.yield_file(
                    file_id, offset, 0, current_chunk_size, 1, current_chunk_size
                ):
                    f.write(chunk)
                
                offset += current_chunk_size
                
                # Log progress
                progress = (offset / file_size) * 100
                if progress % 10 < 1:  # Log every 10%
                    logger.info(f"Download progress: {progress:.1f}%")
        
        logger.info(f"Download completed: {output_file}")
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

async def start_ffmpeg_transcoding(source_file, output_dir, preset, quality):
    """Start FFmpeg transcoding with optimized settings"""
    
    try:
        # Output file
        output_file = output_dir / f"output_{quality}.mp4"
        
        # Build FFmpeg command with YouTube-style optimization
        cmd = [
            'ffmpeg',
            '-i', str(source_file),
            '-c:v', 'libx264',
            '-preset', 'veryfast',  # Fast encoding
            '-profile:v', preset['profile'],
            '-level:v', preset['level'],
            '-vf', f"scale=-2:{preset['height']}",  # Maintain aspect ratio
            '-b:v', preset['bitrate'],
            '-maxrate', preset['bitrate'],
            '-bufsize', str(int(preset['bitrate'].replace('k', '')) * 2) + 'k',
            '-r', str(preset['fps']),
            '-c:a', 'aac',
            '-b:a', preset['audio_bitrate'],
            '-ac', '2',  # Stereo audio
            '-movflags', '+faststart',  # Enable fast start for web
            '-f', 'mp4',
            '-y',  # Overwrite output
            str(output_file)
        ]
        
        logger.info(f"Starting FFmpeg: {' '.join(cmd)}")
        
        # Run FFmpeg
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            logger.info(f"FFmpeg transcoding completed successfully for {quality}")
        else:
            logger.error(f"FFmpeg failed: {stderr.decode()}")
            raise Exception(f"FFmpeg transcoding failed: {stderr.decode()}")
            
    except Exception as e:
        logger.error(f"FFmpeg transcoding error: {e}")
        raise

async def stream_transcoded_segments(cache_key, request, range_header, quality):
    """Stream the transcoded video segments"""
    
    try:
        # Wait for transcoding to complete
        max_wait = 60  # Maximum wait time in seconds
        wait_time = 0
        
        while cache_key not in transcode_cache and wait_time < max_wait:
            await asyncio.sleep(1)
            wait_time += 1
        
        if cache_key not in transcode_cache:
            # Fallback to original quality if transcoding takes too long
            logger.warning(f"Transcoding timeout for {quality}, falling back to original")
            return Response(
                status_code=302,
                headers={"Location": f"?quality=original"}
            )
        
        cache_info = transcode_cache[cache_key]
        output_dir = cache_info['output_dir']
        output_file = output_dir / f"output_{quality}.mp4"
        
        if not output_file.exists():
            raise Exception(f"Transcoded file not found: {output_file}")
        
        # Stream the transcoded file
        return await stream_file_with_range(output_file, request, range_header)
        
    except Exception as e:
        logger.error(f"Streaming transcoded segments failed: {e}")
        # Fallback to original quality
        return Response(
            status_code=302,
            headers={"Location": f"?quality=original"}
        )

async def stream_file_with_range(file_path, request, range_header):
    """Stream a local file with range support"""
    
    file_size = file_path.stat().st_size
    
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

    chunk_size = 1024 * 1024  # 1MB chunks
    req_length = until_bytes - from_bytes + 1

    async def file_streamer():
        with open(file_path, 'rb') as f:
            f.seek(from_bytes)
            remaining = req_length
            
            while remaining > 0:
                chunk_size_to_read = min(chunk_size, remaining)
                chunk = f.read(chunk_size_to_read)
                if not chunk:
                    break
                yield chunk
                remaining -= len(chunk)

    mime_type = "video/mp4"
    disposition = "inline"

    # Optimized headers for video streaming
    response_headers = {
        "Content-Type": mime_type,
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{quote(file_path.name)}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
        "Connection": "keep-alive",
    }

    return StreamingResponse(
        status_code=206 if range_header else 200,
        content=file_streamer(),
        headers=response_headers,
        media_type=mime_type,
    )

async def stream_original_quality(tg_connect, file_id, file_name, request, range_header, file_size):
    """Stream original quality (existing logic)"""
    
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
    chunk_size = get_adaptive_chunk_size(request, "original")
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
        "ETag": f'"{file_id.media_id}-{file_size}-original"',
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

def detect_optimal_quality(request):
    """Detect optimal quality based on connection and device"""
    
    # Check for connection speed hints from headers
    connection_type = request.headers.get("Connection-Type", "").lower()
    save_data = request.headers.get("Save-Data", "").lower() == "on"
    user_agent = request.headers.get("User-Agent", "").lower()
    
    # Detect mobile devices
    is_mobile = any(mobile in user_agent for mobile in ['mobile', 'android', 'iphone', 'ipad'])
    
    # Auto-select quality based on conditions
    if save_data or "slow" in connection_type:
        return "240p"
    elif is_mobile:
        return "480p"
    elif "fast" in connection_type:
        return "1080p"
    else:
        return "720p"  # Default for desktop

def get_adaptive_chunk_size(request, quality):
    """Determine optimal chunk size based on quality and connection"""
    
    # Base chunk sizes for different qualities
    chunk_sizes = {
        "240p": 64 * 1024,      # 64KB for low quality
        "360p": 128 * 1024,     # 128KB for medium-low quality
        "480p": 256 * 1024,     # 256KB for medium quality
        "720p": 512 * 1024,     # 512KB for high quality
        "1080p": 1024 * 1024,   # 1MB for very high quality
        "original": 256 * 1024   # 256KB for original
    }
    
    # Check for connection speed hints from headers
    connection_type = request.headers.get("Connection-Type", "").lower()
    save_data = request.headers.get("Save-Data", "").lower() == "on"
    user_agent = request.headers.get("User-Agent", "").lower()
    
    # Detect mobile devices and adjust accordingly
    is_mobile = any(mobile in user_agent for mobile in ['mobile', 'android', 'iphone', 'ipad'])
    
    base_size = chunk_sizes.get(quality, chunk_sizes["720p"])
    
    # Adjust based on connection
    if save_data or "slow" in connection_type or is_mobile:
        return max(32 * 1024, base_size // 2)  # Reduce chunk size
    elif "fast" in connection_type:
        return min(2 * 1024 * 1024, base_size * 2)  # Increase chunk size
    else:
        return base_size

def is_video_file(filename):
    """Check if file is a video"""
    video_extensions = {'.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', 
                       '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg'}
    extension = os.path.splitext(filename.lower())[1]
    return extension in video_extensions

# Cleanup function for cache management
async def cleanup_transcode_cache():
    """Clean up old transcoded files"""
    try:
        cache_dir = Path("./cache/transcode")
        if cache_dir.exists():
            # Remove files older than 1 hour
            import time
            current_time = time.time()
            
            for item in cache_dir.iterdir():
                if item.is_dir():
                    # Check if directory is old
                    if current_time - item.stat().st_mtime > 3600:  # 1 hour
                        import shutil
                        shutil.rmtree(item)
                        logger.info(f"Cleaned up old transcode cache: {item}")
    except Exception as e:
        logger.error(f"Cache cleanup error: {e}")

# Start cleanup task
import asyncio
asyncio.create_task(cleanup_transcode_cache())