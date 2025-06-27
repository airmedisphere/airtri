from utils.clients import get_client
from pyrogram import Client
from pyrogram.types import Message
from config import STORAGE_CHANNEL
import os
from utils.logger import Logger
from urllib.parse import unquote_plus
import subprocess
import json
import asyncio
import time

logger = Logger(__name__)
PROGRESS_CACHE = {}
STOP_TRANSMISSION = []

# Add rate limiting and retry mechanism
UPLOAD_SEMAPHORE = asyncio.Semaphore(2)  # Limit concurrent uploads
RATE_LIMIT_DELAY = 1.0  # Delay between uploads in seconds


async def progress_callback(current, total, id, client: Client, file_path):
    global PROGRESS_CACHE, STOP_TRANSMISSION

    PROGRESS_CACHE[id] = ("running", current, total)
    if id in STOP_TRANSMISSION:
        logger.info(f"Stopping transmission {id}")
        client.stop_transmission()
        try:
            os.remove(file_path)
        except:
            pass


def get_video_duration(file_path):
    """Extract video duration using ffprobe"""
    try:
        cmd = [
            'ffprobe', 
            '-v', 'quiet', 
            '-print_format', 'json', 
            '-show_format', 
            str(file_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            duration = float(data.get('format', {}).get('duration', 0))
            return int(duration)
        else:
            logger.warning(f"ffprobe failed for {file_path}: {result.stderr}")
            return 0
            
    except subprocess.TimeoutExpired:
        logger.warning(f"ffprobe timeout for {file_path}")
        return 0
    except (subprocess.SubprocessError, json.JSONDecodeError, ValueError, KeyError) as e:
        logger.warning(f"Error getting duration for {file_path}: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error getting duration for {file_path}: {e}")
        return 0


def is_video_file(filename):
    """Check if file is a video based on extension"""
    video_extensions = {'.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', 
                       '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg'}
    extension = os.path.splitext(filename.lower())[1]
    return extension in video_extensions


async def start_file_uploader(
    file_path, id, directory_path, filename, file_size, delete=True, retry_count=0
):
    global PROGRESS_CACHE
    from utils.directoryHandler import DRIVE_DATA

    # Use semaphore to limit concurrent uploads
    async with UPLOAD_SEMAPHORE:
        logger.info(f"Uploading file {file_path} {id} (attempt {retry_count + 1})")

        # Add rate limiting delay
        if retry_count > 0:
            delay = min(RATE_LIMIT_DELAY * (2 ** retry_count), 30)  # Exponential backoff, max 30s
            logger.info(f"Rate limiting: waiting {delay}s before retry")
            await asyncio.sleep(delay)
        elif RATE_LIMIT_DELAY > 0:
            await asyncio.sleep(RATE_LIMIT_DELAY)

        try:
            if file_size > 1.98 * 1024 * 1024 * 1024:
                # Use premium client for files larger than 2 GB
                client: Client = get_client(premium_required=True)
            else:
                client: Client = get_client()

            PROGRESS_CACHE[id] = ("running", 0, file_size)

            # Extract video duration if it's a video file
            duration = 0
            if is_video_file(filename):
                duration = get_video_duration(file_path)
                logger.info(f"Video duration for {filename}: {duration} seconds")

            # Add timeout and retry mechanism
            try:
                message: Message = await asyncio.wait_for(
                    client.send_document(
                        STORAGE_CHANNEL,
                        file_path,
                        progress=progress_callback,
                        progress_args=(id, client, file_path),
                        disable_notification=True,
                    ),
                    timeout=300  # 5 minute timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"Upload timeout for {filename}")
                if retry_count < 3:  # Max 3 retries
                    logger.info(f"Retrying upload for {filename}")
                    return await start_file_uploader(
                        file_path, id, directory_path, filename, file_size, delete, retry_count + 1
                    )
                else:
                    PROGRESS_CACHE[id] = ("error", 0, file_size)
                    raise Exception("Upload failed after multiple retries")

            size = (
                message.photo
                or message.document
                or message.video
                or message.audio
                or message.sticker
            ).file_size

            filename = unquote_plus(filename)

            DRIVE_DATA.new_file(directory_path, filename, message.id, size, duration)
            PROGRESS_CACHE[id] = ("completed", size, size)

            logger.info(f"Uploaded file {file_path} {id}")

            if delete:
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.warning(f"Failed to delete file {file_path}: {e}")

        except Exception as e:
            logger.error(f"Upload failed for {filename}: {e}")
            PROGRESS_CACHE[id] = ("error", 0, file_size)
            
            # Retry logic for network errors
            if retry_count < 3 and ("network" in str(e).lower() or "timeout" in str(e).lower()):
                logger.info(f"Retrying upload for {filename} due to network error")
                return await start_file_uploader(
                    file_path, id, directory_path, filename, file_size, delete, retry_count + 1
                )
            else:
                if delete:
                    try:
                        os.remove(file_path)
                    except:
                        pass
                raise e