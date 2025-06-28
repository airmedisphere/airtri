from utils.clients import get_client, release_client
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

# Enhanced rate limiting and retry mechanism
UPLOAD_SEMAPHORE = asyncio.Semaphore(3)  # Increased to 3 concurrent uploads
RATE_LIMIT_DELAY = 0.5  # Reduced delay between uploads
MAX_RETRIES = 5  # Increased retry attempts

# Upload queue for better management
upload_queue = asyncio.Queue(maxsize=50)
upload_workers_started = False


async def start_upload_workers():
    """Start background workers to process upload queue"""
    global upload_workers_started
    if upload_workers_started:
        return
    
    upload_workers_started = True
    
    # Start multiple workers
    for i in range(3):
        asyncio.create_task(upload_worker(f"worker-{i}"))
    
    logger.info("Started 3 upload workers")


async def upload_worker(worker_name):
    """Background worker to process uploads from queue"""
    logger.info(f"Upload worker {worker_name} started")
    
    while True:
        try:
            # Get upload task from queue
            upload_task = await upload_queue.get()
            
            if upload_task is None:  # Shutdown signal
                break
                
            # Process the upload
            await process_upload_task(upload_task, worker_name)
            
            # Mark task as done
            upload_queue.task_done()
            
        except Exception as e:
            logger.error(f"Upload worker {worker_name} error: {e}")
            await asyncio.sleep(1)


async def process_upload_task(upload_task, worker_name):
    """Process a single upload task"""
    file_path, upload_id, directory_path, filename, file_size, delete, retry_count = upload_task
    
    logger.info(f"Worker {worker_name} processing upload {upload_id} (attempt {retry_count + 1})")
    
    try:
        await _upload_file_internal(
            file_path, upload_id, directory_path, filename, file_size, delete, retry_count
        )
    except Exception as e:
        logger.error(f"Worker {worker_name} failed to process upload {upload_id}: {e}")
        PROGRESS_CACHE[upload_id] = ("error", 0, file_size)


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
    """Extract video duration using ffprobe with improved error handling"""
    try:
        cmd = [
            'ffprobe', 
            '-v', 'quiet', 
            '-print_format', 'json', 
            '-show_format', 
            str(file_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        
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
    """Queue file for upload processing"""
    global upload_queue
    
    # Start workers if not already started
    await start_upload_workers()
    
    # Add to upload queue
    upload_task = (file_path, id, directory_path, filename, file_size, delete, retry_count)
    
    try:
        await upload_queue.put(upload_task)
        logger.info(f"Queued upload {id} for processing")
    except Exception as e:
        logger.error(f"Failed to queue upload {id}: {e}")
        PROGRESS_CACHE[id] = ("error", 0, file_size)


async def _upload_file_internal(
    file_path, id, directory_path, filename, file_size, delete=True, retry_count=0
):
    """Internal upload function with improved error handling and speed optimization"""
    global PROGRESS_CACHE
    from utils.directoryHandler import DRIVE_DATA

    # Use semaphore to limit concurrent uploads
    async with UPLOAD_SEMAPHORE:
        logger.info(f"Uploading file {file_path} {id} (attempt {retry_count + 1})")

        # Dynamic rate limiting based on retry count
        if retry_count > 0:
            delay = min(RATE_LIMIT_DELAY * (1.5 ** retry_count), 10)  # Max 10s delay
            logger.info(f"Rate limiting: waiting {delay:.1f}s before retry")
            await asyncio.sleep(delay)
        elif RATE_LIMIT_DELAY > 0:
            await asyncio.sleep(RATE_LIMIT_DELAY)

        client = None
        try:
            # Smart client selection based on file size
            if file_size > 1.98 * 1024 * 1024 * 1024:
                # Use premium client for files larger than 2 GB
                client = get_client(premium_required=True)
                logger.info(f"Using premium client for large file {filename}")
            else:
                client = get_client()

            PROGRESS_CACHE[id] = ("running", 0, file_size)

            # Extract video duration if it's a video file
            duration = 0
            if is_video_file(filename):
                duration = get_video_duration(file_path)
                if duration > 0:
                    logger.info(f"Video duration for {filename}: {duration} seconds")

            # Optimized upload with dynamic timeout based on file size
            base_timeout = 300  # 5 minutes base
            size_factor = file_size / (100 * 1024 * 1024)  # 100MB chunks
            upload_timeout = min(base_timeout + (size_factor * 60), 1800)  # Max 30 minutes
            
            logger.info(f"Upload timeout set to {upload_timeout:.0f}s for {filename}")

            try:
                message: Message = await asyncio.wait_for(
                    client.send_document(
                        STORAGE_CHANNEL,
                        file_path,
                        progress=progress_callback,
                        progress_args=(id, client, file_path),
                        disable_notification=True,
                    ),
                    timeout=upload_timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"Upload timeout for {filename} after {upload_timeout}s")
                if retry_count < MAX_RETRIES:
                    logger.info(f"Retrying upload for {filename} (attempt {retry_count + 2})")
                    # Re-queue for retry
                    await upload_queue.put((file_path, id, directory_path, filename, file_size, delete, retry_count + 1))
                    return
                else:
                    PROGRESS_CACHE[id] = ("error", 0, file_size)
                    raise Exception(f"Upload failed after {MAX_RETRIES} retries due to timeout")

            # Get actual file size from uploaded message
            size = (
                message.photo
                or message.document
                or message.video
                or message.audio
                or message.sticker
            ).file_size

            filename = unquote_plus(filename)

            # Add to drive data
            DRIVE_DATA.new_file(directory_path, filename, message.id, size, duration)
            PROGRESS_CACHE[id] = ("completed", size, size)

            logger.info(f"Successfully uploaded file {filename} (ID: {id})")

            # Clean up file
            if delete:
                try:
                    os.remove(file_path)
                    logger.debug(f"Cleaned up temporary file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete file {file_path}: {e}")

        except Exception as e:
            logger.error(f"Upload failed for {filename}: {e}")
            PROGRESS_CACHE[id] = ("error", 0, file_size)
            
            # Retry logic for recoverable errors
            if retry_count < MAX_RETRIES and should_retry_error(e):
                logger.info(f"Retrying upload for {filename} due to recoverable error")
                # Re-queue for retry
                await upload_queue.put((file_path, id, directory_path, filename, file_size, delete, retry_count + 1))
                return
            else:
                if delete:
                    try:
                        os.remove(file_path)
                    except:
                        pass
                raise e
        finally:
            # Release client back to pool
            if client:
                release_client(client, file_size > 1.98 * 1024 * 1024 * 1024)


def should_retry_error(error):
    """Determine if an error is recoverable and should be retried"""
    error_str = str(error).lower()
    
    # Retry on network-related errors
    recoverable_errors = [
        'network', 'timeout', 'connection', 'flood', 'rate limit',
        'internal server error', 'bad gateway', 'service unavailable',
        'too many requests', 'temporary failure'
    ]
    
    return any(err in error_str for err in recoverable_errors)


# Cleanup function for graceful shutdown
async def shutdown_upload_system():
    """Gracefully shutdown the upload system"""
    global upload_queue, upload_workers_started
    
    if not upload_workers_started:
        return
    
    logger.info("Shutting down upload system...")
    
    # Wait for current uploads to complete
    await upload_queue.join()
    
    # Send shutdown signals to workers
    for _ in range(3):
        await upload_queue.put(None)
    
    logger.info("Upload system shutdown complete")