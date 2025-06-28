import asyncio
import os
import subprocess
import json
import tempfile
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from utils.logger import Logger
from utils.clients import get_client, release_client, update_client_performance
from utils.directoryHandler import DRIVE_DATA
from config import STORAGE_CHANNEL
import time

logger = Logger(__name__)

# Transcoding progress cache
TRANSCODE_PROGRESS = {}
STOP_TRANSCODE = []

# Supported formats and presets
SUPPORTED_FORMATS = {
    'mp4': {
        'extension': '.mp4',
        'codec': 'libx264',
        'audio_codec': 'aac',
        'container': 'mp4'
    },
    'webm': {
        'extension': '.webm',
        'codec': 'libvpx-vp9',
        'audio_codec': 'libopus',
        'container': 'webm'
    },
    'mkv': {
        'extension': '.mkv',
        'codec': 'libx264',
        'audio_codec': 'aac',
        'container': 'matroska'
    },
    'avi': {
        'extension': '.avi',
        'codec': 'libx264',
        'audio_codec': 'aac',
        'container': 'avi'
    }
}

QUALITY_PRESETS = {
    '240p': {
        'resolution': '426x240',
        'bitrate': '400k',
        'audio_bitrate': '64k',
        'fps': 24
    },
    '360p': {
        'resolution': '640x360',
        'bitrate': '800k',
        'audio_bitrate': '96k',
        'fps': 30
    },
    '480p': {
        'resolution': '854x480',
        'bitrate': '1200k',
        'audio_bitrate': '128k',
        'fps': 30
    },
    '720p': {
        'resolution': '1280x720',
        'bitrate': '2500k',
        'audio_bitrate': '192k',
        'fps': 30
    },
    '1080p': {
        'resolution': '1920x1080',
        'bitrate': '5000k',
        'audio_bitrate': '256k',
        'fps': 30
    }
}

SPEED_PRESETS = {
    'ultrafast': 'ultrafast',
    'superfast': 'superfast',
    'veryfast': 'veryfast',
    'faster': 'faster',
    'fast': 'fast',
    'medium': 'medium',
    'slow': 'slow',
    'slower': 'slower',
    'veryslow': 'veryslow'
}


class VideoTranscoder:
    def __init__(self):
        self.cache_dir = Path("./cache/transcode")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
    async def get_video_info(self, file_path: str) -> Dict:
        """Get video information using ffprobe"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                str(file_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                
                # Extract video stream info
                video_stream = None
                audio_stream = None
                
                for stream in data.get('streams', []):
                    if stream.get('codec_type') == 'video' and not video_stream:
                        video_stream = stream
                    elif stream.get('codec_type') == 'audio' and not audio_stream:
                        audio_stream = stream
                
                format_info = data.get('format', {})
                
                return {
                    'duration': float(format_info.get('duration', 0)),
                    'size': int(format_info.get('size', 0)),
                    'bitrate': int(format_info.get('bit_rate', 0)),
                    'format_name': format_info.get('format_name', ''),
                    'video': {
                        'codec': video_stream.get('codec_name', '') if video_stream else '',
                        'width': int(video_stream.get('width', 0)) if video_stream else 0,
                        'height': int(video_stream.get('height', 0)) if video_stream else 0,
                        'fps': eval(video_stream.get('r_frame_rate', '0/1')) if video_stream else 0,
                        'bitrate': int(video_stream.get('bit_rate', 0)) if video_stream else 0
                    },
                    'audio': {
                        'codec': audio_stream.get('codec_name', '') if audio_stream else '',
                        'sample_rate': int(audio_stream.get('sample_rate', 0)) if audio_stream else 0,
                        'channels': int(audio_stream.get('channels', 0)) if audio_stream else 0,
                        'bitrate': int(audio_stream.get('bit_rate', 0)) if audio_stream else 0
                    }
                }
            else:
                raise Exception(f"ffprobe failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"Error getting video info: {e}")
            raise e

    async def transcode_video(
        self,
        input_file: str,
        output_format: str,
        quality: str,
        transcode_id: str,
        custom_settings: Optional[Dict] = None,
        speed_preset: str = 'fast'
    ) -> str:
        """Transcode video with specified parameters"""
        
        if transcode_id in STOP_TRANSCODE:
            raise Exception("Transcoding cancelled")
            
        # Validate parameters
        if output_format not in SUPPORTED_FORMATS:
            raise Exception(f"Unsupported format: {output_format}")
            
        if quality not in QUALITY_PRESETS:
            raise Exception(f"Unsupported quality: {quality}")
            
        format_config = SUPPORTED_FORMATS[output_format]
        quality_config = QUALITY_PRESETS[quality]
        
        # Generate output filename
        input_path = Path(input_file)
        output_filename = f"{input_path.stem}_{quality}_{output_format}{format_config['extension']}"
        output_path = self.cache_dir / output_filename
        
        try:
            # Get input video info
            video_info = await self.get_video_info(input_file)
            total_duration = video_info['duration']
            
            TRANSCODE_PROGRESS[transcode_id] = {
                'status': 'starting',
                'progress': 0,
                'duration': total_duration,
                'current_time': 0,
                'speed': 0,
                'eta': 0,
                'output_file': str(output_path)
            }
            
            # Build ffmpeg command
            cmd = await self._build_ffmpeg_command(
                input_file,
                str(output_path),
                format_config,
                quality_config,
                custom_settings,
                speed_preset
            )
            
            logger.info(f"Starting transcoding: {' '.join(cmd)}")
            
            # Start transcoding process
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Monitor progress
            await self._monitor_transcode_progress(
                process,
                transcode_id,
                total_duration
            )
            
            # Wait for completion
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                TRANSCODE_PROGRESS[transcode_id]['status'] = 'completed'
                TRANSCODE_PROGRESS[transcode_id]['progress'] = 100
                logger.info(f"Transcoding completed: {output_path}")
                return str(output_path)
            else:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Transcoding failed: {error_msg}")
                TRANSCODE_PROGRESS[transcode_id]['status'] = 'error'
                raise Exception(f"Transcoding failed: {error_msg}")
                
        except Exception as e:
            TRANSCODE_PROGRESS[transcode_id]['status'] = 'error'
            logger.error(f"Transcoding error: {e}")
            
            # Clean up output file on error
            if output_path.exists():
                output_path.unlink()
                
            raise e

    async def _build_ffmpeg_command(
        self,
        input_file: str,
        output_file: str,
        format_config: Dict,
        quality_config: Dict,
        custom_settings: Optional[Dict],
        speed_preset: str
    ) -> List[str]:
        """Build ffmpeg command with specified parameters"""
        
        cmd = [
            'ffmpeg',
            '-i', input_file,
            '-y',  # Overwrite output file
            '-progress', 'pipe:2',  # Progress to stderr
            '-nostats',  # No stats output
            '-loglevel', 'error'  # Only show errors
        ]
        
        # Video codec and settings
        cmd.extend(['-c:v', format_config['codec']])
        
        # Quality settings
        if format_config['codec'] == 'libx264':
            cmd.extend([
                '-preset', speed_preset,
                '-crf', '23',  # Constant Rate Factor for quality
                '-maxrate', quality_config['bitrate'],
                '-bufsize', str(int(quality_config['bitrate'].rstrip('k')) * 2) + 'k'
            ])
        elif format_config['codec'] == 'libvpx-vp9':
            cmd.extend([
                '-deadline', 'good',
                '-cpu-used', '2',
                '-b:v', quality_config['bitrate']
            ])
        
        # Resolution and frame rate
        cmd.extend([
            '-vf', f"scale={quality_config['resolution']}:force_original_aspect_ratio=decrease",
            '-r', str(quality_config['fps'])
        ])
        
        # Audio codec and settings
        cmd.extend([
            '-c:a', format_config['audio_codec'],
            '-b:a', quality_config['audio_bitrate'],
            '-ar', '44100'  # Sample rate
        ])
        
        # Custom settings
        if custom_settings:
            for key, value in custom_settings.items():
                cmd.extend([f'-{key}', str(value)])
        
        # Output format
        cmd.extend(['-f', format_config['container']])
        
        # Output file
        cmd.append(output_file)
        
        return cmd

    async def _monitor_transcode_progress(
        self,
        process: asyncio.subprocess.Process,
        transcode_id: str,
        total_duration: float
    ):
        """Monitor transcoding progress from ffmpeg output"""
        
        TRANSCODE_PROGRESS[transcode_id]['status'] = 'transcoding'
        
        async def read_progress():
            if not process.stderr:
                return
                
            buffer = ""
            start_time = time.time()
            
            while True:
                try:
                    # Check if transcoding should be stopped
                    if transcode_id in STOP_TRANSCODE:
                        process.terminate()
                        await process.wait()
                        TRANSCODE_PROGRESS[transcode_id]['status'] = 'cancelled'
                        return
                    
                    chunk = await process.stderr.read(1024)
                    if not chunk:
                        break
                        
                    buffer += chunk.decode('utf-8', errors='ignore')
                    
                    # Process complete lines
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        
                        # Parse progress information
                        if 'out_time_ms=' in line:
                            try:
                                time_ms = int(line.split('out_time_ms=')[1].split()[0])
                                current_time = time_ms / 1000000  # Convert to seconds
                                
                                if total_duration > 0:
                                    progress = min((current_time / total_duration) * 100, 100)
                                    
                                    # Calculate speed and ETA
                                    elapsed_time = time.time() - start_time
                                    if elapsed_time > 0:
                                        speed = current_time / elapsed_time
                                        remaining_time = (total_duration - current_time) / speed if speed > 0 else 0
                                    else:
                                        speed = 0
                                        remaining_time = 0
                                    
                                    TRANSCODE_PROGRESS[transcode_id].update({
                                        'progress': progress,
                                        'current_time': current_time,
                                        'speed': speed,
                                        'eta': remaining_time
                                    })
                                    
                            except (ValueError, IndexError):
                                continue
                                
                except Exception as e:
                    logger.error(f"Error monitoring progress: {e}")
                    break
        
        # Start progress monitoring
        await read_progress()

    def get_supported_formats(self) -> Dict:
        """Get list of supported output formats"""
        return {
            format_name: {
                'extension': config['extension'],
                'description': f"{format_name.upper()} format"
            }
            for format_name, config in SUPPORTED_FORMATS.items()
        }

    def get_quality_presets(self) -> Dict:
        """Get list of available quality presets"""
        return {
            quality: {
                'resolution': config['resolution'],
                'description': f"{quality} ({config['resolution']})"
            }
            for quality, config in QUALITY_PRESETS.items()
        }

    def get_speed_presets(self) -> Dict:
        """Get list of encoding speed presets"""
        return {
            preset: f"{preset.title()} (Quality vs Speed trade-off)"
            for preset in SPEED_PRESETS.keys()
        }


# Global transcoder instance
transcoder = VideoTranscoder()


async def start_video_transcode(
    file_path: str,
    file_id: str,
    output_format: str,
    quality: str,
    transcode_id: str,
    directory_path: str,
    original_filename: str,
    custom_settings: Optional[Dict] = None,
    speed_preset: str = 'fast'
):
    """Start video transcoding process"""
    
    try:
        logger.info(f"Starting transcode: {file_path} -> {output_format} {quality}")
        
        # Download original file from Telegram
        client = get_client()
        
        try:
            # Get the message containing the file
            message = await client.get_messages(STORAGE_CHANNEL, int(file_id))
            
            if not message or not message.video and not message.document:
                raise Exception("File not found or not a video")
            
            # Download to temporary location
            temp_input = await message.download(file_name=f"temp_input_{transcode_id}")
            
            # Transcode the video
            output_file = await transcoder.transcode_video(
                temp_input,
                output_format,
                quality,
                transcode_id,
                custom_settings,
                speed_preset
            )
            
            # Upload transcoded file back to Telegram
            from utils.uploader import start_file_uploader
            
            # Generate new filename
            original_name = Path(original_filename).stem
            new_filename = f"{original_name}_{quality}_{output_format}{SUPPORTED_FORMATS[output_format]['extension']}"
            
            # Get file size
            output_size = os.path.getsize(output_file)
            
            # Start upload process
            await start_file_uploader(
                output_file,
                transcode_id,
                directory_path,
                new_filename,
                output_size,
                delete=True
            )
            
            # Clean up temporary input file
            if os.path.exists(temp_input):
                os.unlink(temp_input)
                
            logger.info(f"Transcode completed and uploaded: {new_filename}")
            
        finally:
            release_client(client)
            update_client_performance(client, True)
            
    except Exception as e:
        logger.error(f"Transcode failed: {e}")
        TRANSCODE_PROGRESS[transcode_id] = {
            'status': 'error',
            'error': str(e),
            'progress': 0
        }
        
        # Clean up files
        try:
            if 'temp_input' in locals() and os.path.exists(temp_input):
                os.unlink(temp_input)
        except:
            pass


def cancel_transcode(transcode_id: str):
    """Cancel ongoing transcoding"""
    if transcode_id not in STOP_TRANSCODE:
        STOP_TRANSCODE.append(transcode_id)
        logger.info(f"Transcode cancellation requested: {transcode_id}")


def get_transcode_progress(transcode_id: str) -> Optional[Dict]:
    """Get transcoding progress"""
    return TRANSCODE_PROGRESS.get(transcode_id)


def cleanup_transcode_cache():
    """Clean up old transcoding cache files"""
    try:
        cache_dir = Path("./cache/transcode")
        if cache_dir.exists():
            for file in cache_dir.glob("*"):
                if file.is_file():
                    # Remove files older than 1 hour
                    if time.time() - file.stat().st_mtime > 3600:
                        file.unlink()
                        logger.debug(f"Cleaned up old transcode file: {file}")
    except Exception as e:
        logger.error(f"Error cleaning transcode cache: {e}")


# Schedule cleanup task
async def schedule_cleanup():
    """Schedule periodic cleanup of transcode cache"""
    while True:
        await asyncio.sleep(1800)  # Run every 30 minutes
        cleanup_transcode_cache()


# Start cleanup task
asyncio.create_task(schedule_cleanup())