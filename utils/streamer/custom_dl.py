import asyncio
from typing import Dict, Union
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from utils.logger import Logger
import time

logger = Logger(__name__)


class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        self.connection_stats = {"speed": "medium", "last_update": time.time()}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, channel, message_id: int) -> FileId:
        if message_id not in self.cached_file_ids:
            await self.generate_file_properties(channel, message_id)
        return self.cached_file_ids[message_id]

    async def generate_file_properties(self, channel, message_id: int) -> FileId:
        file_id = await get_file_ids(self.client, channel, message_id)
        if not file_id:
            raise Exception("FileNotFound")
        self.cached_file_ids[message_id] = file_id
        return self.cached_file_ids[message_id]

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        This is required for getting the bytes from Telegram servers.
        """

        media_session = client.media_sessions.get(file_id.dc_id, None)

        if media_session is None:
            if file_id.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await Auth(
                        client, file_id.dc_id, await client.storage.test_mode()
                    ).create(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

                for _ in range(6):
                    exported_auth = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                    )

                    try:
                        await media_session.invoke(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        logger.debug(
                            f"Invalid authorization bytes for DC {file_id.dc_id}"
                        )
                        continue
                else:
                    await media_session.stop()
                    raise AuthBytesInvalid
            else:
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()
            logger.debug(f"Created media session for DC {file_id.dc_id}")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            logger.debug(f"Using cached media session for DC {file_id.dc_id}")
        return media_session

    @staticmethod
    async def get_location(
        file_id: FileId,
    ) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        """
        Returns the file location for the media file.
        """
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def yield_file(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        """
        Custom generator that yields the bytes of the media file.
        """
        client = self.client
        logger.debug(f"Starting to yielding file with client.")
        media_session = await self.generate_media_session(client, file_id)

        current_part = 1
        location = await self.get_location(file_id)

        try:
            r = await media_session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=offset, limit=chunk_size
                ),
            )
            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        break
                    elif part_count == 1:
                        yield chunk[first_part_cut:last_part_cut]
                    elif current_part == 1:
                        yield chunk[first_part_cut:]
                    elif current_part == part_count:
                        yield chunk[:last_part_cut]
                    else:
                        yield chunk

                    current_part += 1
                    offset += chunk_size

                    if current_part > part_count:
                        break

                    r = await media_session.invoke(
                        raw.functions.upload.GetFile(
                            location=location, offset=offset, limit=chunk_size
                        ),
                    )
        except (TimeoutError, AttributeError):
            pass
        finally:
            logger.debug(f"Finished yielding file with {current_part} parts.")

    async def yield_file_adaptive(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
        quality: str = "auto"
    ):
        """
        Adaptive generator that adjusts chunk size and retry logic for slow connections
        """
        client = self.client
        logger.debug(f"Starting adaptive file streaming with quality: {quality}")
        media_session = await self.generate_media_session(client, file_id)

        current_part = 1
        location = await self.get_location(file_id)
        retry_count = 0
        max_retries = 3
        
        # Adaptive chunk size based on quality
        adaptive_chunk_size = self.get_adaptive_chunk_size(chunk_size, quality)
        
        try:
            while current_part <= part_count:
                try:
                    start_time = time.time()
                    
                    r = await asyncio.wait_for(
                        media_session.invoke(
                            raw.functions.upload.GetFile(
                                location=location, 
                                offset=offset, 
                                limit=adaptive_chunk_size
                            ),
                        ),
                        timeout=30.0  # 30 second timeout
                    )
                    
                    if isinstance(r, raw.types.upload.File):
                        chunk = r.bytes
                        if not chunk:
                            break
                            
                        # Measure download speed
                        download_time = time.time() - start_time
                        self.update_connection_stats(len(chunk), download_time)
                        
                        # Yield appropriate chunk portion
                        if part_count == 1:
                            yield chunk[first_part_cut:last_part_cut]
                        elif current_part == 1:
                            yield chunk[first_part_cut:]
                        elif current_part == part_count:
                            yield chunk[:last_part_cut]
                        else:
                            yield chunk

                        current_part += 1
                        offset += adaptive_chunk_size
                        retry_count = 0  # Reset retry count on success
                        
                        # Adaptive delay for slow connections
                        if self.connection_stats["speed"] == "slow":
                            await asyncio.sleep(0.1)  # Small delay for slow connections
                            
                except (asyncio.TimeoutError, Exception) as e:
                    retry_count += 1
                    logger.warning(f"Chunk {current_part} failed (attempt {retry_count}): {e}")
                    
                    if retry_count >= max_retries:
                        logger.error(f"Max retries reached for chunk {current_part}")
                        break
                    
                    # Exponential backoff with jitter
                    delay = min(2 ** retry_count + (retry_count * 0.1), 10)
                    await asyncio.sleep(delay)
                    
                    # Reduce chunk size on repeated failures
                    if retry_count > 1:
                        adaptive_chunk_size = max(adaptive_chunk_size // 2, 64 * 1024)
                        logger.info(f"Reduced chunk size to {adaptive_chunk_size} bytes")
                        
        except Exception as e:
            logger.error(f"Fatal error in adaptive streaming: {e}")
        finally:
            logger.debug(f"Finished adaptive streaming with {current_part-1} parts.")

    def get_adaptive_chunk_size(self, base_chunk_size: int, quality: str) -> int:
        """Get adaptive chunk size based on quality and connection speed"""
        quality_multipliers = {
            "low": 0.25,     # 25% of base size for slow connections
            "medium": 0.5,   # 50% of base size
            "high": 1.0,     # Full base size
            "auto": 0.5      # Default to medium
        }
        
        multiplier = quality_multipliers.get(quality, 0.5)
        
        # Further adjust based on connection speed
        if self.connection_stats["speed"] == "slow":
            multiplier *= 0.5
        elif self.connection_stats["speed"] == "fast":
            multiplier *= 1.5
            
        return max(int(base_chunk_size * multiplier), 64 * 1024)  # Minimum 64KB

    def update_connection_stats(self, bytes_downloaded: int, download_time: float):
        """Update connection speed statistics"""
        if download_time > 0:
            speed_bps = bytes_downloaded / download_time
            speed_kbps = speed_bps / 1024
            
            # Categorize connection speed
            if speed_kbps < 100:  # Less than 100 KB/s
                self.connection_stats["speed"] = "slow"
            elif speed_kbps < 500:  # Less than 500 KB/s
                self.connection_stats["speed"] = "medium"
            else:
                self.connection_stats["speed"] = "fast"
                
            self.connection_stats["last_update"] = time.time()
            logger.debug(f"Connection speed: {speed_kbps:.1f} KB/s ({self.connection_stats['speed']})")

    async def clean_cache(self) -> None:
        """
        function to clean the cache to reduce memory usage
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            logger.debug("Cleaned the cache")