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
        self.connection_stats = {"speed": "fast", "last_update": time.time()}
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

    async def yield_file_fast(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
        quality: str = "720p"
    ):
        """
        Ultra-fast file streaming optimized for speed and minimal buffering
        """
        client = self.client
        logger.debug(f"Fast streaming: quality={quality}, chunk_size={chunk_size}")
        media_session = await self.generate_media_session(client, file_id)

        current_part = 1
        location = await self.get_location(file_id)
        
        # Aggressive optimization for speed
        max_retries = 2  # Reduced retries for speed
        timeout = 10.0   # Shorter timeout
        
        # Parallel chunk fetching for better speed
        concurrent_chunks = min(3, part_count)  # Fetch up to 3 chunks in parallel
        
        try:
            while current_part <= part_count:
                # Fetch multiple chunks concurrently for speed
                chunk_tasks = []
                
                for i in range(min(concurrent_chunks, part_count - current_part + 1)):
                    chunk_offset = offset + (i * chunk_size)
                    task = self._fetch_chunk_fast(media_session, location, chunk_offset, chunk_size, timeout)
                    chunk_tasks.append(task)
                
                # Wait for chunks and yield them in order
                try:
                    chunks = await asyncio.gather(*chunk_tasks, return_exceptions=True)
                    
                    for i, chunk_result in enumerate(chunks):
                        if isinstance(chunk_result, Exception):
                            logger.warning(f"Chunk {current_part + i} failed: {chunk_result}")
                            # Try single chunk fetch as fallback
                            chunk_offset = offset + (i * chunk_size)
                            chunk_result = await self._fetch_chunk_fast(media_session, location, chunk_offset, chunk_size, timeout)
                        
                        if chunk_result:
                            # Yield appropriate chunk portion
                            if part_count == 1:
                                yield chunk_result[first_part_cut:last_part_cut]
                            elif current_part == 1:
                                yield chunk_result[first_part_cut:]
                            elif current_part == part_count:
                                yield chunk_result[:last_part_cut]
                            else:
                                yield chunk_result
                        
                        current_part += 1
                        offset += chunk_size
                        
                        if current_part > part_count:
                            break
                            
                except Exception as e:
                    logger.error(f"Parallel chunk fetch failed: {e}")
                    # Fallback to single chunk mode
                    break
                    
        except Exception as e:
            logger.error(f"Fast streaming error: {e}")
            # Fallback to original method
            async for chunk in self.yield_file(file_id, offset, first_part_cut, last_part_cut, part_count, chunk_size):
                yield chunk

    async def _fetch_chunk_fast(self, media_session, location, offset, chunk_size, timeout):
        """Fetch a single chunk with optimized settings"""
        try:
            r = await asyncio.wait_for(
                media_session.invoke(
                    raw.functions.upload.GetFile(
                        location=location, 
                        offset=offset, 
                        limit=chunk_size
                    ),
                ),
                timeout=timeout
            )
            
            if isinstance(r, raw.types.upload.File):
                return r.bytes
            return None
            
        except asyncio.TimeoutError:
            logger.warning(f"Chunk fetch timeout at offset {offset}")
            return None
        except Exception as e:
            logger.warning(f"Chunk fetch error at offset {offset}: {e}")
            return None

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
        Fallback streaming method with standard optimization
        """
        client = self.client
        logger.debug(f"Standard streaming: chunk_size={chunk_size}")
        media_session = await self.generate_media_session(client, file_id)

        current_part = 1
        location = await self.get_location(file_id)
        retry_count = 0
        max_retries = 2

        try:
            while current_part <= part_count:
                try:
                    start_time = time.time()
                    
                    r = await asyncio.wait_for(
                        media_session.invoke(
                            raw.functions.upload.GetFile(
                                location=location, offset=offset, limit=chunk_size
                            ),
                        ),
                        timeout=15.0
                    )
                    
                    if isinstance(r, raw.types.upload.File):
                        chunk = r.bytes
                        if not chunk:
                            break
                            
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
                        offset += chunk_size
                        retry_count = 0
                        
                        # Minimal delay for very fast requests
                        download_time = time.time() - start_time
                        if download_time < 0.05:
                            await asyncio.sleep(0.01)
                            
                except (asyncio.TimeoutError, Exception) as e:
                    retry_count += 1
                    logger.warning(f"Chunk {current_part} failed (attempt {retry_count}): {e}")
                    
                    if retry_count >= max_retries:
                        logger.error(f"Max retries reached for chunk {current_part}")
                        break
                    
                    # Quick retry with minimal delay
                    await asyncio.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Streaming error: {e}")
        finally:
            logger.debug(f"Finished streaming {current_part-1} parts")

    async def clean_cache(self) -> None:
        """
        function to clean the cache to reduce memory usage
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            logger.debug("Cleaned the cache")