import asyncio
import json
import re
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import config
from utils.logger import Logger
from pathlib import Path

logger = Logger(f"{__name__}")

START_CMD = """🚀 **Welcome To TG Drive's Bot Mode**

You can use this bot to upload files to your TG Drive website directly instead of doing it from website.

🗄 **Commands:**
/set_folder - Set folder for file uploads
/current_folder - Check current folder
/create_folder - Create a new folder in current directory
/bulk_import - Import files in bulk from Telegram channel/group

📤 **How To Upload Files:** Send a file to this bot and it will be uploaded to your TG Drive website. You can also set a folder for file uploads using /set_folder command.

📁 **How To Create Folders:** Use /create_folder command to create new folders in your current directory.

📦 **How To Bulk Import:** Use /bulk_import command to import multiple files from a Telegram channel/group by providing a range of message links.

Read more about [TG Drive's Bot Mode](https://github.com/TechShreyash/TGDrive#tg-drives-bot-mode)
"""

SET_FOLDER_PATH_CACHE = {}
DRIVE_DATA = None
BOT_MODE = None 

# Global progress tracking for bulk imports
BULK_IMPORT_PROGRESS = {}

session_cache_path = Path(f"./cache")
session_cache_path.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_FOLDER_CONFIG_FILE = Path("./default_folder_config.json")

main_bot = Client(
    name="main_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.MAIN_BOT_TOKEN,
    sleep_threshold=config.SLEEP_THRESHOLD,
    workdir=session_cache_path,
)

# --- Manual 'ask' implementation setup ---
# Stores {chat_id: (asyncio.Queue, asyncio.Event, pyrogram.filters)}
_pending_requests = {}

async def manual_ask(client: Client, chat_id: int, text: str, timeout: int = 60, filters=None) -> Message:
    """
    A manual implementation of the 'ask' functionality for older Pyrogram versions.
    Sends a message and waits for a response from the specified chat_id.
    """
    queue = asyncio.Queue(1)
    event = asyncio.Event()
    
    _pending_requests[chat_id] = (queue, event, filters)

    await client.send_message(chat_id, text)

    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        response_message = await queue.get()
        return response_message
    except asyncio.TimeoutError:
        raise asyncio.TimeoutError # Re-raise if timed out
    finally:
        # Clean up the pending request regardless of outcome
        if chat_id in _pending_requests:
            del _pending_requests[chat_id]
# --- End Manual 'ask' implementation setup ---


# --- COMMAND HANDLERS (Prioritized) ---

@main_bot.on_message(
    filters.command(["start", "help"])
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def start_handler(client: Client, message: Message):
    """
    Handles the /start and /help commands, sending the welcome message.
    """
    await message.reply_text(START_CMD)


@main_bot.on_message(
    filters.command("bulk_import")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def bulk_import_handler(client: Client, message: Message):
    """
    Handles the /bulk_import command to import files in bulk from Telegram channels/groups.
    """
    global BOT_MODE, DRIVE_DATA

    # Check if there's already a pending ask for this chat to prevent re-triggering
    if message.chat.id in _pending_requests:
        await message.reply_text("I'm already waiting for your input. Please provide the required information or /cancel.")
        return 

    # Check if current folder is set
    if not BOT_MODE.current_folder:
        await message.reply_text(
            "❌ **Error:** No current folder set. Please use /set_folder to set a folder first before bulk importing files."
        )
        return

    await message.reply_text(
        "📦 **Bulk Import Files**\n\n"
        "This feature allows you to import multiple files from a Telegram channel or group.\n\n"
        "**How to use:**\n"
        "1. Get the link of the first file you want to import\n"
        "2. Get the link of the last file you want to import\n"
        "3. I'll import all files between these two messages\n\n"
        "**Example:**\n"
        "From: `https://t.me/ParmarEnglishPyqSeriesPart1/3`\n"
        "To: `https://t.me/ParmarEnglishPyqSeriesPart1/79`\n\n"
        "**Note:** Both links must be from the same channel/group.\n"
        "**Maximum:** Up to 1,000 files per bulk import.\n\n"
        "Let's start! Send /cancel to cancel anytime."
    )

    # Get the starting link
    try:
        start_link_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=(
                "📎 **Step 1/2: Starting Link**\n\n"
                "Please send the Telegram link of the **first file** you want to import.\n\n"
                "**Format:** `https://t.me/channel_name/message_id`\n\n"
                "Send /cancel to cancel"
            ),
            timeout=300,  # 5 minutes timeout
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nBulk import cancelled. Use /bulk_import to try again.")
        return

    if start_link_msg.text.lower() == "/cancel":
        await message.reply_text("❌ **Cancelled**\n\nBulk import cancelled.")
        return

    start_link = start_link_msg.text.strip()
    
    # Validate and parse the starting link
    start_parsed = parse_telegram_link(start_link)
    if not start_parsed:
        await message.reply_text(
            "❌ **Invalid Link Format**\n\n"
            "Please provide a valid Telegram link in the format:\n"
            "`https://t.me/channel_name/message_id`\n\n"
            "Use /bulk_import to try again."
        )
        return

    # Get the ending link
    try:
        end_link_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=(
                "📎 **Step 2/2: Ending Link**\n\n"
                "Please send the Telegram link of the **last file** you want to import.\n\n"
                "**Format:** `https://t.me/channel_name/message_id`\n\n"
                f"**Starting from:** {start_parsed['channel']}/{start_parsed['message_id']}\n\n"
                "Send /cancel to cancel"
            ),
            timeout=300,  # 5 minutes timeout
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nBulk import cancelled. Use /bulk_import to try again.")
        return

    if end_link_msg.text.lower() == "/cancel":
        await message.reply_text("❌ **Cancelled**\n\nBulk import cancelled.")
        return

    end_link = end_link_msg.text.strip()
    
    # Validate and parse the ending link
    end_parsed = parse_telegram_link(end_link)
    if not end_parsed:
        await message.reply_text(
            "❌ **Invalid Link Format**\n\n"
            "Please provide a valid Telegram link in the format:\n"
            "`https://t.me/channel_name/message_id`\n\n"
            "Use /bulk_import to try again."
        )
        return

    # Validate that both links are from the same channel
    if start_parsed['channel'] != end_parsed['channel']:
        await message.reply_text(
            "❌ **Channel Mismatch**\n\n"
            "Both links must be from the same channel or group.\n\n"
            f"**Starting link:** {start_parsed['channel']}\n"
            f"**Ending link:** {end_parsed['channel']}\n\n"
            "Use /bulk_import to try again."
        )
        return

    # Validate message ID range
    start_id = start_parsed['message_id']
    end_id = end_parsed['message_id']
    
    if start_id >= end_id:
        await message.reply_text(
            "❌ **Invalid Range**\n\n"
            "The starting message ID must be less than the ending message ID.\n\n"
            f"**Starting ID:** {start_id}\n"
            f"**Ending ID:** {end_id}\n\n"
            "Use /bulk_import to try again."
        )
        return

    # Calculate the number of files to import
    file_count = end_id - start_id + 1
    
    # Reduced limit to 1000 files for better performance
    if file_count > 1000:
        await message.reply_text(
            "❌ **Too Many Files**\n\n"
            f"You're trying to import {file_count:,} files. The maximum allowed is 1,000 files per bulk import.\n\n"
            "**Suggestions:**\n"
            "• Split your import into smaller ranges\n"
            "• Import in batches of 1,000 or fewer files\n\n"
            "Please reduce the range and try again."
        )
        return

    # Show warning for large imports
    warning_message = ""
    if file_count > 100:
        warning_message = (
            f"⚠️ **Large Import Warning:** You're importing {file_count:,} files. "
            f"This may take a significant amount of time (estimated: {file_count // 10 + 1} minutes).\n\n"
        )

    # Confirm the import
    confirmation_msg = await message.reply_text(
        f"📋 **Confirm Bulk Import**\n\n"
        f"**Channel:** {start_parsed['channel']}\n"
        f"**Range:** {start_id:,} to {end_id:,}\n"
        f"**Total files:** {file_count:,}\n"
        f"**Destination folder:** {BOT_MODE.current_folder_name}\n\n"
        f"{warning_message}"
        f"**Important:** This will import {file_count:,} files. Make sure you have enough storage space.\n\n"
        f"Type **YES** to confirm or **NO** to cancel."
    )

    try:
        confirm_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text="Please type **YES** to confirm or **NO** to cancel:",
            timeout=60,
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nBulk import cancelled due to timeout.")
        return

    if confirm_msg.text.upper() not in ["YES", "Y"]:
        await message.reply_text("❌ **Cancelled**\n\nBulk import cancelled by user.")
        return

    # Start the bulk import process
    import_id = f"bulk_{message.chat.id}_{start_id}_{end_id}"
    BULK_IMPORT_PROGRESS[import_id] = {
        'total': file_count,
        'imported': 0,
        'skipped': 0,
        'errors': 0,
        'current_message_id': start_id,
        'status': 'starting'
    }

    await message.reply_text(
        f"🚀 **Starting Bulk Import**\n\n"
        f"Importing {file_count:,} files from {start_parsed['channel']}...\n"
        f"This may take a while. I'll send you updates every 10 files.\n\n"
        f"**Current folder:** {BOT_MODE.current_folder_name}\n\n"
        f"**Progress:** 0/{file_count:,} (0%)"
    )

    # Start the bulk import task
    asyncio.create_task(
        bulk_import_files(
            client, 
            message.chat.id, 
            start_parsed['channel'], 
            start_id, 
            end_id,
            BOT_MODE.current_folder,
            import_id
        )
    )


def parse_telegram_link(link):
    """
    Parse a Telegram link and extract channel name and message ID.
    Returns a dict with 'channel' and 'message_id' or None if invalid.
    """
    # Pattern to match Telegram links
    patterns = [
        r'https://t\.me/([^/]+)/(\d+)',  # https://t.me/channel/123
        r'https://telegram\.me/([^/]+)/(\d+)',  # https://telegram.me/channel/123
        r't\.me/([^/]+)/(\d+)',  # t.me/channel/123
    ]
    
    for pattern in patterns:
        match = re.match(pattern, link.strip())
        if match:
            channel = match.group(1)
            message_id = int(match.group(2))
            return {
                'channel': channel,
                'message_id': message_id
            }
    
    return None


async def bulk_import_files(client, user_chat_id, channel_name, start_id, end_id, destination_folder, import_id):
    """
    Import files in bulk from a Telegram channel/group with improved progress tracking.
    """
    global DRIVE_DATA, BULK_IMPORT_PROGRESS
    
    try:
        # Try to resolve the channel
        try:
            channel = await client.get_chat(channel_name)
            channel_id = channel.id
        except Exception as e:
            await client.send_message(
                user_chat_id,
                f"❌ **Error accessing channel**\n\n"
                f"Could not access channel `{channel_name}`. Make sure:\n"
                f"1. The channel/group exists\n"
                f"2. The bot has access to the channel\n"
                f"3. The channel username is correct\n\n"
                f"**Error:** {str(e)}"
            )
            return

        total_files = end_id - start_id + 1
        progress = BULK_IMPORT_PROGRESS[import_id]
        progress['status'] = 'running'
        
        # Send initial status
        status_msg = await client.send_message(
            user_chat_id,
            f"📊 **Import Progress**\n\n"
            f"**Total:** {total_files:,}\n"
            f"**Imported:** 0\n"
            f"**Skipped:** 0\n"
            f"**Errors:** 0\n\n"
            f"**Progress:** 0/{total_files:,} (0%)\n"
            f"**Status:** Starting import..."
        )

        # Process files in smaller batches to avoid overwhelming the system
        batch_size = 5  # Process 5 files at a time
        current_batch = []
        
        for message_id in range(start_id, end_id + 1):
            progress['current_message_id'] = message_id
            
            try:
                # Get the message
                try:
                    source_message = await client.get_messages(channel_id, message_id)
                except Exception as e:
                    logger.warning(f"Could not get message {message_id} from {channel_name}: {e}")
                    progress['skipped'] += 1
                    continue

                # Check if message has media
                if not source_message or source_message.empty:
                    progress['skipped'] += 1
                    continue

                # Check if message has a file
                media = (
                    source_message.document
                    or source_message.video
                    or source_message.audio
                    or source_message.photo
                    or source_message.sticker
                )

                if not media:
                    progress['skipped'] += 1
                    continue

                # Add to batch for processing
                current_batch.append((message_id, source_message, media))
                
                # Process batch when it's full or we're at the end
                if len(current_batch) >= batch_size or message_id == end_id:
                    await process_batch(current_batch, destination_folder, progress, client, config.STORAGE_CHANNEL)
                    current_batch = []
                    
                    # Add delay between batches to avoid rate limiting
                    await asyncio.sleep(2)

                # Update status every 10 files or at the end
                processed = progress['imported'] + progress['skipped'] + progress['errors']
                if processed % 10 == 0 or message_id == end_id:
                    try:
                        progress_percentage = (processed / total_files) * 100
                        await status_msg.edit_text(
                            f"📊 **Import Progress**\n\n"
                            f"**Total:** {total_files:,}\n"
                            f"**Imported:** {progress['imported']:,}\n"
                            f"**Skipped:** {progress['skipped']:,}\n"
                            f"**Errors:** {progress['errors']:,}\n\n"
                            f"**Progress:** {processed:,}/{total_files:,} ({progress_percentage:.1f}%)\n"
                            f"**Current:** Processing message {message_id:,}"
                        )
                    except:
                        pass  # Ignore edit errors

            except Exception as e:
                logger.error(f"Unexpected error processing message {message_id}: {e}")
                progress['errors'] += 1

        # Send final status
        progress['status'] = 'completed'
        success_rate = (progress['imported'] / total_files * 100) if total_files > 0 else 0
        
        await client.send_message(
            user_chat_id,
            f"✅ **Bulk Import Completed**\n\n"
            f"**Total files processed:** {total_files:,}\n"
            f"**Successfully imported:** {progress['imported']:,}\n"
            f"**Skipped (no media):** {progress['skipped']:,}\n"
            f"**Errors:** {progress['errors']:,}\n\n"
            f"**Success rate:** {success_rate:.1f}%\n"
            f"**Destination folder:** {BOT_MODE.current_folder_name}\n\n"
            f"All imported files are now available on your TG Drive website! 🎉"
        )

    except Exception as e:
        logger.error(f"Bulk import failed: {e}")
        await client.send_message(
            user_chat_id,
            f"❌ **Bulk Import Failed**\n\n"
            f"An unexpected error occurred during the bulk import process.\n\n"
            f"**Error:** {str(e)}\n\n"
            f"Please try again or contact support if the issue persists."
        )
    finally:
        # Clean up progress tracking
        if import_id in BULK_IMPORT_PROGRESS:
            del BULK_IMPORT_PROGRESS[import_id]


async def process_batch(batch, destination_folder, progress, client, storage_channel):
    """Process a batch of files concurrently with rate limiting"""
    global DRIVE_DATA
    
    # Use semaphore to limit concurrent operations
    semaphore = asyncio.Semaphore(2)  # Max 2 concurrent uploads per batch
    
    async def process_single_file(message_id, source_message, media):
        async with semaphore:
            try:
                # Copy the message to storage channel
                copied_message = await source_message.copy(storage_channel)
                
                # Get file info from copied message
                copied_media = (
                    copied_message.document
                    or copied_message.video
                    or copied_message.audio
                    or copied_message.photo
                    or copied_message.sticker
                )

                # Add file to drive data
                DRIVE_DATA.new_file(
                    destination_folder,
                    copied_media.file_name or f"file_{message_id}",
                    copied_message.id,
                    copied_media.file_size or 0,
                )

                progress['imported'] += 1
                logger.info(f"Imported file from message {message_id}: {copied_media.file_name}")
                
                # Small delay to avoid overwhelming the API
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Error copying message {message_id}: {e}")
                progress['errors'] += 1

    # Process all files in the batch concurrently
    tasks = [
        process_single_file(message_id, source_message, media)
        for message_id, source_message, media in batch
    ]
    
    await asyncio.gather(*tasks, return_exceptions=True)


@main_bot.on_message(
    filters.command("create_folder")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def create_folder_handler(client: Client, message: Message):
    """
    Handles the /create_folder command to create new folders.
    Supports /create_folder <folder_name> for direct creation,
    or falls back to interactive mode if no argument provided.
    """
    global BOT_MODE, DRIVE_DATA

    # Check if there's already a pending ask for this chat to prevent re-triggering
    if message.chat.id in _pending_requests:
        await message.reply_text("I'm already waiting for your input. Please provide the folder name or /cancel.")
        return 

    # Check if current folder is set
    if not BOT_MODE.current_folder:
        await message.reply_text(
            "❌ **Error:** No current folder set. Please use /set_folder to set a folder first before creating new folders."
        )
        return

    # Extract the argument from the command, if any
    command_args = message.command
    folder_name_from_command = None
    if len(command_args) > 1:
        folder_name_from_command = " ".join(command_args[1:]).strip()

    target_folder_name = None
    if folder_name_from_command:
        # If a folder name was provided in the command, try to create it directly
        logger.info(f"Attempting direct folder creation: '{folder_name_from_command}'")
        
        # Validate folder name
        if not is_valid_folder_name(folder_name_from_command):
            await message.reply_text(
                "❌ **Invalid folder name!**\n\n"
                "Folder names can only contain:\n"
                "• Letters (a-z, A-Z)\n"
                "• Numbers (0-9)\n"
                "• Spaces, hyphens (-), underscores (_)\n"
                "• Brackets [ ] ( )\n"
                "• Some special characters: @ # ! $ % * + = { } : ; < > , . ? / | \\ ~ `"
            )
            return

        # Check if folder already exists
        current_folder_data = DRIVE_DATA.get_directory(BOT_MODE.current_folder)
        for item in current_folder_data.contents.values():
            if item.type == "folder" and item.name.lower() == folder_name_from_command.lower():
                await message.reply_text(
                    f"❌ **Folder already exists!**\n\n"
                    f"A folder named '{folder_name_from_command}' already exists in the current directory.\n\n"
                    f"**Current folder:** {BOT_MODE.current_folder_name}"
                )
                return

        # Create the folder
        try:
            new_folder_path = DRIVE_DATA.new_folder(BOT_MODE.current_folder, folder_name_from_command)
            await message.reply_text(
                f"✅ **Folder created successfully!**\n\n"
                f"**Folder name:** {folder_name_from_command}\n"
                f"**Location:** {BOT_MODE.current_folder_name}\n"
                f"**Full path:** {new_folder_path}"
            )
            logger.info(f"Folder '{folder_name_from_command}' created successfully at {new_folder_path}")
            return
        except Exception as e:
            await message.reply_text(
                f"❌ **Error creating folder!**\n\n"
                f"Failed to create folder '{folder_name_from_command}': {str(e)}"
            )
            logger.error(f"Failed to create folder '{folder_name_from_command}': {e}")
            return

    # If no argument was provided, proceed with the interactive 'ask' process
    while True:
        try:
            folder_name_input_msg = await manual_ask(
                client=client,
                chat_id=message.chat.id,
                text=(
                    f"📁 **Create New Folder**\n\n"
                    f"**Current location:** {BOT_MODE.current_folder_name}\n\n"
                    f"Please send the name for the new folder:\n\n"
                    f"**Rules:**\n"
                    f"• Use only letters, numbers, spaces, and basic symbols\n"
                    f"• Avoid special characters like: < > : \" | ? * \\\n"
                    f"• Maximum 255 characters\n\n"
                    f"Send /cancel to cancel"
                ),
                timeout=60,
                filters=filters.text,
            )
        except asyncio.TimeoutError:
            await message.reply_text("⏰ **Timeout**\n\nFolder creation cancelled. Use /create_folder to try again.")
            return

        if folder_name_input_msg.text.lower() == "/cancel":
            await message.reply_text("❌ **Cancelled**\n\nFolder creation cancelled.")
            return

        target_folder_name = folder_name_input_msg.text.strip()
        
        # Validate folder name
        if not target_folder_name:
            await message.reply_text("❌ **Empty name!** Please provide a valid folder name or /cancel.")
            continue
            
        if not is_valid_folder_name(target_folder_name):
            await message.reply_text(
                "❌ **Invalid folder name!**\n\n"
                "Folder names can only contain:\n"
                "• Letters (a-z, A-Z)\n"
                "• Numbers (0-9)\n"
                "• Spaces, hyphens (-), underscores (_)\n"
                "• Brackets [ ] ( )\n"
                "• Some special characters: @ # ! $ % * + = { } : ; < > , . ? / | \\ ~ `\n\n"
                "Please try again or /cancel."
            )
            continue

        # Check if folder already exists
        current_folder_data = DRIVE_DATA.get_directory(BOT_MODE.current_folder)
        folder_exists = False
        for item in current_folder_data.contents.values():
            if item.type == "folder" and item.name.lower() == target_folder_name.lower():
                folder_exists = True
                break

        if folder_exists:
            await message.reply_text(
                f"❌ **Folder already exists!**\n\n"
                f"A folder named '{target_folder_name}' already exists in the current directory.\n"
                f"Please choose a different name or /cancel."
            )
            continue

        # Create the folder
        try:
            new_folder_path = DRIVE_DATA.new_folder(BOT_MODE.current_folder, target_folder_name)
            await message.reply_text(
                f"✅ **Folder created successfully!**\n\n"
                f"**Folder name:** {target_folder_name}\n"
                f"**Location:** {BOT_MODE.current_folder_name}\n"
                f"**Full path:** {new_folder_path}"
            )
            logger.info(f"Folder '{target_folder_name}' created successfully at {new_folder_path}")
            break
        except Exception as e:
            await message.reply_text(
                f"❌ **Error creating folder!**\n\n"
                f"Failed to create folder '{target_folder_name}': {str(e)}\n\n"
                f"Please try again or /cancel."
            )
            logger.error(f"Failed to create folder '{target_folder_name}': {e}")
            continue


def is_valid_folder_name(name):
    """
    Validate folder name according to common file system restrictions.
    """
    if not name or len(name) > 255:
        return False
    
    # Check for invalid characters (similar to the web interface validation)
    import re
    pattern = r'^[a-zA-Z0-9 \-_\\[\]()@#!$%*+={}:;<>,.?/|\\~`]*$'
    return bool(re.match(pattern, name))


@main_bot.on_message(
    filters.command("set_folder")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def set_folder_handler(client: Client, message: Message):
    """
    Handles the /set_folder command.
    Supports /set_folder <folder_name> for direct setting,
    or falls back to interactive mode if no argument or ambiguity.
    """
    global SET_FOLDER_PATH_CACHE, DRIVE_DATA

    # Check if there's already a pending ask for this chat to prevent re-triggering
    if message.chat.id in _pending_requests:
        await message.reply_text("I'm already waiting for your input. Please provide the folder name or /cancel.")
        return 

    # Extract the argument from the command, if any
    # message.text will be something like "/set_folder grammar"
    # message.command will be ["set_folder", "grammar"]
    command_args = message.command
    folder_name_from_command = None
    if len(command_args) > 1:
        folder_name_from_command = " ".join(command_args[1:]).strip()

    target_folder_name = None
    if folder_name_from_command:
        # If a folder name was provided in the command, try to find it directly
        logger.info(f"Attempting direct set_folder for: '{folder_name_from_command}'")
        search_result = DRIVE_DATA.search_file_folder(folder_name_from_command)
        
        found_folders = {}
        for item in search_result.values():
            if item.type == "folder":
                found_folders[item.id] = item

        if len(found_folders) == 1:
            # Exactly one folder found, set it directly
            folder_id = list(found_folders.keys())[0]
            folder = found_folders[folder_id]
            path_segments = [seg for seg in folder.path.strip("/").split("/") if seg]
            folder_path = "/" + ("/".join(path_segments + [folder.id]))
            
            BOT_MODE.set_folder(folder_path, folder.name)

            # Persist the selected folder to the configuration file
            try:
                with open(DEFAULT_FOLDER_CONFIG_FILE, "w") as f:
                    json.dump({"current_folder": folder_path, "current_folder_name": folder.name}, f)
                logger.info(f"Saved default folder to config: {folder.name} -> {folder_path}")
            except Exception as e:
                logger.error(f"Failed to save default folder config: {e}")

            await message.reply_text(
                f"📁 **Folder Set Successfully!**\n\n"
                f"**Current folder:** {folder.name}\n\n"
                f"Now you can send/forward files to me and they will be uploaded to this folder.\n"
                f"You can also use /create_folder to create new folders in this location."
            )
            return # Exit handler after direct setting

        elif len(found_folders) > 1:
            # Multiple folders found, proceed to interactive selection
            await message.reply_text(f"Multiple folders found with name '{folder_name_from_command}'. Please select one:")
            target_folder_name = folder_name_from_command # Use this for generating buttons below
        else:
            # No folder found with the given name, prompt for interactive input
            await message.reply_text(f"No folder found with name '{folder_name_from_command}'. Please send the exact folder name:")
            target_folder_name = None # Will trigger the manual_ask below


    # If no argument was provided, or if direct setting was ambiguous/failed,
    # proceed with the interactive 'ask' process.
    if target_folder_name is None: # This means we need to ask the user for input
        while True:
            try:
                folder_name_input_msg = await manual_ask(
                    client=client,
                    chat_id=message.chat.id,
                    text="Send the folder name where you want to upload files\n\n/cancel to cancel",
                    timeout=60,
                    filters=filters.text,
                )
            except asyncio.TimeoutError:
                await message.reply_text("Timeout\n\nUse /set_folder to set folder again")
                return

            if folder_name_input_msg.text.lower() == "/cancel":
                await message.reply_text("Cancelled")
                return

            target_folder_name = folder_name_input_msg.text.strip()
            if not target_folder_name: # Handle empty input after ask
                await message.reply_text("Folder name cannot be empty. Please send a valid name or /cancel.")
                continue # Ask again
            
            search_result = DRIVE_DATA.search_file_folder(target_folder_name)
            
            folders = {}
            for item in search_result.values():
                if item.type == "folder":
                    folders[item.id] = item

            if len(folders) == 0:
                await message.reply_text(f"No Folder found with name '{target_folder_name}'")
            else:
                break # Found folders, proceed to show buttons
    else: # If target_folder_name was set due to ambiguity, we re-search with it
          # This branch handles the case where direct command resulted in multiple matches.
        search_result = DRIVE_DATA.search_file_folder(target_folder_name)
        folders = {}
        for item in search_result.values():
            if item.type == "folder":
                folders[item.id] = item

    # Proceed to show inline buttons for selection if interactive mode is needed
    if folders: # Only show buttons if there are folders to select
        buttons = []
        folder_cache = {}
        folder_cache_id = len(SET_FOLDER_PATH_CACHE) + 1

        for folder in folders.values():
            path_segments = [seg for seg in folder.path.strip("/").split("/") if seg]
            folder_path = "/" + ("/".join(path_segments + [folder.id]))
            
            folder_cache[folder.id] = (folder_path, folder.name)
            buttons.append(
                [
                    InlineKeyboardButton(
                        folder.name,
                        callback_data=f"set_folder_{folder_cache_id}_{folder.id}",
                    )
                ]
            )
        SET_FOLDER_PATH_CACHE[folder_cache_id] = folder_cache

        await message.reply_text(
            "Select the folder where you want to upload files",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    else:
        # This case should ideally be caught by len(folders) == 0 check earlier,
        # but as a safeguard if interactive input also yields no results.
        await message.reply_text(f"No folders found for '{target_folder_name}' after search. Please try /set_folder again.")


@main_bot.on_callback_query(
    filters.user(config.TELEGRAM_ADMIN_IDS) & filters.regex(r"set_folder_")
)
async def set_folder_callback(client: Client, callback_query: Message):
    """
    Handles the callback query when a user selects a folder from the inline buttons.
    Sets the selected folder as the current default and saves it to a config file.
    """
    global SET_FOLDER_PATH_CACHE, BOT_MODE

    folder_cache_id_str, folder_id = callback_query.data.split("_")[2:]
    folder_cache_id = int(folder_cache_id_str)

    folder_path_cache = SET_FOLDER_PATH_CACHE.get(folder_cache_id)
    if folder_path_cache is None:
        await callback_query.answer("Request Expired, Send /set_folder again")
        await callback_query.message.delete()
        return

    folder_path, name = folder_path_cache.get(folder_id)
    if folder_path is None:
        await callback_query.answer("Selected folder not found in cache. Please try again.")
        await callback_query.message.delete()
        return

    del SET_FOLDER_PATH_CACHE[folder_cache_id]

    BOT_MODE.set_folder(folder_path, name)

    try:
        with open(DEFAULT_FOLDER_CONFIG_FILE, "w") as f:
            json.dump({"current_folder": folder_path, "current_folder_name": name}, f)
        logger.info(f"Saved default folder to config: {name} -> {folder_path}")
    except Exception as e:
        logger.error(f"Failed to save default folder config: {e}")

    await callback_query.answer(f"Folder Set Successfully To : {name}")
    await callback_query.message.edit(
        f"📁 **Folder Set Successfully!**\n\n"
        f"**Current folder:** {name}\n\n"
        f"Now you can send/forward files to me and they will be uploaded to this folder.\n"
        f"You can also use /create_folder to create new folders in this location."
    )


@main_bot.on_message(
    filters.command("current_folder")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def current_folder_handler(client: Client, message: Message):
    """
    Handles the /current_folder command, displaying the currently set default folder.
    """
    global BOT_MODE

    if BOT_MODE.current_folder:
        await message.reply_text(
            f"📁 **Current Folder Information**\n\n"
            f"**Folder:** {BOT_MODE.current_folder_name}\n"
            f"**Path:** {BOT_MODE.current_folder}\n\n"
            f"💡 **Available commands:**\n"
            f"• Send files to upload them here\n"
            f"• /create_folder - Create new folders\n"
            f"• /set_folder - Change current folder\n"
            f"• /bulk_import - Import files in bulk"
        )
    else:
        await message.reply_text(
            f"❌ **No current folder set**\n\n"
            f"Use /set_folder to set a folder first.\n\n"
            f"💡 **Available commands:**\n"
            f"• /set_folder - Set current folder\n"
            f"• /help - Show all commands"
        )


@main_bot.on_message(
    filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS)
    & (
        filters.document
        | filters.video
        | filters.audio
        | filters.photo
        | filters.sticker
    )
)
async def file_handler(client: Client, message: Message):
    """
    Handles incoming file messages (documents, videos, audio, photos, stickers).
    Uploads the file to the currently set default folder.
    """
    global BOT_MODE, DRIVE_DATA

    # Ensure there's no pending ask request for this chat before processing files
    # This prevents file uploads from interfering with an active /set_folder conversation
    if message.chat.id in _pending_requests:
        logger.debug(f"Ignoring file from {message.chat.id} due to pending ask request.")
        return # Do not process file if waiting for text input

    if not BOT_MODE.current_folder:
        await message.reply_text(
            "❌ **Error:** No default folder set.\n\n"
            "Please use /set_folder to set one before uploading files.\n\n"
            "💡 **Quick start:**\n"
            "1. Use /set_folder to choose a folder\n"
            "2. Send files to upload them\n"
            "3. Use /create_folder to create new folders\n"
            "4. Use /bulk_import to import files in bulk"
        )
        return

    # Add rate limiting for individual file uploads
    await asyncio.sleep(0.5)  # Small delay to prevent overwhelming the system

    copied_message = await message.copy(config.STORAGE_CHANNEL)
    file = (
        copied_message.document
        or copied_message.video
        or copied_message.audio
        or copied_message.photo
        or copied_message.sticker
    )

    DRIVE_DATA.new_file(
        BOT_MODE.current_folder,
        file.file_name,
        copied_message.id,
        file.file_size,
    )

    await message.reply_text(
        f"""✅ **File Uploaded Successfully!**
                             
**File Name:** {file.file_name}
**Folder:** {BOT_MODE.current_folder_name}
**Size:** {file.file_size / (1024*1024):.2f} MB

💡 **What's next?**
• Send more files to upload them
• Use /create_folder to create new folders
• Use /set_folder to change location
• Use /bulk_import to import files in bulk
"""
    )

# --- GENERIC MESSAGE HANDLER (Lowest Priority) ---
# This handler MUST be defined AFTER all specific command and file handlers.
@main_bot.on_message(filters.private & filters.user(config.TELEGRAM_ADMIN_IDS) & filters.text)
async def _handle_all_messages(client: Client, message: Message):
    """
    This handler listens for all private text messages from authorized users.
    If a pending 'ask' request exists for this chat, it fulfills it and
    then explicitly returns to prevent further handler processing for this message.
    This handler is placed last to give precedence to specific command handlers.
    """
    chat_id = message.chat.id
    if chat_id in _pending_requests:
        queue, event, msg_filters = _pending_requests[chat_id]

        if msg_filters is None or msg_filters(None, message): 
            await queue.put(message)
            event.set() # Signal that a response has been received
            return # CRITICAL: Stop processing this message, it's been handled for 'ask'
        else:
            logger.debug(f"Message from {chat_id} did not match pending ask filter. Allowing other handlers.")


async def start_bot_mode(d, b):
    """
    Initializes the bot mode, starts the main bot client, and sets the initial
    default folder based on saved configuration or falls back to 'grammar'.
    """
    global DRIVE_DATA, BOT_MODE
    DRIVE_DATA = d
    BOT_MODE = b

    logger.info("Starting Main Bot")
    await main_bot.start()

    default_folder_path = None
    default_folder_name_to_use = None

    if DEFAULT_FOLDER_CONFIG_FILE.exists():
        try:
            with open(DEFAULT_FOLDER_CONFIG_FILE, "r") as f:
                config_data = json.load(f)
                default_folder_path = config_data.get("current_folder")
                default_folder_name_to_use = config_data.get("current_folder_name")
            if default_folder_path and default_folder_name_to_use:
                BOT_MODE.set_folder(default_folder_path, default_folder_name_to_use)
                logger.info(f"Loaded default folder from config: {default_folder_name_to_use} -> {default_folder_path}")
            else:
                logger.warning("Default folder config file found but data is incomplete. Falling back to 'grammar'.")
                default_folder_path = None
                default_folder_name_to_use = None
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error reading default folder config file: {e}. Falling back to 'grammar'.")
            default_folder_path = None
            default_folder_name_to_use = None

    if default_folder_path and default_folder_name_to_use:
        BOT_MODE.set_folder(default_folder_path, default_folder_name_to_use)
        message_to_send = f"Main Bot Started -> TG Drive's Bot Mode Enabled with previously set folder: {default_folder_name_to_use}"
    else:
        hardcoded_default_folder_name = "grammar"
        search_result = DRIVE_DATA.search_file_folder(hardcoded_default_folder_name)
        found_grammar = False
        for item in search_result.values():
            if item.type == "folder":
                path_segments = [seg for seg in item.path.strip("/").split("/") if seg]
                folder_path = "/" + ("/".join(path_segments + [item.id]))
                
                BOT_MODE.set_folder(folder_path, item.name)
                logger.info(f"Default folder set to: {item.name} -> {folder_path}")
                try:
                    with open(DEFAULT_FOLDER_CONFIG_FILE, "w") as f:
                        json.dump({"current_folder": folder_path, "current_folder_name": item.name}, f)
                    logger.info(f"Saved initial 'grammar' default folder to config.")
                except Exception as e:
                    logger.error(f"Failed to save initial default folder config: {e}")
                found_grammar = True
                break
        if not found_grammar:
            logger.warning(f"No folder found with name '{hardcoded_default_folder_name}'. No default folder set initially.")
            BOT_MODE.set_folder(None, "No default folder set. Please use /set_folder.") 
            message_to_send = "Main Bot Started -> TG Drive's Bot Mode Enabled. No 'grammar' folder found, please use /set_folder to choose one."

        else:
            message_to_send = "Main Bot Started -> TG Drive's Bot Mode Enabled with default folder Grammar"

    await main_bot.send_message(
        config.STORAGE_CHANNEL,
        message_to_send,
    )
    logger.info(message_to_send)