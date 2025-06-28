import asyncio
import os
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from utils.logger import Logger
from utils.directoryHandler import NewDriveData, NewBotMode
import config
from utils.transcoder import start_video_transcode, get_transcode_progress, QUALITY_PRESETS, SUPPORTED_FORMATS
from utils.directoryHandler import getRandomID
import time

logger = Logger(__name__)

# Global variables for bot mode
main_bot = None
BULK_IMPORT_PROGRESS = {}

# Transcode command states
TRANSCODE_STATES = {}

async def start_bot_mode(drive_data: NewDriveData, bot_mode: NewBotMode):
    global main_bot
    
    if not config.MAIN_BOT_TOKEN:
        logger.warning("MAIN_BOT_TOKEN not provided, bot mode disabled")
        return
    
    try:
        main_bot = Client(
            "main_bot",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=config.MAIN_BOT_TOKEN,
            workdir="./cache"
        )
        
        await main_bot.start()
        logger.info("Main bot started successfully")
        
        # Register handlers
        register_handlers(drive_data, bot_mode)
        
    except Exception as e:
        logger.error(f"Failed to start main bot: {e}")

def register_handlers(drive_data: NewDriveData, bot_mode: NewBotMode):
    """Register all bot command handlers"""
    
    @main_bot.on_message(filters.command("start") & filters.private)
    async def start_command(client: Client, message: Message):
        if message.from_user.id not in config.TELEGRAM_ADMIN_IDS:
            await message.reply("❌ You are not authorized to use this bot.")
            return
        
        welcome_text = """
🚀 **Welcome to TGDrive Bot!**

Available commands:
📁 `/set_folder` - Set upload folder
📂 `/current_folder` - Check current folder
🆕 `/create_folder` - Create new folder
🎬 `/transcode` - Transcode videos in current folder
📊 `/transcode_status` - Check transcoding progress

Just send me any file and I'll upload it to your current folder!
        """
        
        await message.reply(welcome_text)

    @main_bot.on_message(filters.command("set_folder") & filters.private)
    async def set_folder_command(client: Client, message: Message):
        if message.from_user.id not in config.TELEGRAM_ADMIN_IDS:
            await message.reply("❌ You are not authorized to use this bot.")
            return
        
        # Get folder tree and create inline keyboard
        folder_tree = drive_data.get_folder_tree()
        keyboard = create_folder_keyboard(folder_tree)
        
        await message.reply(
            "📁 **Select a folder:**\n\nChoose the folder where you want to upload files:",
            reply_markup=keyboard
        )

    @main_bot.on_message(filters.command("current_folder") & filters.private)
    async def current_folder_command(client: Client, message: Message):
        if message.from_user.id not in config.TELEGRAM_ADMIN_IDS:
            await message.reply("❌ You are not authorized to use this bot.")
            return
        
        current_folder = bot_mode.current_folder_name
        await message.reply(f"📂 **Current folder:** {current_folder}")

    @main_bot.on_message(filters.command("create_folder") & filters.private)
    async def create_folder_command(client: Client, message: Message):
        if message.from_user.id not in config.TELEGRAM_ADMIN_IDS:
            await message.reply("❌ You are not authorized to use this bot.")
            return
        
        # Extract folder name from command
        command_parts = message.text.split(maxsplit=1)
        
        if len(command_parts) > 1:
            folder_name = command_parts[1].strip()
            
            if folder_name:
                try:
                    new_folder_path = drive_data.new_folder(bot_mode.current_folder, folder_name)
                    await message.reply(f"✅ **Folder created successfully!**\n\n📁 **Name:** {folder_name}\n📂 **Path:** {new_folder_path}")
                except Exception as e:
                    await message.reply(f"❌ **Error creating folder:** {str(e)}")
            else:
                await message.reply("❌ **Please provide a folder name.**\n\nUsage: `/create_folder My New Folder`")
        else:
            await message.reply(
                "🆕 **Create New Folder**\n\n"
                "Please send the folder name you want to create.\n\n"
                "**Usage:** `/create_folder <folder_name>`\n"
                "**Example:** `/create_folder My Documents`"
            )

    @main_bot.on_message(filters.command("transcode") & filters.private)
    async def transcode_command(client: Client, message: Message):
        if message.from_user.id not in config.TELEGRAM_ADMIN_IDS:
            await message.reply("❌ You are not authorized to use this bot.")
            return
        
        # Get video files in current folder
        current_folder_data = drive_data.get_directory(bot_mode.current_folder)
        video_files = []
        
        for item_id, item in current_folder_data.contents.items():
            if item.type == "file" and is_video_file(item.name):
                video_files.append(item)
        
        if not video_files:
            await message.reply(
                f"📂 **Current folder:** {bot_mode.current_folder_name}\n\n"
                "❌ **No video files found in the current folder.**\n\n"
                "Please upload some video files first or change to a folder that contains videos."
            )
            return
        
        # Show transcode options
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🎬 Start Transcoding", callback_data="transcode_start"),
                InlineKeyboardButton("📋 Show Videos", callback_data="transcode_list")
            ],
            [
                InlineKeyboardButton("⚙️ Settings", callback_data="transcode_settings"),
                InlineKeyboardButton("❌ Cancel", callback_data="transcode_cancel")
            ]
        ])
        
        await message.reply(
            f"🎬 **Video Transcoding**\n\n"
            f"📂 **Current folder:** {bot_mode.current_folder_name}\n"
            f"🎥 **Video files found:** {len(video_files)}\n\n"
            f"**Default settings:**\n"
            f"📤 Format: MP4\n"
            f"🎯 Quality: 720p\n"
            f"⚡ Speed: Fast\n\n"
            f"Choose an option below:",
            reply_markup=keyboard
        )
        
        # Store user state
        TRANSCODE_STATES[message.from_user.id] = {
            'video_files': video_files,
            'format': 'mp4',
            'quality': '720p',
            'speed': 'fast',
            'folder_path': bot_mode.current_folder,
            'folder_name': bot_mode.current_folder_name
        }

    @main_bot.on_message(filters.command("transcode_status") & filters.private)
    async def transcode_status_command(client: Client, message: Message):
        if message.from_user.id not in config.TELEGRAM_ADMIN_IDS:
            await message.reply("❌ You are not authorized to use this bot.")
            return
        
        # Check for active transcoding operations
        active_transcodes = []
        for transcode_id, progress in get_all_transcode_progress().items():
            if progress.get('status') in ['starting', 'transcoding']:
                active_transcodes.append((transcode_id, progress))
        
        if not active_transcodes:
            await message.reply("📊 **No active transcoding operations.**")
            return
        
        status_text = "📊 **Active Transcoding Operations:**\n\n"
        
        for transcode_id, progress in active_transcodes:
            status = progress.get('status', 'unknown')
            progress_percent = progress.get('progress', 0)
            speed = progress.get('speed', 0)
            eta = progress.get('eta', 0)
            
            status_text += f"🎬 **ID:** `{transcode_id}`\n"
            status_text += f"📊 **Progress:** {progress_percent:.1f}%\n"
            status_text += f"⚡ **Speed:** {speed:.1f}x\n"
            status_text += f"⏱️ **ETA:** {format_duration(eta)}\n\n"
        
        await message.reply(status_text)

    @main_bot.on_callback_query()
    async def handle_callback_query(client: Client, callback_query: CallbackQuery):
        if callback_query.from_user.id not in config.TELEGRAM_ADMIN_IDS:
            await callback_query.answer("❌ You are not authorized to use this bot.")
            return
        
        data = callback_query.data
        user_id = callback_query.from_user.id
        
        # Handle folder selection
        if data.startswith("folder_"):
            folder_path = data.replace("folder_", "").replace("__", "/")
            if folder_path == "root":
                folder_path = "/"
            
            # Get folder name
            if folder_path == "/":
                folder_name = "/ (root directory)"
            else:
                folder_data = drive_data.get_directory(folder_path)
                folder_name = folder_data.name
            
            bot_mode.set_folder(folder_path, folder_name)
            
            await callback_query.edit_message_text(
                f"✅ **Folder set successfully!**\n\n📁 **Selected folder:** {folder_name}\n📂 **Path:** {folder_path}"
            )
        
        # Handle transcode callbacks
        elif data == "transcode_start":
            if user_id not in TRANSCODE_STATES:
                await callback_query.answer("❌ Session expired. Please run /transcode again.")
                return
            
            state = TRANSCODE_STATES[user_id]
            await start_bulk_transcode(callback_query, state)
        
        elif data == "transcode_list":
            if user_id not in TRANSCODE_STATES:
                await callback_query.answer("❌ Session expired. Please run /transcode again.")
                return
            
            state = TRANSCODE_STATES[user_id]
            await show_video_list(callback_query, state)
        
        elif data == "transcode_settings":
            if user_id not in TRANSCODE_STATES:
                await callback_query.answer("❌ Session expired. Please run /transcode again.")
                return
            
            await show_transcode_settings(callback_query, user_id)
        
        elif data == "transcode_cancel":
            if user_id in TRANSCODE_STATES:
                del TRANSCODE_STATES[user_id]
            
            await callback_query.edit_message_text("❌ **Transcoding cancelled.**")
        
        # Handle settings callbacks
        elif data.startswith("set_format_"):
            format_name = data.replace("set_format_", "")
            if user_id in TRANSCODE_STATES:
                TRANSCODE_STATES[user_id]['format'] = format_name
            await show_transcode_settings(callback_query, user_id)
        
        elif data.startswith("set_quality_"):
            quality = data.replace("set_quality_", "")
            if user_id in TRANSCODE_STATES:
                TRANSCODE_STATES[user_id]['quality'] = quality
            await show_transcode_settings(callback_query, user_id)
        
        elif data.startswith("set_speed_"):
            speed = data.replace("set_speed_", "")
            if user_id in TRANSCODE_STATES:
                TRANSCODE_STATES[user_id]['speed'] = speed
            await show_transcode_settings(callback_query, user_id)
        
        elif data == "settings_back":
            await show_main_transcode_menu(callback_query, user_id)

    @main_bot.on_message(filters.document | filters.video | filters.audio | filters.photo)
    async def handle_file_upload(client: Client, message: Message):
        if message.from_user.id not in config.TELEGRAM_ADMIN_IDS:
            await message.reply("❌ You are not authorized to use this bot.")
            return
        
        # Handle file upload logic (existing functionality)
        # This would be the existing file upload code
        pass

def create_folder_keyboard(folder_tree, prefix=""):
    """Create inline keyboard for folder selection"""
    keyboard = []
    
    # Add root folder option
    if prefix == "":
        keyboard.append([InlineKeyboardButton("📁 Root Folder", callback_data="folder_root")])
    
    # Add subfolders
    for child in folder_tree.get("children", []):
        folder_name = child["name"]
        folder_path = child["path"].replace("/", "__")
        
        # Limit folder name length for display
        display_name = folder_name[:25] + "..." if len(folder_name) > 25 else folder_name
        
        keyboard.append([InlineKeyboardButton(f"📁 {display_name}", callback_data=f"folder_{folder_path}")])
    
    return InlineKeyboardMarkup(keyboard)

async def show_video_list(callback_query: CallbackQuery, state: dict):
    """Show list of video files in current folder"""
    video_files = state['video_files']
    
    video_list = "🎥 **Video files in current folder:**\n\n"
    
    for i, video in enumerate(video_files[:10], 1):  # Limit to 10 files for display
        file_size = format_file_size(video.size)
        duration = format_duration(video.duration) if video.duration else "Unknown"
        
        video_list += f"{i}. **{video.name}**\n"
        video_list += f"   📊 Size: {file_size}\n"
        video_list += f"   ⏱️ Duration: {duration}\n\n"
    
    if len(video_files) > 10:
        video_list += f"... and {len(video_files) - 10} more files\n\n"
    
    video_list += f"**Total:** {len(video_files)} video files"
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 Start Transcoding", callback_data="transcode_start"),
            InlineKeyboardButton("⚙️ Settings", callback_data="transcode_settings")
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="transcode_cancel")]
    ])
    
    await callback_query.edit_message_text(video_list, reply_markup=keyboard)

async def show_transcode_settings(callback_query: CallbackQuery, user_id: int):
    """Show transcoding settings menu"""
    if user_id not in TRANSCODE_STATES:
        await callback_query.answer("❌ Session expired. Please run /transcode again.")
        return
    
    state = TRANSCODE_STATES[user_id]
    
    # Format selection keyboard
    format_keyboard = []
    for format_name in SUPPORTED_FORMATS.keys():
        current = "✅ " if state['format'] == format_name else ""
        format_keyboard.append([InlineKeyboardButton(
            f"{current}{format_name.upper()}", 
            callback_data=f"set_format_{format_name}"
        )])
    
    # Quality selection keyboard
    quality_keyboard = []
    for quality in QUALITY_PRESETS.keys():
        current = "✅ " if state['quality'] == quality else ""
        quality_keyboard.append([InlineKeyboardButton(
            f"{current}{quality}", 
            callback_data=f"set_quality_{quality}"
        )])
    
    # Speed selection keyboard
    speed_options = ['ultrafast', 'fast', 'medium', 'slow']
    speed_keyboard = []
    for speed in speed_options:
        current = "✅ " if state['speed'] == speed else ""
        speed_keyboard.append([InlineKeyboardButton(
            f"{current}{speed.title()}", 
            callback_data=f"set_speed_{speed}"
        )])
    
    settings_text = (
        f"⚙️ **Transcoding Settings**\n\n"
        f"📂 **Folder:** {state['folder_name']}\n"
        f"🎥 **Videos:** {len(state['video_files'])}\n\n"
        f"**Current Settings:**\n"
        f"📤 **Format:** {state['format'].upper()}\n"
        f"🎯 **Quality:** {state['quality']}\n"
        f"⚡ **Speed:** {state['speed'].title()}\n\n"
        f"**Select format:**"
    )
    
    # Combine all keyboards
    all_keyboards = format_keyboard + [
        [InlineKeyboardButton("🔽 Quality Settings 🔽", callback_data="quality_header")]
    ] + quality_keyboard + [
        [InlineKeyboardButton("🔽 Speed Settings 🔽", callback_data="speed_header")]
    ] + speed_keyboard + [
        [
            InlineKeyboardButton("🔙 Back", callback_data="settings_back"),
            InlineKeyboardButton("🎬 Start", callback_data="transcode_start")
        ]
    ]
    
    keyboard = InlineKeyboardMarkup(all_keyboards)
    
    await callback_query.edit_message_text(settings_text, reply_markup=keyboard)

async def show_main_transcode_menu(callback_query: CallbackQuery, user_id: int):
    """Show main transcode menu"""
    if user_id not in TRANSCODE_STATES:
        await callback_query.answer("❌ Session expired. Please run /transcode again.")
        return
    
    state = TRANSCODE_STATES[user_id]
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 Start Transcoding", callback_data="transcode_start"),
            InlineKeyboardButton("📋 Show Videos", callback_data="transcode_list")
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="transcode_settings"),
            InlineKeyboardButton("❌ Cancel", callback_data="transcode_cancel")
        ]
    ])
    
    menu_text = (
        f"🎬 **Video Transcoding**\n\n"
        f"📂 **Current folder:** {state['folder_name']}\n"
        f"🎥 **Video files found:** {len(state['video_files'])}\n\n"
        f"**Current settings:**\n"
        f"📤 Format: {state['format'].upper()}\n"
        f"🎯 Quality: {state['quality']}\n"
        f"⚡ Speed: {state['speed'].title()}\n\n"
        f"Choose an option below:"
    )
    
    await callback_query.edit_message_text(menu_text, reply_markup=keyboard)

async def start_bulk_transcode(callback_query: CallbackQuery, state: dict):
    """Start bulk transcoding of all video files"""
    video_files = state['video_files']
    format_name = state['format']
    quality = state['quality']
    speed = state['speed']
    folder_path = state['folder_path']
    
    # Create bulk transcode ID
    bulk_id = getRandomID()
    
    # Initialize progress tracking
    BULK_IMPORT_PROGRESS[bulk_id] = {
        'total': len(video_files),
        'completed': 0,
        'failed': 0,
        'current_file': '',
        'status': 'starting',
        'transcode_ids': []
    }
    
    await callback_query.edit_message_text(
        f"🎬 **Starting bulk transcoding...**\n\n"
        f"📊 **Total files:** {len(video_files)}\n"
        f"📤 **Format:** {format_name.upper()}\n"
        f"🎯 **Quality:** {quality}\n"
        f"⚡ **Speed:** {speed}\n\n"
        f"⏳ **Status:** Initializing..."
    )
    
    # Start transcoding each file
    asyncio.create_task(process_bulk_transcode(
        callback_query, bulk_id, video_files, format_name, quality, speed, folder_path
    ))

async def process_bulk_transcode(
    callback_query: CallbackQuery,
    bulk_id: str,
    video_files: list,
    format_name: str,
    quality: str,
    speed: str,
    folder_path: str
):
    """Process bulk transcoding of video files"""
    
    progress = BULK_IMPORT_PROGRESS[bulk_id]
    progress['status'] = 'processing'
    
    for i, video_file in enumerate(video_files):
        try:
            progress['current_file'] = video_file.name
            
            # Generate transcode ID
            transcode_id = getRandomID()
            progress['transcode_ids'].append(transcode_id)
            
            # Start transcoding
            file_path = video_file.path + '/' + video_file.id
            
            await start_video_transcode(
                file_path,
                video_file.file_id,
                format_name,
                quality,
                transcode_id,
                folder_path,
                video_file.name,
                {},
                speed
            )
            
            # Wait for completion or timeout
            timeout = 300  # 5 minutes timeout per file
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                transcode_progress = get_transcode_progress(transcode_id)
                
                if not transcode_progress:
                    await asyncio.sleep(5)
                    continue
                
                if transcode_progress.get('status') == 'completed':
                    progress['completed'] += 1
                    break
                elif transcode_progress.get('status') == 'error':
                    progress['failed'] += 1
                    break
                
                await asyncio.sleep(5)
            else:
                # Timeout
                progress['failed'] += 1
            
            # Update progress message
            await update_bulk_progress_message(callback_query, bulk_id, i + 1, len(video_files))
            
        except Exception as e:
            logger.error(f"Error transcoding {video_file.name}: {e}")
            progress['failed'] += 1
    
    # Final update
    progress['status'] = 'completed'
    await update_bulk_progress_message(callback_query, bulk_id, len(video_files), len(video_files), final=True)

async def update_bulk_progress_message(
    callback_query: CallbackQuery,
    bulk_id: str,
    current: int,
    total: int,
    final: bool = False
):
    """Update bulk transcoding progress message"""
    
    progress = BULK_IMPORT_PROGRESS.get(bulk_id, {})
    completed = progress.get('completed', 0)
    failed = progress.get('failed', 0)
    current_file = progress.get('current_file', '')
    
    progress_percent = (current / total) * 100 if total > 0 else 0
    
    if final:
        status_text = (
            f"✅ **Bulk transcoding completed!**\n\n"
            f"📊 **Results:**\n"
            f"✅ **Completed:** {completed}\n"
            f"❌ **Failed:** {failed}\n"
            f"📁 **Total:** {total}\n\n"
            f"🎉 **All files processed successfully!**"
        )
    else:
        status_text = (
            f"🎬 **Bulk transcoding in progress...**\n\n"
            f"📊 **Progress:** {current}/{total} ({progress_percent:.1f}%)\n"
            f"✅ **Completed:** {completed}\n"
            f"❌ **Failed:** {failed}\n\n"
            f"📄 **Current file:** {current_file[:30]}{'...' if len(current_file) > 30 else ''}"
        )
    
    try:
        await callback_query.edit_message_text(status_text)
    except Exception as e:
        logger.error(f"Error updating progress message: {e}")

def is_video_file(filename: str) -> bool:
    """Check if file is a video"""
    video_extensions = {'.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', 
                       '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg'}
    extension = os.path.splitext(filename.lower())[1]
    return extension in video_extensions

def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"

def format_duration(seconds: float) -> str:
    """Format duration in human readable format"""
    if not seconds or seconds == 0:
        return "0:00"
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"

def get_all_transcode_progress() -> dict:
    """Get all active transcode progress"""
    from utils.transcoder import TRANSCODE_PROGRESS
    return TRANSCODE_PROGRESS