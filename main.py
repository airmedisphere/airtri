from utils.downloader import (
    download_file,
    get_file_info_from_url,
)
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
import aiofiles
from fastapi import FastAPI, HTTPException, Request, File, UploadFile, Form, Response
from fastapi.responses import FileResponse, JSONResponse
from config import ADMIN_PASSWORD, MAX_FILE_SIZE, STORAGE_CHANNEL
from utils.clients import initialize_clients
from utils.directoryHandler import getRandomID
from utils.extra import auto_ping_website, convert_class_to_dict, reset_cache_dir
from utils.streamer import media_streamer
from utils.uploader import start_file_uploader
from utils.logger import Logger
from utils.transcoder import (
    transcoder, 
    start_video_transcode, 
    get_transcode_progress, 
    cancel_transcode,
    TRANSCODE_PROGRESS
)
import urllib.parse


# Startup Event
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Reset the cache directory, delete cache files
    reset_cache_dir()

    # Initialize the clients
    await initialize_clients()

    # Start the website auto ping task
    asyncio.create_task(auto_ping_website())

    yield


app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)
logger = Logger(__name__)


@app.get("/")
async def home_page():
    return FileResponse("website/home.html")


@app.get("/stream")
async def stream_page():
    return FileResponse("website/VideoPlayer.html")


@app.get("/pdf-viewer")
async def pdf_viewer_page():
    return FileResponse("website/PDFViewer.html")


@app.get("/static/{file_path:path}")
async def static_files(file_path):
    if "apiHandler.js" in file_path:
        with open(Path("website/static/js/apiHandler.js")) as f:
            content = f.read()
            content = content.replace("MAX_FILE_SIZE__SDGJDG", str(MAX_FILE_SIZE))
        return Response(content=content, media_type="application/javascript")
    return FileResponse(f"website/static/{file_path}")


@app.get("/file")
async def dl_file(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    path = request.query_params["path"]
    file = DRIVE_DATA.get_file(path)
    return await media_streamer(STORAGE_CHANNEL, file.file_id, file.name, request)


# Api Routes


@app.post("/api/checkPassword")
async def check_password(request: Request):
    data = await request.json()
    if data["pass"] == ADMIN_PASSWORD:
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "Invalid password"})


@app.post("/api/createNewFolder")
async def api_new_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"createNewFolder {data}")
    folder_data = DRIVE_DATA.get_directory(data["path"]).contents
    for id in folder_data:
        f = folder_data[id]
        if f.type == "folder":
            if f.name == data["name"]:
                return JSONResponse(
                    {
                        "status": "Folder with the name already exist in current directory"
                    }
                )

    DRIVE_DATA.new_folder(data["path"], data["name"])
    return JSONResponse({"status": "ok"})


@app.post("/api/getDirectory")
async def api_get_directory(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] == ADMIN_PASSWORD:
        is_admin = True
    else:
        is_admin = False

    auth = data.get("auth")
    sort_by = data.get("sort_by", "date")  # name, date, size
    sort_order = data.get("sort_order", "desc")  # asc, desc

    logger.info(f"getFolder {data}")

    if data["path"] == "/trash":
        data = {"contents": DRIVE_DATA.get_trashed_files_folders()}
        folder_data = convert_class_to_dict(data, isObject=False, showtrash=True, sort_by=sort_by, sort_order=sort_order)

    elif "/search_" in data["path"]:
        query = urllib.parse.unquote(data["path"].split("_", 1)[1])
        print(query)
        data = {"contents": DRIVE_DATA.search_file_folder(query)}
        print(data)
        folder_data = convert_class_to_dict(data, isObject=False, showtrash=False, sort_by=sort_by, sort_order=sort_order)
        print(folder_data)

    elif "/share_" in data["path"]:
        path = data["path"].split("_", 1)[1]
        folder_data, auth_home_path = DRIVE_DATA.get_directory(path, is_admin, auth)
        auth_home_path= auth_home_path.replace("//", "/") if auth_home_path else None
        folder_data = convert_class_to_dict(folder_data, isObject=True, showtrash=False, sort_by=sort_by, sort_order=sort_order)
        return JSONResponse(
            {"status": "ok", "data": folder_data, "auth_home_path": auth_home_path}
        )

    else:
        folder_data = DRIVE_DATA.get_directory(data["path"])
        folder_data = convert_class_to_dict(folder_data, isObject=True, showtrash=False, sort_by=sort_by, sort_order=sort_order)
    return JSONResponse({"status": "ok", "data": folder_data, "auth_home_path": None})


SAVE_PROGRESS = {}


@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    path: str = Form(...),
    password: str = Form(...),
    id: str = Form(...),
    total_size: str = Form(...),
):
    global SAVE_PROGRESS

    if password != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    total_size = int(total_size)
    SAVE_PROGRESS[id] = ("running", 0, total_size)

    ext = file.filename.lower().split(".")[-1]

    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    file_location = cache_dir / f"{id}.{ext}"

    file_size = 0

    try:
        async with aiofiles.open(file_location, "wb") as buffer:
            while chunk := await file.read(1024 * 1024):  # Read file in chunks of 1MB
                SAVE_PROGRESS[id] = ("running", file_size, total_size)
                file_size += len(chunk)
                if file_size > MAX_FILE_SIZE:
                    await buffer.close()
                    file_location.unlink()  # Delete the partially written file
                    raise HTTPException(
                        status_code=400,
                        detail=f"File size exceeds {MAX_FILE_SIZE} bytes limit",
                    )
                await buffer.write(chunk)

        SAVE_PROGRESS[id] = ("completed", file_size, file_size)

        # Start upload with improved error handling
        asyncio.create_task(
            start_file_uploader(file_location, id, path, file.filename, file_size)
        )

        return JSONResponse({"id": id, "status": "ok"})
        
    except Exception as e:
        SAVE_PROGRESS[id] = ("error", file_size, total_size)
        logger.error(f"Upload error for {file.filename}: {e}")
        
        # Clean up file on error
        try:
            if file_location.exists():
                file_location.unlink()
        except:
            pass
            
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/getSaveProgress")
async def get_save_progress(request: Request):
    global SAVE_PROGRESS

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getUploadProgress {data}")
    try:
        progress = SAVE_PROGRESS[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})


@app.post("/api/getUploadProgress")
async def get_upload_progress(request: Request):
    from utils.uploader import PROGRESS_CACHE

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getUploadProgress {data}")

    try:
        progress = PROGRESS_CACHE[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})


@app.post("/api/cancelUpload")
async def cancel_upload(request: Request):
    from utils.uploader import STOP_TRANSMISSION
    from utils.downloader import STOP_DOWNLOAD

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"cancelUpload {data}")
    STOP_TRANSMISSION.append(data["id"])
    STOP_DOWNLOAD.append(data["id"])
    return JSONResponse({"status": "ok"})


# Enhanced API endpoint for bulk import progress with better tracking
@app.post("/api/getBulkImportProgress")
async def get_bulk_import_progress(request: Request):
    from utils.bot_mode import BULK_IMPORT_PROGRESS

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        import_id = data["id"]
        progress = BULK_IMPORT_PROGRESS.get(import_id)
        if progress:
            # Enhanced progress data with more details
            enhanced_progress = {
                'total': progress['total'],
                'imported': progress['imported'],
                'skipped': progress['skipped'],
                'errors': progress['errors'],
                'current_message_id': progress.get('current_message_id', 0),
                'status': progress['status'],
                'type': progress.get('type', 'bulk'),  # 'bulk' or 'fast'
                'progress_text': f"{progress['imported'] + progress['skipped'] + progress['errors']}/{progress['total']}",
                'percentage': ((progress['imported'] + progress['skipped'] + progress['errors']) / progress['total'] * 100) if progress['total'] > 0 else 0
            }
            return JSONResponse({"status": "ok", "data": enhanced_progress})
        else:
            return JSONResponse({"status": "not found"})
    except Exception as e:
        logger.error(f"Error getting bulk import progress: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


@app.post("/api/renameFileFolder")
async def rename_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"renameFileFolder {data}")
    DRIVE_DATA.rename_file_folder(data["path"], data["name"])
    return JSONResponse({"status": "ok"})


@app.post("/api/trashFileFolder")
async def trash_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"trashFileFolder {data}")
    DRIVE_DATA.trash_file_folder(data["path"], data["trash"])
    return JSONResponse({"status": "ok"})


@app.post("/api/deleteFileFolder")
async def delete_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"deleteFileFolder {data}")
    DRIVE_DATA.delete_file_folder(data["path"])
    return JSONResponse({"status": "ok"})


@app.post("/api/moveFileFolder")
async def move_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"moveFileFolder {data}")
    try:
        DRIVE_DATA.move_file_folder(data["source_path"], data["destination_path"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/copyFileFolder")
async def copy_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"copyFileFolder {data}")
    try:
        DRIVE_DATA.copy_file_folder(data["source_path"], data["destination_path"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getFolderTree")
async def get_folder_tree(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getFolderTree {data}")
    try:
        folder_tree = DRIVE_DATA.get_folder_tree()
        return JSONResponse({"status": "ok", "data": folder_tree})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getFileInfoFromUrl")
async def getFileInfoFromUrl(request: Request):

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getFileInfoFromUrl {data}")
    try:
        file_info = await get_file_info_from_url(data["url"])
        return JSONResponse({"status": "ok", "data": file_info})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/startFileDownloadFromUrl")
async def startFileDownloadFromUrl(request: Request):
    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"startFileDownloadFromUrl {data}")
    try:
        id = getRandomID()
        asyncio.create_task(
            download_file(data["url"], id, data["path"], data["filename"], data["singleThreaded"])
        )
        return JSONResponse({"status": "ok", "id": id})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getFileDownloadProgress")
async def getFileDownloadProgress(request: Request):
    from utils.downloader import DOWNLOAD_PROGRESS

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getFileDownloadProgress {data}")

    try:
        progress = DOWNLOAD_PROGRESS[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})


@app.post("/api/getFolderShareAuth")
async def getFolderShareAuth(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getFolderShareAuth {data}")

    try:
        auth = DRIVE_DATA.get_folder_auth(data["path"])
        return JSONResponse({"status": "ok", "auth": auth})
    except:
        return JSONResponse({"status": "not found"})


# New API endpoint for client statistics
@app.post("/api/getClientStats")
async def get_client_stats(request: Request):
    from utils.clients import get_client_stats

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        stats = await get_client_stats()
        return JSONResponse({"status": "ok", "data": stats})
    except Exception as e:
        logger.error(f"Error getting client stats: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


# New API endpoint for streaming optimization
@app.post("/api/getStreamingStats")
async def get_streaming_stats(request: Request):
    """Get streaming performance statistics"""
    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        # Get connection info from request headers
        connection_type = request.headers.get("Connection-Type", "unknown")
        save_data = request.headers.get("Save-Data", "off") == "on"
        user_agent = request.headers.get("User-Agent", "")
        
        # Determine if mobile
        is_mobile = any(mobile in user_agent.lower() for mobile in ['mobile', 'android', 'iphone', 'ipad'])
        
        # Recommend quality based on connection
        recommended_quality = "medium"
        if save_data or "slow" in connection_type.lower():
            recommended_quality = "low"
        elif "fast" in connection_type.lower() and not is_mobile:
            recommended_quality = "high"
        
        stats = {
            "recommended_quality": recommended_quality,
            "is_mobile": is_mobile,
            "save_data_enabled": save_data,
            "connection_type": connection_type
        }
        
        return JSONResponse({"status": "ok", "data": stats})
    except Exception as e:
        logger.error(f"Error getting streaming stats: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


# Transcoding API endpoints
@app.post("/api/getTranscodeFormats")
async def get_transcode_formats(request: Request):
    """Get supported transcoding formats"""
    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        formats = transcoder.get_supported_formats()
        qualities = transcoder.get_quality_presets()
        speed_presets = transcoder.get_speed_presets()
        
        return JSONResponse({
            "status": "ok", 
            "data": {
                "formats": formats,
                "qualities": qualities,
                "speed_presets": speed_presets
            }
        })
    except Exception as e:
        logger.error(f"Error getting transcode formats: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


@app.post("/api/getVideoInfo")
async def get_video_info(request: Request):
    """Get video file information"""
    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        from utils.directoryHandler import DRIVE_DATA
        from utils.clients import get_client
        
        file_path = data["file_path"]
        file = DRIVE_DATA.get_file(file_path)
        
        # Download file temporarily to get info
        client = get_client()
        message = await client.get_messages(STORAGE_CHANNEL, int(file.file_id))
        
        if not message or (not message.video and not message.document):
            return JSONResponse({"status": "error", "message": "File not found or not a video"})
        
        # Download to temporary location
        temp_file = await message.download(file_name=f"temp_info_{getRandomID()}")
        
        try:
            # Get video information
            video_info = await transcoder.get_video_info(temp_file)
            return JSONResponse({"status": "ok", "data": video_info})
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file):
                os.unlink(temp_file)
                
    except Exception as e:
        logger.error(f"Error getting video info: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


@app.post("/api/startTranscode")
async def start_transcode(request: Request):
    """Start video transcoding"""
    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        from utils.directoryHandler import DRIVE_DATA
        
        file_path = data["file_path"]
        output_format = data["output_format"]
        quality = data["quality"]
        speed_preset = data.get("speed_preset", "fast")
        custom_settings = data.get("custom_settings", {})
        
        # Get file information
        file = DRIVE_DATA.get_file(file_path)
        
        # Generate transcode ID
        transcode_id = getRandomID()
        
        # Start transcoding process
        asyncio.create_task(
            start_video_transcode(
                file_path,
                file.file_id,
                output_format,
                quality,
                transcode_id,
                file.path,
                file.name,
                custom_settings,
                speed_preset
            )
        )
        
        return JSONResponse({"status": "ok", "transcode_id": transcode_id})
        
    except Exception as e:
        logger.error(f"Error starting transcode: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


@app.post("/api/getTranscodeProgress")
async def get_transcode_progress_api(request: Request):
    """Get transcoding progress"""
    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        transcode_id = data["transcode_id"]
        progress = get_transcode_progress(transcode_id)
        
        if progress:
            return JSONResponse({"status": "ok", "data": progress})
        else:
            return JSONResponse({"status": "not found"})
            
    except Exception as e:
        logger.error(f"Error getting transcode progress: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


@app.post("/api/cancelTranscode")
async def cancel_transcode_api(request: Request):
    """Cancel transcoding"""
    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        transcode_id = data["transcode_id"]
        cancel_transcode(transcode_id)
        
        return JSONResponse({"status": "ok"})
        
    except Exception as e:
        logger.error(f"Error cancelling transcode: {e}")
        return JSONResponse({"status": "error", "message": str(e)})