import asyncio, config
from pathlib import Path
from pyrogram import Client
from utils.directoryHandler import backup_drive_data, loadDriveData
from utils.logger import Logger
import os
import signal
import time

logger = Logger(__name__)

multi_clients = {}
premium_clients = {}
work_loads = {}
premium_work_loads = {}
main_bot = None

# Add client rotation and health tracking
client_last_used = {}
client_health = {}


async def initialize_clients():
    global multi_clients, work_loads, premium_clients, premium_work_loads
    global client_last_used, client_health
    
    logger.info("Initializing Clients")

    session_cache_path = Path(f"./cache")
    session_cache_path.parent.mkdir(parents=True, exist_ok=True)

    all_tokens = dict((i, t) for i, t in enumerate(config.BOT_TOKENS, start=1))
    all_sessions = dict(
        (i, s) for i, s in enumerate(config.STRING_SESSIONS, start=len(all_tokens) + 1)
    )

    async def start_client(client_id, token, type):
        try:
            logger.info(f"Starting - {type.title()} Client {client_id}")

            if type == "bot":
                client = Client(
                    name=str(client_id),
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    bot_token=token,
                    workdir=session_cache_path,
                    sleep_threshold=config.SLEEP_THRESHOLD,
                )
                client.loop = asyncio.get_running_loop()
                await client.start()
                await client.send_message(
                    config.STORAGE_CHANNEL,
                    f"Started - {type.title()} Client {client_id}",
                )
                multi_clients[client_id] = client
                work_loads[client_id] = 0
                client_last_used[client_id] = 0
                client_health[client_id] = True
                
            elif type == "user":
                client = await Client(
                    name=str(client_id),
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    session_string=token,
                    sleep_threshold=config.SLEEP_THRESHOLD,
                    workdir=session_cache_path,
                    no_updates=True,
                ).start()
                await client.send_message(
                    config.STORAGE_CHANNEL,
                    f"Started - {type.title()} Client {client_id}",
                )
                premium_clients[client_id] = client
                premium_work_loads[client_id] = 0
                client_last_used[client_id] = 0
                client_health[client_id] = True

            logger.info(f"Started - {type.title()} Client {client_id}")
        except Exception as e:
            logger.error(
                f"Failed To Start {type.title()} Client - {client_id} Error: {e}"
            )

    await asyncio.gather(
        *(
            [
                start_client(client_id, client, "bot")
                for client_id, client in all_tokens.items()
            ]
            + [
                start_client(client_id, client, "user")
                for client_id, client in all_sessions.items()
            ]
        )
    )
    if len(multi_clients) == 0:
        logger.error("No Clients Were Initialized")

        # Forcefully terminates the program immediately
        os.kill(os.getpid(), signal.SIGKILL)

    if len(premium_clients) == 0:
        logger.info("No Premium Clients Were Initialized")

    logger.info("Clients Initialized")

    # Load the drive data
    await loadDriveData()

    # Start the backup drive data task
    asyncio.create_task(backup_drive_data())
    
    # Start client health monitoring
    asyncio.create_task(monitor_client_health())


async def monitor_client_health():
    """Monitor client health and reset overloaded clients"""
    global work_loads, premium_work_loads, client_health
    
    while True:
        try:
            current_time = time.time()
            
            # Reset work loads periodically to prevent accumulation
            for client_id in work_loads:
                if current_time - client_last_used.get(client_id, 0) > 300:  # 5 minutes
                    work_loads[client_id] = max(0, work_loads[client_id] - 1)
                    
            for client_id in premium_work_loads:
                if current_time - client_last_used.get(client_id, 0) > 300:  # 5 minutes
                    premium_work_loads[client_id] = max(0, premium_work_loads[client_id] - 1)
            
            # Check for overloaded clients
            for client_id, load in work_loads.items():
                if load > 10:  # If client has more than 10 active operations
                    logger.warning(f"Client {client_id} is overloaded with {load} operations")
                    work_loads[client_id] = 5  # Reset to moderate load
                    
            for client_id, load in premium_work_loads.items():
                if load > 10:
                    logger.warning(f"Premium client {client_id} is overloaded with {load} operations")
                    premium_work_loads[client_id] = 5
                    
        except Exception as e:
            logger.error(f"Error in client health monitoring: {e}")
            
        await asyncio.sleep(60)  # Check every minute


def get_client(premium_required=False) -> Client:
    global multi_clients, work_loads, premium_clients, premium_work_loads
    global client_last_used, client_health

    current_time = time.time()
    
    if premium_required:
        if not premium_clients:
            # Fallback to regular clients if no premium clients available
            logger.warning("No premium clients available, falling back to regular clients")
            premium_required = False
        else:
            # Find the least loaded healthy premium client
            available_clients = {
                client_id: load for client_id, load in premium_work_loads.items()
                if client_health.get(client_id, True)
            }
            
            if not available_clients:
                # Reset all premium clients if none are healthy
                for client_id in premium_work_loads:
                    client_health[client_id] = True
                available_clients = premium_work_loads
            
            index = min(available_clients, key=available_clients.get)
            premium_work_loads[index] += 1
            client_last_used[index] = current_time
            return premium_clients[index]

    if not premium_required:
        # Find the least loaded healthy regular client
        available_clients = {
            client_id: load for client_id, load in work_loads.items()
            if client_health.get(client_id, True)
        }
        
        if not available_clients:
            # Reset all clients if none are healthy
            for client_id in work_loads:
                client_health[client_id] = True
            available_clients = work_loads
        
        index = min(available_clients, key=available_clients.get)
        work_loads[index] += 1
        client_last_used[index] = current_time
        return multi_clients[index]


def release_client(client: Client, premium_required=False):
    """Release a client after use to reduce its load count"""
    global work_loads, premium_work_loads
    
    try:
        if premium_required:
            for client_id, c in premium_clients.items():
                if c == client:
                    premium_work_loads[client_id] = max(0, premium_work_loads[client_id] - 1)
                    break
        else:
            for client_id, c in multi_clients.items():
                if c == client:
                    work_loads[client_id] = max(0, work_loads[client_id] - 1)
                    break
    except Exception as e:
        logger.error(f"Error releasing client: {e}")