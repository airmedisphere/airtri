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

# Enhanced client rotation and health tracking
client_last_used = {}
client_health = {}
client_performance = {}  # Track performance metrics


async def initialize_clients():
    global multi_clients, work_loads, premium_clients, premium_work_loads
    global client_last_used, client_health, client_performance
    
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
                client_performance[client_id] = {'success_count': 0, 'error_count': 0, 'avg_speed': 0}
                
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
                client_performance[client_id] = {'success_count': 0, 'error_count': 0, 'avg_speed': 0}

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
    
    # Start enhanced client health monitoring
    asyncio.create_task(monitor_client_health())


async def monitor_client_health():
    """Enhanced client health monitoring with performance tracking"""
    global work_loads, premium_work_loads, client_health, client_performance
    
    while True:
        try:
            current_time = time.time()
            
            # Reset work loads periodically and update performance metrics
            for client_id in work_loads:
                if current_time - client_last_used.get(client_id, 0) > 180:  # 3 minutes
                    work_loads[client_id] = max(0, work_loads[client_id] - 1)
                    
                # Reset health if client has been idle and had errors
                if (current_time - client_last_used.get(client_id, 0) > 300 and 
                    not client_health.get(client_id, True)):
                    client_health[client_id] = True
                    logger.info(f"Reset health status for client {client_id}")
                    
            for client_id in premium_work_loads:
                if current_time - client_last_used.get(client_id, 0) > 180:
                    premium_work_loads[client_id] = max(0, premium_work_loads[client_id] - 1)
                    
                if (current_time - client_last_used.get(client_id, 0) > 300 and 
                    not client_health.get(client_id, True)):
                    client_health[client_id] = True
                    logger.info(f"Reset health status for premium client {client_id}")
            
            # Check for overloaded clients and reset if needed
            for client_id, load in work_loads.items():
                if load > 15:  # Increased threshold
                    logger.warning(f"Client {client_id} is overloaded with {load} operations")
                    work_loads[client_id] = 3  # Reset to low load
                    
            for client_id, load in premium_work_loads.items():
                if load > 15:
                    logger.warning(f"Premium client {client_id} is overloaded with {load} operations")
                    premium_work_loads[client_id] = 3
                    
            # Log performance statistics every 10 minutes
            if int(current_time) % 600 == 0:
                log_performance_stats()
                    
        except Exception as e:
            logger.error(f"Error in client health monitoring: {e}")
            
        await asyncio.sleep(30)  # Check every 30 seconds


def log_performance_stats():
    """Log client performance statistics"""
    logger.info("=== Client Performance Stats ===")
    
    for client_id, perf in client_performance.items():
        if perf['success_count'] + perf['error_count'] > 0:
            success_rate = (perf['success_count'] / (perf['success_count'] + perf['error_count'])) * 100
            client_type = "Premium" if client_id in premium_clients else "Regular"
            logger.info(f"{client_type} Client {client_id}: {success_rate:.1f}% success rate, "
                       f"{perf['success_count']} successes, {perf['error_count']} errors")


def get_client(premium_required=False) -> Client:
    """Enhanced client selection with performance-based routing"""
    global multi_clients, work_loads, premium_clients, premium_work_loads
    global client_last_used, client_health, client_performance

    current_time = time.time()
    
    if premium_required:
        if not premium_clients:
            # Fallback to regular clients if no premium clients available
            logger.warning("No premium clients available, falling back to regular clients")
            premium_required = False
        else:
            # Find the best performing healthy premium client
            available_clients = {
                client_id: {
                    'load': load,
                    'performance': client_performance.get(client_id, {'success_count': 0, 'error_count': 0}),
                    'last_used': client_last_used.get(client_id, 0)
                }
                for client_id, load in premium_work_loads.items()
                if client_health.get(client_id, True)
            }
            
            if not available_clients:
                # Reset all premium clients if none are healthy
                for client_id in premium_work_loads:
                    client_health[client_id] = True
                available_clients = {
                    client_id: {
                        'load': load,
                        'performance': client_performance.get(client_id, {'success_count': 0, 'error_count': 0}),
                        'last_used': client_last_used.get(client_id, 0)
                    }
                    for client_id, load in premium_work_loads.items()
                }
            
            # Select client based on load and performance
            best_client_id = select_best_client(available_clients)
            premium_work_loads[best_client_id] += 1
            client_last_used[best_client_id] = current_time
            return premium_clients[best_client_id]

    if not premium_required:
        # Find the best performing healthy regular client
        available_clients = {
            client_id: {
                'load': load,
                'performance': client_performance.get(client_id, {'success_count': 0, 'error_count': 0}),
                'last_used': client_last_used.get(client_id, 0)
            }
            for client_id, load in work_loads.items()
            if client_health.get(client_id, True)
        }
        
        if not available_clients:
            # Reset all clients if none are healthy
            for client_id in work_loads:
                client_health[client_id] = True
            available_clients = {
                client_id: {
                    'load': load,
                    'performance': client_performance.get(client_id, {'success_count': 0, 'error_count': 0}),
                    'last_used': client_last_used.get(client_id, 0)
                }
                for client_id, load in work_loads.items()
            }
        
        best_client_id = select_best_client(available_clients)
        work_loads[best_client_id] += 1
        client_last_used[best_client_id] = current_time
        return multi_clients[best_client_id]


def select_best_client(available_clients):
    """Select the best client based on load, performance, and last used time"""
    if not available_clients:
        return None
    
    # Score each client
    scored_clients = []
    current_time = time.time()
    
    for client_id, data in available_clients.items():
        load = data['load']
        perf = data['performance']
        last_used = data['last_used']
        
        # Calculate performance score (higher is better)
        total_ops = perf['success_count'] + perf['error_count']
        if total_ops > 0:
            success_rate = perf['success_count'] / total_ops
        else:
            success_rate = 1.0  # New client, assume good
        
        # Calculate recency score (prefer recently used clients, but not overloaded ones)
        time_since_use = current_time - last_used
        recency_score = min(time_since_use / 300, 1.0)  # Normalize to 5 minutes
        
        # Combined score (lower is better)
        # Prioritize: low load > high success rate > recent use
        score = (load * 2) + ((1 - success_rate) * 3) + (recency_score * 0.5)
        
        scored_clients.append((client_id, score))
    
    # Sort by score and return the best client
    scored_clients.sort(key=lambda x: x[1])
    return scored_clients[0][0]


def release_client(client: Client, premium_required=False):
    """Release a client after use and update performance metrics"""
    global work_loads, premium_work_loads, client_performance
    
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


def update_client_performance(client: Client, success: bool, premium_required=False):
    """Update client performance metrics"""
    global client_performance, client_health
    
    try:
        client_id = None
        
        if premium_required:
            for cid, c in premium_clients.items():
                if c == client:
                    client_id = cid
                    break
        else:
            for cid, c in multi_clients.items():
                if c == client:
                    client_id = cid
                    break
        
        if client_id:
            if client_id not in client_performance:
                client_performance[client_id] = {'success_count': 0, 'error_count': 0, 'avg_speed': 0}
            
            if success:
                client_performance[client_id]['success_count'] += 1
                client_health[client_id] = True
            else:
                client_performance[client_id]['error_count'] += 1
                # Mark as unhealthy if too many consecutive errors
                total_ops = client_performance[client_id]['success_count'] + client_performance[client_id]['error_count']
                if total_ops > 5:  # Only after some operations
                    error_rate = client_performance[client_id]['error_count'] / total_ops
                    if error_rate > 0.5:  # More than 50% error rate
                        client_health[client_id] = False
                        logger.warning(f"Marked client {client_id} as unhealthy due to high error rate")
                        
    except Exception as e:
        logger.error(f"Error updating client performance: {e}")


# Enhanced client management functions
async def get_client_stats():
    """Get detailed client statistics"""
    stats = {
        'regular_clients': len(multi_clients),
        'premium_clients': len(premium_clients),
        'total_load': sum(work_loads.values()) + sum(premium_work_loads.values()),
        'healthy_clients': sum(1 for h in client_health.values() if h),
        'performance': client_performance.copy()
    }
    return stats


async def reset_all_clients():
    """Reset all client health and load states"""
    global client_health, work_loads, premium_work_loads
    
    for client_id in multi_clients:
        client_health[client_id] = True
        work_loads[client_id] = 0
        
    for client_id in premium_clients:
        client_health[client_id] = True
        premium_work_loads[client_id] = 0
        
    logger.info("Reset all client health and load states")