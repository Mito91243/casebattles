import asyncio
import sys
from loguru import logger
from sockets.hypedrop import HypeDropSocket
from db.database import DatabaseManager
from db.db_writer import DBWriter
from config.config import Config

# ⚠️ CRITICAL: Must be set BEFORE any asyncio calls
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def console_printer(queue: asyncio.Queue):
    """Optional: Console printer for debugging"""
    logger.info("🖥️ Console Printer started...")
    
    while True:
        data = await queue.get()
        
        if isinstance(data, list) and len(data) > 0:
            print("\n" + "="*50)
            print(f"🎰 GAME FINISHED ({len(data)} Players)")
            print("="*50)
            
            for p in data:
                print(f"👤 {p['username']} | 💰 ${p['total_bet']}")
        
        queue.task_done()


async def main():
    # 1. Initialize Database with config from environment
    db_manager = DatabaseManager(Config.get_db_config())
    await db_manager.initialize()
    
    # 2. Create Shared Queue
    data_queue = asyncio.Queue()
    
    # 3. Initialize Sockets
    hypedrop = HypeDropSocket(data_queue)
    
    # 4. Initialize DB Writer
    db_writer = DBWriter(
        db_manager=db_manager,
        backup_file=Config.BACKUP_FILE_PATH
    )
    
    # 5. Create Tasks
    socket_tasks = [
        asyncio.create_task(hypedrop.connect_and_listen(), name="HypeDrop")
    ]
    
    db_task = asyncio.create_task(
        db_writer.process_queue(data_queue), 
        name="DBWriter"
    )
    
    try:
        logger.info(f"🚀 Starting application in {Config.ENVIRONMENT} mode")

        # Run forever
        # NOTE: console_printer is disabled to avoid race condition with db_writer
        # Both would consume from the same queue. Enable only for debugging.
        await asyncio.gather(
            *socket_tasks,
            db_task,
            # console_printer(queue=data_queue)  # Disabled - causes race condition
        )
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down gracefully...")
    finally:
        # Cleanup
        await db_manager.close()
        logger.info("👋 Application stopped")


if __name__ == "__main__":
    # Configure logger
    logger.remove()
    logger.add(
        lambda msg: print(msg, end=""),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level='INFO'
    )
    
    asyncio.run(main())