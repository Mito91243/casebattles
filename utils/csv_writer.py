import asyncio
import csv
import aiofiles # Used for ASYNCHRONOUS file writing
from loguru import logger

# The fieldnames MUST match the keys returned by HypeDropSocket's parse_message
FIELDNAMES = [
    "game_id", 
    "display_name", 
    "user_id", 
    "total_bet", 
    "total_profit", 
    "payout", 
    "avatar"
]
OUTPUT_FILE = "game_results.csv"
BATCH_SIZE = 50  # How many rows to collect before writing to disk

async def csv_writer_worker(queue: asyncio.Queue):
    """
    Worker that consumes cleaned data from the queue and writes it 
    to a CSV file in batches.
    """
    logger.info("CSV Writer Worker started.")
    
    # Use a set to track written headers, ensuring we only write them once
    is_header_written = False
    
    # The batch list stores data before we flush it to the file
    current_batch = []
    
    # 1. Open the file asynchronously and keep it open
    async with aiofiles.open(OUTPUT_FILE, mode='a', newline='', encoding='utf-8') as afp:
        
        while True:
            # 2. Get data from the queue (blocking wait)
            data = await queue.get()
            
            # The HypeDropSocket returns {'event': 'game_finished', 'players': [...]}
            if data.get("event") == "game_finished":
                
                # 3. Add all human players from this game to the batch
                current_batch.extend(data["players"])
                
                # 4. Check if we have enough data to write a batch
                if len(current_batch) >= BATCH_SIZE:
                    
                    # Log the batch size before writing
                    logger.info(f"Writing batch of {len(current_batch)} records to CSV...")
                    
                    # 5. Non-blocking writing function
                    await write_batch_to_csv(afp, current_batch, is_header_written)
                    
                    # Mark header as written
                    is_header_written = True
                    
                    # Clear the batch after writing
                    current_batch = []
                    
            # Important: Tell the queue this item is processed
            queue.task_done()
            
            # This short sleep yields control back to the event loop, 
            # allowing other tasks (like the sockets) to run.
            await asyncio.sleep(0.001)


async def write_batch_to_csv(afp, batch_data, is_header_written):
    """Handles the actual asynchronous write operation."""
    
    # The standard 'csv' module is sync, but writing to the aiofiles 
    # file pointer (afp) is wrapped to be non-blocking.
    writer = csv.DictWriter(afp, fieldnames=FIELDNAMES, extrasaction='ignore')
    
    if not is_header_written:
        # csv.DictWriter's writeheader is a synchronous call.
        # We run it using aiofiles' wrapper to make it async.
        await afp.write(",".join(FIELDNAMES) + "\n")

    for row in batch_data:
        # csv.DictWriter's writerow is a synchronous call.
        # We run it using aiofiles' wrapper to make it async.
        # Note: We manually format the row since DictWriter is tricky with aiofiles
        line = ",".join(str(row.get(key, '')) for key in FIELDNAMES) + "\n"
        await afp.write(line)
        
    # Flush the data to disk without blocking
    await afp.flush()