import asyncio
import csv
import aiofiles
from loguru import logger

# Updated fieldnames to match the new normalized format
FIELDNAMES = [
    "external_id",
    "username",
    "profile_url",
    "level",
    "avatar_url",
    "date",
    "total_bet",
    "total_profit",
    "total_payout"
]
OUTPUT_FILE = "game_results.csv"
BATCH_SIZE = 50

async def csv_writer_worker(queue: asyncio.Queue):

    logger.info("CSV Writer Worker started.")
    
    is_header_written = False
    current_batch = []
    
    async with aiofiles.open(OUTPUT_FILE, mode='a', newline='', encoding='utf-8') as afp:
        
        while True:
            # Get data from the queue (now it's a list of players directly)
            players = await queue.get()
            
            # Check if we got valid data (list of players)
            if players and isinstance(players, list):
                
                # Add all players from this game to the batch
                current_batch.extend(players)
                
                # Check if we have enough data to write a batch
                if len(current_batch) >= BATCH_SIZE:
                    
                    logger.info(f"Writing batch of {len(current_batch)} records to CSV...")
                    
                    await write_batch_to_csv(afp, current_batch, is_header_written)
                    
                    is_header_written = True
                    current_batch = []
                    
            queue.task_done()
            await asyncio.sleep(0.001)


async def write_batch_to_csv(afp, batch_data, is_header_written):    
    if not is_header_written:
        await afp.write(",".join(FIELDNAMES) + "\n")

    for row in batch_data:
        # Handle None values and escape commas in strings
        values = []
        for key in FIELDNAMES:
            value = row.get(key, '')
            # Convert None to empty string
            if value is None:
                value = ''
            # Escape commas and quotes in string values
            value_str = str(value)
            if ',' in value_str or '"' in value_str:
                value_str = f'"{value_str.replace("\"", "\"\"")}"'
            values.append(value_str)
        
        line = ",".join(values) + "\n"
        await afp.write(line)
        
    await afp.flush()