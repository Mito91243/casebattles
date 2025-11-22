import asyncio
from loguru import logger
from sockets.hypedrop import HypeDropSocket
from utils.csv_writer import csv_writer_worker

async def console_printer(queue: asyncio.Queue):
    logger.info("Printer Worker started... waiting for games to finish.")
    
    while True:
        data = await queue.get()
        
        # 2. Process the data
        if data.get("event") == "game_finished":
            players = data["players"]
            
            print("\n" + "="*50)
            print(f"🎰 GAME FINISHED (Found {len(players)} Human Players)")
            print("="*50)
            
            for p in players:
                print(f"👤 Player: {p['display_name']}")
                print(f"🆔 ID:     {p['user_id']}")
                print(f"💰 Bet:    ${p['total_bet']}")
                print(f"📈 Profit: ${p['total_profit']}")
                print(f"🖼️ Avatar: {p['avatar']}")
                print("-" * 20)
        
        queue.task_done()

async def main():
    # 1. Create the shared queue
    # The socket puts data in here, the printer pulls it out.
    data_queue = asyncio.Queue()

    # 2. Initialize the Socket
    hypedrop = HypeDropSocket(data_queue)

    # 3. Create Tasks (Run them in the background)
    # Task A: Run the Socket (Connects, listens, handles reconnects)
    socket_task = asyncio.create_task(hypedrop.connect_and_listen())
    
    # Task B: Run the Printer (Reads queue, prints to console)
    printer_task = asyncio.create_task(console_printer(data_queue))

    # Task C: Run the CSV Writer (Consumer)
    csv_task = asyncio.create_task(csv_writer_worker(data_queue))
    # asyncio.gather will wait for these tasks (which run forever)
    await asyncio.gather(socket_task ,csv_task ,printer_task)

if __name__ == "__main__":
    try:
        # This is the entry point
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping...")