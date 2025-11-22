import asyncio
import json
import websockets
from abc import ABC, abstractmethod
from loguru import logger

class BaseSocket(ABC):

    def __init__(self, url: str, queue: asyncio.Queue, source_name: str):
        self.url = url              # The server address (e.g., wss://stream.site.com/...)
        self.queue = queue          # The shared queue to push data to workers
        self.source_name = source_name # A unique name for logging (e.g., "Binance")
        self.is_running = False     # Flag to control the main loop
        self.reconnect_delay = 2    # Initial wait time for retries
        self.connection_headers = {}  # Default empty, child classes can override
        
    async def connect_and_listen(self):
        self.is_running = True
        
        while self.is_running:
            try:
                logger.info(f"[{self.source_name}] Connecting to {self.url}...")
                
                # Use additional_headers instead of extra_headers
                connect_kwargs = {
                    "subprotocols": ["graphql-transport-ws"]
                }
                
                # Only add headers if they exist
                if self.connection_headers:
                    connect_kwargs["additional_headers"] = self.connection_headers
                
                async with websockets.connect(self.url, **connect_kwargs) as websocket:
                    logger.success(f"[{self.source_name}] Connected!")
                    self.reconnect_delay = 2                    
                    await self.on_open(websocket)

                    async for message in websocket:
                        if not self.is_running:
                            break
                        
                        data = await self.parse_message(message)
                        
                        if data:
                            await self.queue.put(data)

            except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
                logger.warning(f"[{self.source_name}] Connection lost: {e}. Retrying in {self.reconnect_delay}s...")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, 60)
            
            except Exception as e:
                logger.error(f"[{self.source_name}] Critical Error: {e}")
                await asyncio.sleep(5)

    @abstractmethod
    async def parse_message(self, message) -> dict:
        pass

    async def on_open(self, websocket):
        pass