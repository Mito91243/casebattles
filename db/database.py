import asyncmy
import asyncio
from loguru import logger
from contextlib import asynccontextmanager
from typing import Optional

class DatabaseManager:
    def __init__(self, config: dict):
        self.config = config
        self.pool: Optional[asyncmy.Pool] = None
        
    async def _test_connection(self, config: dict) -> bool:
        """Test a single connection before creating the pool"""
        conn = None
        try:
            logger.info("🔍 Testing database connection...")
            logger.info(f"📡 Connecting to {config['host']}:{config['port']}")
            conn = await asyncio.wait_for(
                asyncmy.connect(
                    host=config['host'],
                    port=config['port'],
                    user=config['user'],
                    password=config['password'],
                    db=config['database'],
                    autocommit=False,
                    connect_timeout=10,
                    ssl=config.get('sslmode') == 'REQUIRED'
                ),
                timeout=15.0  # Total timeout including SSL handshake
            )
            
            # Test the connection with a simple query
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                await cursor.fetchone()
            
            logger.success("✅ Connection test successful!")
            return True
            
        except asyncio.TimeoutError:
            logger.error("❌ Connection test timed out after 15 seconds")
            return False
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False
        finally:
            # Ensure connection is properly closed
            if conn:
                try:
                    await conn.ensure_closed()
                except:
                    pass
    
    async def initialize(self):
        """Create connection pool on startup"""
        try:
            logger.info("📡 Attempting to connect to database...")
            
            # Prepare connection configuration
            connection_config = {
                'host': self.config['host'],
                'port': self.config['port'],
                'user': self.config['user'],
                'password': self.config['password'],
                'database': self.config['database'],
                'autocommit': False,
                'connect_timeout': 10,
            }
            
            # SSL configuration - if sslmode is REQUIRED, force SSL
            if self.config.get('sslmode') == 'REQUIRED':
                connection_config['ssl'] = True
                logger.info("🔒 SSL mode enabled (certificate verification disabled)")
            else:
                connection_config['ssl'] = False
            
            # Test connection first
            if not await self._test_connection(connection_config):
                raise ConnectionError("Failed to establish initial database connection")
            
            # Prepare pool configuration
            pool_config = {
                **connection_config,
                'db': connection_config['database'],  # asyncmy uses 'db' for pool
                'minsize': 1,  # Start with 1 connection to avoid hanging on multiple connections
                'maxsize': 10,
                'pool_recycle': 3600,
            }
            
            logger.info(f"🔌 Creating connection pool...")

            # Create pool with timeout wrapper
            self.pool = await asyncio.wait_for(
                asyncmy.create_pool(**pool_config),
                timeout=30.0  # Total timeout for pool creation
            )

            # Get MySQL version to confirm connection
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT VERSION()")
                    version = await cursor.fetchone()
                    logger.info(f"📊 MySQL Version: {version[0]}")

            logger.success(f"✅ Database pool created successfully with {pool_config['minsize']}-{pool_config['maxsize']} connections!")
            
        except asyncio.TimeoutError:
            error_msg = "❌ Database pool creation timed out after 30 seconds. Check network connectivity and database availability."
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        except Exception as e:
            logger.error(f"❌ Failed to create DB pool: {e}")
            raise
    
    async def close(self):
        """Close connection pool on shutdown"""
        if self.pool:
            logger.info("🔌 Closing database pool...")
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("✅ Database pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Context manager for getting connections from pool"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self.pool.acquire() as conn:
            yield conn