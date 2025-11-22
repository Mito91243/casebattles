import aiomysql
import asyncio
import ssl
from loguru import logger
from contextlib import asynccontextmanager
from typing import Optional

class DatabaseManager:
    def __init__(self, config: dict):
        self.config = config
        self.pool: Optional[aiomysql.Pool] = None
        
    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context for DigitalOcean managed database"""
        if self.config.get('sslmode') == 'REQUIRED':
            # Create SSL context that doesn't verify certificates
            # This is needed for DigitalOcean managed databases
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx
        return None
        
    async def _test_connection(self) -> bool:
        """Test a single connection before creating the pool"""
        conn = None
        try:
            logger.info("🔍 Testing database connection...")
            logger.info(f"📡 Connecting to {self.config['host']}:{self.config['port']}")
            
            # Create connection with timeout
            ssl_ctx = self._create_ssl_context()
            
            conn = await asyncio.wait_for(
                aiomysql.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['user'],
                    password=self.config['password'],
                    db=self.config['database'],
                    autocommit=False,
                    ssl=ssl_ctx,
                    connect_timeout=10
                ),
                timeout=15.0
            )
            
            # Test the connection with a simple query
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                result = await cursor.fetchone()
                logger.debug(f"Test query result: {result}")
            
            logger.success("✅ Connection test successful!")
            return True
            
        except asyncio.TimeoutError:
            logger.error("❌ Connection test timed out after 15 seconds")
            return False
        except Exception as e:
            logger.error(f"❌ Connection test failed: {type(e).__name__}: {e}")
            return False
        finally:
            # Ensure connection is properly closed
            if conn:
                try:
                    conn.close()
                    await conn.ensure_closed()
                except:
                    pass
    
    async def initialize(self):
        """Create connection pool on startup"""
        try:
            logger.info("📡 Attempting to connect to database...")
            logger.info(f"🔌 Database: {self.config['database']} @ {self.config['host']}:{self.config['port']}")
            
            # SSL configuration
            ssl_ctx = None
            if self.config.get('sslmode') == 'REQUIRED':
                ssl_ctx = self._create_ssl_context()
                logger.info("🔒 SSL mode enabled (certificate verification disabled)")
            else:
                logger.info("⚠️ SSL mode disabled")
            
            # Test connection first
            if not await self._test_connection():
                raise ConnectionError("Failed to establish initial database connection")
            
            logger.info(f"🔌 Creating connection pool...")
            
            # Create pool with proper configuration for aiomysql
            self.pool = await asyncio.wait_for(
                aiomysql.create_pool(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['user'],
                    password=self.config['password'],
                    db=self.config['database'],
                    minsize=1,  # Start with minimal connections
                    maxsize=10,
                    autocommit=False,
                    ssl=ssl_ctx,
                    connect_timeout=10,
                    echo=False,  # Set to True for debugging SQL queries
                    pool_recycle=3600,  # Recycle connections after 1 hour
                ),
                timeout=30.0
            )
            
            # Test the pool by acquiring and releasing a connection
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT VERSION()")
                    version = await cursor.fetchone()
                    logger.info(f"📊 MySQL Version: {version[0]}")
            
            logger.success(f"✅ Database pool created successfully with {self.pool.minsize}-{self.pool.maxsize} connections!")
            
        except asyncio.TimeoutError:
            error_msg = "❌ Database pool creation timed out after 30 seconds. Check network connectivity and database availability."
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        except Exception as e:
            logger.error(f"❌ Failed to create DB pool: {type(e).__name__}: {e}")
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
        
        conn = await self.pool.acquire()
        try:
            yield conn
        finally:
            self.pool.release(conn)
    
    async def execute_query(self, query: str, params: tuple = None) -> list:
        """Helper method to execute SELECT queries"""
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()
    
    async def execute_update(self, query: str, params: tuple = None) -> int:
        """Helper method to execute INSERT/UPDATE/DELETE queries"""
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                await conn.commit()
                return cursor.rowcount