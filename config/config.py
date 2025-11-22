import os
from dotenv import load_dotenv
from loguru import logger

# Load environment variables from .env file
load_dotenv()

class Config:
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = int(os.getenv('DB_PORT', 3306))
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    DB_SSL_MODE = os.getenv('DB_SSL_MODE', 'REQUIRED')

    # Application Configuration
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'production')
    BACKUP_FILE_PATH = os.getenv('BACKUP_FILE_PATH', 'data/failed_writes.jsonl')
    
    
    @classmethod
    def get_db_config(cls) -> dict:
        required = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
        missing = [key for key in required if not getattr(cls, key)]
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        return {
            'host': cls.DB_HOST,
            'port': cls.DB_PORT,
            'user': cls.DB_USER,
            'password': cls.DB_PASSWORD,
            'database': cls.DB_NAME,
            'sslmode': cls.DB_SSL_MODE
        }
    
    @classmethod
    def validate(cls):
        try:
            cls.get_db_config()
            logger.success("✅ Configuration validated successfully")
        except ValueError as e:
            logger.error(f"❌ Configuration error: {e}")
            raise

# Validate on import
Config.validate()