"""
MongoDB Connection Pool Manager
Provides singleton connection pool for better performance
"""
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from app.core.settings import settings
import logging

logger = logging.getLogger(__name__)

class MongoDBConnection:
    """Singleton MongoDB connection pool"""
    _instance = None
    _client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._connect()
    
    def _connect(self):
        """Initialize MongoDB connection with pooling"""
        try:
            self._client = MongoClient(
                settings.mongodb_uri,
                maxPoolSize=10,
                minPoolSize=5,
                maxIdleTimeMS=30000,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=20000,
                retryWrites=True,
                w="majority"
            )
            self._db = self._client[settings.mongodb_db]
            logger.info(f"Connected to MongoDB: {settings.mongodb_db}")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {e}")
            raise
    
    def get_database(self):
        """Get database instance"""
        if self._client is None:
            self._connect()
        return self._db
        return self._client[settings.mongodb_db]
    
    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("MongoDB connection closed")

# Singleton instance
_mongo_connection = MongoDBConnection()

def get_db():
    """Get MongoDB database instance with connection pooling"""
    return _mongo_connection.get_database()
