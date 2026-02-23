"""
Simple in-memory cache service with TTL for performance optimization
Phase 2D: Safe caching layer with short TTL
"""
from datetime import datetime, timedelta
from typing import Any, Optional, Dict
import logging
from functools import wraps
import hashlib
import json
import threading

logger = logging.getLogger(__name__)


class SimpleCache:
    """Thread-safe in-memory cache with TTL"""
    
    def __init__(self):
        self._cache: Dict[str, tuple[Any, datetime]] = {}
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired"""
        with self._lock:
            if key not in self._cache:
                return None
            
            value, expiry = self._cache[key]
            if datetime.utcnow() > expiry:
                # Expired, remove from cache
                del self._cache[key]
                return None
            
            return value
    
    def set(self, key: str, value: Any, ttl_seconds: int = 60):
        """Set value in cache with TTL"""
        expiry = datetime.utcnow() + timedelta(seconds=ttl_seconds)
        with self._lock:
            self._cache[key] = (value, expiry)
    
    def delete(self, key: str):
        """Delete key from cache"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
    
    def clear(self):
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()
    
    def cleanup_expired(self):
        """Remove expired entries"""
        now = datetime.utcnow()
        with self._lock:
            expired_keys = [k for k, (_, expiry) in self._cache.items() if now > expiry]
            for key in expired_keys:
                del self._cache[key]


# Global cache instance
_cache = SimpleCache()


def get_cache() -> SimpleCache:
    """Get the global cache instance"""
    return _cache


def cache_key(*args, **kwargs) -> str:
    """Generate cache key from function arguments"""
    key_data = {
        'args': args,
        'kwargs': kwargs
    }
    key_str = json.dumps(key_data, sort_keys=True, default=str)
    return hashlib.md5(key_str.encode()).hexdigest()


def cached(ttl_seconds: int = 60, key_prefix: str = ""):
    """
    Decorator to cache function results with TTL
    
    Args:
        ttl_seconds: Time to live in seconds (default: 60)
        key_prefix: Prefix for cache key (default: function name)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            prefix = key_prefix or func.__name__
            key = f"{prefix}:{cache_key(*args, **kwargs)}"
            
            # Try to get from cache
            cached_value = _cache.get(key)
            if cached_value is not None:
                logger.debug(f"Cache HIT: {prefix}")
                return cached_value
            
            # Cache miss - execute function
            logger.debug(f"Cache MISS: {prefix}")
            result = func(*args, **kwargs)
            
            # Store in cache
            _cache.set(key, result, ttl_seconds)
            
            return result
        
        # Add cache control methods
        wrapper.cache_clear = lambda: _cache.clear()
        wrapper.cache_delete = lambda *args, **kwargs: _cache.delete(
            f"{key_prefix or func.__name__}:{cache_key(*args, **kwargs)}"
        )
        
        return wrapper
    return decorator
