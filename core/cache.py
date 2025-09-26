import hashlib
import json
import pickle
from django.core.cache import cache
from django.conf import settings
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DatasetCache:
    """Manages caching of combined datasets for dataset review"""
    
    CACHE_PREFIX = "dataset_review_"
    CACHE_TIMEOUT = 3600  # 1 hour
    
    @classmethod
    def generate_cache_key(cls, filters, max_rows, max_partitions):
        """Generate a unique cache key for the dataset"""
        # Create a deterministic key from filters and parameters
        cache_data = {
            'filters': {k: sorted(v) if isinstance(v, list) else v 
                       for k, v in filters.items() if v},
            'max_rows': max_rows,
            'max_partitions': max_partitions
        }
        
        # Sort keys for consistent hashing
        cache_string = json.dumps(cache_data, sort_keys=True)
        cache_hash = hashlib.md5(cache_string.encode()).hexdigest()
        
        return f"{cls.CACHE_PREFIX}{cache_hash}"
    
    @classmethod
    def get_cached_dataset(cls, cache_key):
        """Retrieve cached dataset"""
        try:
            cached_data = cache.get(cache_key)
            if cached_data:
                logger.info(f"Cache hit for key: {cache_key}")
                return cached_data
            else:
                logger.info(f"Cache miss for key: {cache_key}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving cache: {e}")
            return None
    
    @classmethod
    def cache_dataset(cls, cache_key, dataset_info):
        """Cache dataset information"""
        try:
            # Store metadata, not the actual DataFrame
            cache_data = {
                'partitions_info': dataset_info.get('partitions_info'),
                's3_paths': dataset_info.get('s3_paths'),
                'max_rows': dataset_info.get('max_rows'),
                'max_partitions': dataset_info.get('max_partitions'),
                'load_timestamp': dataset_info.get('load_timestamp'),
                'filters': dataset_info.get('filters')
            }
            
            cache.set(cache_key, cache_data, cls.CACHE_TIMEOUT)
            logger.info(f"Dataset cached with key: {cache_key}")
            return True
        except Exception as e:
            logger.error(f"Error caching dataset: {e}")
            return False
    
    @classmethod
    def invalidate_cache(cls, pattern=None):
        """Invalidate cache entries"""
        try:
            if pattern:
                # Invalidate specific pattern
                cache.delete_many(cache.keys(f"{cls.CACHE_PREFIX}*"))
            else:
                # Invalidate all dataset review cache
                cache.delete_many(cache.keys(f"{cls.CACHE_PREFIX}*"))
            logger.info("Cache invalidated")
            return True
        except Exception as e:
            logger.error(f"Error invalidating cache: {e}")
            return False
