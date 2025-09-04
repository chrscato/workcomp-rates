"""
Django signals for cleanup and optimization
"""
import logging
from django.core.signals import request_finished
from django.dispatch import receiver
from .utils.parquet_utils import ParquetDataManager

logger = logging.getLogger(__name__)

@receiver(request_finished)
def cleanup_connections(sender, **kwargs):
    """
    Clean up database connections after each request to prevent memory leaks
    """
    try:
        # Clean up DuckDB connections periodically
        # Only do this every 10th request to avoid overhead
        import random
        if random.randint(1, 10) == 1:
            ParquetDataManager.cleanup_connections()
    except Exception as e:
        logger.error(f"Error cleaning up connections: {str(e)}")