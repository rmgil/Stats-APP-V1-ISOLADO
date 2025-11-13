"""
Memory monitoring utilities for tracking resource usage during processing
"""
import psutil
import os
import logging

logger = logging.getLogger(__name__)


def get_memory_usage() -> dict:
    """
    Get current memory usage statistics
    
    Returns:
        Dictionary with memory usage info (MB)
    """
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    
    return {
        'rss_mb': mem_info.rss / 1024 / 1024,  # Resident Set Size
        'vms_mb': mem_info.vms / 1024 / 1024,  # Virtual Memory Size
        'percent': process.memory_percent()
    }


def log_memory_usage(context: str = ""):
    """
    Log current memory usage with optional context
    
    Args:
        context: Descriptive context for the log message
    """
    mem = get_memory_usage()
    prefix = f"[{context}] " if context else ""
    logger.info(
        f"{prefix}Memory: RSS={mem['rss_mb']:.1f}MB, "
        f"VMS={mem['vms_mb']:.1f}MB, "
        f"Percent={mem['percent']:.1f}%"
    )


class MemoryMonitor:
    """Context manager for monitoring memory usage during operations"""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_mem = None
        self.end_mem = None
    
    def __enter__(self):
        self.start_mem = get_memory_usage()
        log_memory_usage(f"{self.operation_name} START")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_mem = get_memory_usage()
        delta_rss = self.end_mem['rss_mb'] - self.start_mem['rss_mb']
        
        logger.info(
            f"[{self.operation_name} END] Memory delta: {delta_rss:+.1f}MB, "
            f"Final: {self.end_mem['rss_mb']:.1f}MB"
        )
