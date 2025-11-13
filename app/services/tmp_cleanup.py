"""
Temporary file cleanup service
Prevents /tmp from filling up by cleaning old files at startup
"""
import os
import shutil
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def cleanup_old_tmp_files(max_age_hours: int = 24) -> tuple[int, float]:
    """
    Clean up old temporary files from /tmp to prevent disk quota issues
    
    Args:
        max_age_hours: Maximum age of files to keep (default: 24 hours)
    
    Returns:
        Tuple of (files_removed, mb_freed)
    """
    tmp_dir = Path('/tmp')
    cutoff_time = datetime.now().timestamp() - (max_age_hours * 3600)
    
    files_removed = 0
    bytes_freed = 0
    
    # Patterns to clean
    patterns_to_clean = [
        'processing_*',  # Old processing directories
        '[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]',  # Upload token dirs
        'tmp*',  # Generic temp files
        'pyright-*',  # Pyright cache
    ]
    
    # Directories to preserve
    preserve_dirs = {
        'storage',  # Local storage (will be migrated to cloud)
        'logs',  # Application logs
        'job_queue.db',  # SQLite queue
        'results.db',  # Results database
        'background_worker.lock',  # Worker lock file
    }
    
    logger.info(f"Starting /tmp cleanup (removing files older than {max_age_hours}h)")
    
    for pattern in patterns_to_clean:
        for item in tmp_dir.glob(pattern):
            # Skip preserved items
            if item.name in preserve_dirs:
                continue
            
            try:
                # Check modification time
                mtime = item.stat().st_mtime
                
                if mtime < cutoff_time:
                    # Calculate size before removing
                    if item.is_dir():
                        size = sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
                        shutil.rmtree(item)
                        logger.info(f"Removed old directory: {item.name} ({size / 1024 / 1024:.1f}MB)")
                    else:
                        size = item.stat().st_size
                        item.unlink()
                        logger.info(f"Removed old file: {item.name} ({size / 1024 / 1024:.1f}MB)")
                    
                    files_removed += 1
                    bytes_freed += size
                    
            except Exception as e:
                logger.warning(f"Failed to remove {item}: {e}")
                continue
    
    mb_freed = bytes_freed / 1024 / 1024
    logger.info(f"✓ Cleanup complete: removed {files_removed} items, freed {mb_freed:.1f}MB")
    
    return files_removed, mb_freed


def get_tmp_usage() -> dict:
    """
    Get /tmp disk usage statistics
    
    Returns:
        Dict with total, used, free, percent_used
    """
    try:
        import shutil
        stat = shutil.disk_usage('/tmp')
        
        return {
            'total_gb': stat.total / (1024 ** 3),
            'used_gb': stat.used / (1024 ** 3),
            'free_gb': stat.free / (1024 ** 3),
            'percent_used': (stat.used / stat.total) * 100
        }
    except Exception as e:
        logger.error(f"Failed to get /tmp usage: {e}")
        return {}


def run_startup_cleanup():
    """Run cleanup at application startup"""
    try:
        # Get usage before cleanup
        usage_before = get_tmp_usage()
        
        if usage_before:
            logger.info(f"💾 /tmp usage before cleanup: {usage_before['used_gb']:.1f}GB / {usage_before['total_gb']:.1f}GB ({usage_before['percent_used']:.1f}%)")
        
        # Clean old files (24h)
        files_removed, mb_freed = cleanup_old_tmp_files(max_age_hours=24)
        
        # Get usage after cleanup
        usage_after = get_tmp_usage()
        
        if usage_after:
            logger.info(f"💾 /tmp usage after cleanup: {usage_after['used_gb']:.1f}GB / {usage_after['total_gb']:.1f}GB ({usage_after['percent_used']:.1f}%)")
            
            # Warn if still >80% full
            if usage_after['percent_used'] > 80:
                logger.warning(f"⚠️ /tmp usage still high ({usage_after['percent_used']:.1f}%) - consider enabling Object Storage")
        
    except Exception as e:
        logger.error(f"Startup cleanup failed: {e}")
        # Don't crash the app if cleanup fails
