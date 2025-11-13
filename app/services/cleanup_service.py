"""
Cleanup Service - Automatic cleanup of old sessions, jobs, and temporary files
================================================================================

This service runs periodically to clean up:
1. Old completed/failed processing jobs (>7 days)
2. Old upload sessions and chunks (>7 days)
3. Orphaned temp files in local storage
4. Optionally: old files in Object Storage (future enhancement)

Usage:
    from app.services.cleanup_service import CleanupService
    
    # Run manual cleanup
    stats = CleanupService.run_cleanup()
    
    # In production, this should be called periodically (e.g., daily cron job)
"""
import os
import logging
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

class CleanupService:
    """Centralized cleanup service for database and filesystem"""
    
    @staticmethod
    def get_db_connection():
        """Get database connection"""
        return psycopg2.connect(os.environ.get('DATABASE_URL'))
    
    @staticmethod
    def cleanup_old_jobs(days=7):
        """
        Remove completed/failed processing jobs older than specified days
        
        Args:
            days: Age threshold in days (default: 7)
        
        Returns:
            Number of jobs deleted
        """
        conn = None
        try:
            conn = CleanupService.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM processing_jobs
                WHERE status IN ('completed', 'failed')
                AND completed_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
            """, (days,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            cursor.close()
            
            logger.info(f"Cleaned up {deleted_count} old processing jobs (>{days} days)")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old jobs: {e}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                conn.close()
    
    @staticmethod
    def cleanup_old_upload_sessions(days=7):
        """
        Remove old upload sessions and their chunks
        
        Removes sessions that are:
        - Completed or failed
        - Older than specified days
        - Cascades to chunks via foreign key
        
        Args:
            days: Age threshold in days (default: 7)
        
        Returns:
            Tuple of (sessions_deleted, chunks_deleted)
        """
        conn = None
        try:
            conn = CleanupService.get_db_connection()
            cursor = conn.cursor()
            
            # First, count chunks that will be deleted (for logging)
            cursor.execute("""
                SELECT COUNT(*)
                FROM upload_chunks
                WHERE session_token IN (
                    SELECT token
                    FROM upload_sessions
                    WHERE status IN ('completed', 'failed')
                    AND updated_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
                )
            """, (days,))
            
            chunks_count = cursor.fetchone()[0]
            
            # Delete sessions (chunks will cascade)
            cursor.execute("""
                DELETE FROM upload_sessions
                WHERE status IN ('completed', 'failed')
                AND updated_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
            """, (days,))
            
            sessions_count = cursor.rowcount
            conn.commit()
            cursor.close()
            
            logger.info(
                f"Cleaned up {sessions_count} old upload sessions and "
                f"{chunks_count} chunks (>{days} days)"
            )
            return (sessions_count, chunks_count)
            
        except Exception as e:
            logger.error(f"Error cleaning up old upload sessions: {e}")
            if conn:
                conn.rollback()
            return (0, 0)
        finally:
            if conn:
                conn.close()
    
    @staticmethod
    def cleanup_expired_sessions():
        """
        Remove upload sessions that have explicitly expired
        
        Returns:
            Number of expired sessions deleted
        """
        conn = None
        try:
            conn = CleanupService.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM upload_sessions
                WHERE expires_at IS NOT NULL
                AND expires_at < CURRENT_TIMESTAMP
                AND status NOT IN ('completed', 'failed')
            """)
            
            deleted_count = cursor.rowcount
            conn.commit()
            cursor.close()
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} expired upload sessions")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up expired sessions: {e}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                conn.close()
    
    @staticmethod
    def cleanup_temp_files(days=7):
        """
        Clean up old temporary files from /tmp
        
        Removes:
        - /tmp/processing_{token} directories older than specified days
        - /tmp/upload_{token} directories
        - /tmp/result_{token} files
        
        Args:
            days: Age threshold in days (default: 7)
        
        Returns:
            Number of items deleted
        """
        try:
            tmp_dir = Path("/tmp")
            cutoff_time = datetime.now() - timedelta(days=days)
            deleted_count = 0
            
            # Patterns to clean
            patterns = [
                "processing_*",
                "upload_*",
                "result_*",
                "storage_temp"
            ]
            
            for pattern in patterns:
                for item in tmp_dir.glob(pattern):
                    try:
                        # Check if item is old enough
                        if item.stat().st_mtime < cutoff_time.timestamp():
                            if item.is_file():
                                item.unlink()
                                deleted_count += 1
                            elif item.is_dir():
                                import shutil
                                shutil.rmtree(item)
                                deleted_count += 1
                    except Exception as item_error:
                        logger.warning(f"Could not delete {item}: {item_error}")
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} temporary files/directories (>{days} days)")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up temp files: {e}")
            return 0
    
    @staticmethod
    def cleanup_local_work_directory(days=30):
        """
        Clean up old work directories (local dev only)
        
        In production, results are in Object Storage, so this only cleans local dev files.
        
        Args:
            days: Age threshold in days (default: 30)
        
        Returns:
            Number of directories deleted
        """
        try:
            work_dir = Path("work")
            if not work_dir.exists():
                return 0
            
            cutoff_time = datetime.now() - timedelta(days=days)
            deleted_count = 0
            
            for token_dir in work_dir.iterdir():
                if token_dir.is_dir():
                    try:
                        # Check if directory is old enough
                        if token_dir.stat().st_mtime < cutoff_time.timestamp():
                            import shutil
                            shutil.rmtree(token_dir)
                            deleted_count += 1
                    except Exception as dir_error:
                        logger.warning(f"Could not delete {token_dir}: {dir_error}")
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old work directories (>{days} days)")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up work directories: {e}")
            return 0
    
    @staticmethod
    def run_cleanup(job_days=7, session_days=7, temp_days=7, work_days=30):
        """
        Run full cleanup operation
        
        Args:
            job_days: Age threshold for jobs
            session_days: Age threshold for sessions
            temp_days: Age threshold for temp files
            work_days: Age threshold for work directories
        
        Returns:
            Dictionary with cleanup statistics
        """
        logger.info("Starting scheduled cleanup...")
        
        stats = {
            'timestamp': datetime.now().isoformat(),
            'jobs_deleted': 0,
            'sessions_deleted': 0,
            'chunks_deleted': 0,
            'expired_sessions': 0,
            'temp_files_deleted': 0,
            'work_dirs_deleted': 0
        }
        
        try:
            # Clean up old jobs
            stats['jobs_deleted'] = CleanupService.cleanup_old_jobs(job_days)
            
            # Clean up old upload sessions and chunks
            sessions, chunks = CleanupService.cleanup_old_upload_sessions(session_days)
            stats['sessions_deleted'] = sessions
            stats['chunks_deleted'] = chunks
            
            # Clean up expired sessions
            stats['expired_sessions'] = CleanupService.cleanup_expired_sessions()
            
            # Clean up temp files
            stats['temp_files_deleted'] = CleanupService.cleanup_temp_files(temp_days)
            
            # Clean up work directories (local dev only)
            stats['work_dirs_deleted'] = CleanupService.cleanup_local_work_directory(work_days)
            
            logger.info(f"Cleanup completed: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            stats['error'] = str(e)
            return stats
