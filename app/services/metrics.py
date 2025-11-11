"""
Performance metrics instrumentation for monitoring resource usage.

Task 8: Provides RAM/CPU tracking to validate streaming efficiency
and identify bottlenecks during concurrent uploads.
"""
import psutil
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ResourceMetrics:
    """Track and log resource usage for performance analysis"""
    
    @staticmethod
    def get_process_memory_mb():
        """
        Get current process RAM usage in MB.
        
        Returns:
            float: Memory usage in MB
        """
        try:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            return mem_info.rss / (1024 * 1024)  # Convert bytes to MB
        except Exception as e:
            logger.error(f"Error getting memory: {e}")
            return 0.0
    
    @staticmethod
    def get_system_memory_mb():
        """
        Get system-wide memory usage.
        
        Returns:
            dict: {total_mb, available_mb, used_mb, percent}
        """
        try:
            mem = psutil.virtual_memory()
            return {
                'total_mb': mem.total / (1024 * 1024),
                'available_mb': mem.available / (1024 * 1024),
                'used_mb': mem.used / (1024 * 1024),
                'percent': mem.percent
            }
        except Exception as e:
            logger.error(f"Error getting system memory: {e}")
            return {'total_mb': 0, 'available_mb': 0, 'used_mb': 0, 'percent': 0}
    
    @staticmethod
    def get_cpu_percent():
        """
        Get current CPU usage percentage.
        
        Returns:
            float: CPU usage percentage
        """
        try:
            return psutil.cpu_percent(interval=0.1)
        except Exception as e:
            logger.error(f"Error getting CPU: {e}")
            return 0.0
    
    @staticmethod
    def log_phase_metrics(phase_name, job_token, before_mb=None):
        """
        Log resource metrics for a specific pipeline phase.
        
        Args:
            phase_name: Name of the pipeline phase (e.g., "Extract", "Parse")
            job_token: Job token for identification
            before_mb: If provided, calculates delta from this baseline
        
        Returns:
            float: Current memory usage in MB
        """
        current_mb = ResourceMetrics.get_process_memory_mb()
        cpu_percent = ResourceMetrics.get_cpu_percent()
        system_mem = ResourceMetrics.get_system_memory_mb()
        
        # Calculate delta if baseline provided
        if before_mb is not None:
            delta_mb = current_mb - before_mb
            logger.info(
                f"📊 [{phase_name}] {job_token} | "
                f"RAM: {current_mb:.1f}MB (Δ{delta_mb:+.1f}MB) | "
                f"CPU: {cpu_percent:.1f}% | "
                f"System: {system_mem['used_mb']:.0f}MB/{system_mem['total_mb']:.0f}MB ({system_mem['percent']:.1f}%)"
            )
        else:
            logger.info(
                f"📊 [{phase_name}] {job_token} | "
                f"RAM: {current_mb:.1f}MB | "
                f"CPU: {cpu_percent:.1f}% | "
                f"System: {system_mem['used_mb']:.0f}MB/{system_mem['total_mb']:.0f}MB ({system_mem['percent']:.1f}%)"
            )
        
        return current_mb
    
    @staticmethod
    def log_job_summary(job_token, start_mb, end_mb, duration_seconds):
        """
        Log summary metrics for completed job.
        
        Args:
            job_token: Job token
            start_mb: Initial RAM usage in MB
            end_mb: Final RAM usage in MB
            duration_seconds: Total job duration
        """
        delta_mb = end_mb - start_mb
        logger.info(
            f"📈 [JOB COMPLETE] {job_token} | "
            f"Duration: {duration_seconds:.1f}s | "
            f"RAM Start: {start_mb:.1f}MB | "
            f"RAM End: {end_mb:.1f}MB | "
            f"RAM Delta: {delta_mb:+.1f}MB | "
            f"Peak estimate: {max(start_mb, end_mb):.1f}MB"
        )
