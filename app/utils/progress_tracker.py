"""
Progress tracking system for multi-step processing
Persists progress to disk to survive worker restarts
"""
import json
import os
import threading
import fcntl
import tempfile
from pathlib import Path
from typing import Dict, Optional

class ProgressTracker:
    """File-based progress tracker that survives worker restarts"""
    
    def __init__(self):
        self._lock = threading.Lock()
    
    def _get_progress_file(self, token: str) -> Path:
        """Get path to progress file for a token"""
        return Path(f"work/{token}/progress.json")
    
    def _read_progress(self, token: str) -> Optional[Dict]:
        """Read progress from file with shared lock (process-safe)"""
        progress_file = self._get_progress_file(token)
        if not progress_file.exists():
            return None
        
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    return json.load(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            return None
    
    def _write_progress(self, token: str, data: Dict) -> None:
        """Write progress to file with exclusive lock (process-safe, atomic)"""
        progress_file = self._get_progress_file(token)
        progress_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temp file in same directory (for atomic rename)
        temp_fd, temp_path = tempfile.mkstemp(
            dir=progress_file.parent, 
            prefix='.progress_', 
            suffix='.tmp'
        )
        
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                # Acquire exclusive lock on temp file
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(data, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            
            # Atomic rename - replaces old file atomically
            os.replace(temp_path, progress_file)
        except Exception:
            # Clean up temp file on error
            try:
                os.unlink(temp_path)
            except:
                pass
            raise
    
    def init_job(self, token: str) -> None:
        """Initialize progress tracking for a job"""
        with self._lock:
            progress_data = {
                'status': 'starting',
                'current_step': 'initializing',
                'percentage': 0,
                'message': 'Preparando análise...',
                'stages': {
                    'extraction': {'status': 'pending', 'weight': 10},
                    'detection': {'status': 'pending', 'weight': 5},
                    'classification': {'status': 'pending', 'weight': 25},
                    'parsing': {'status': 'pending', 'weight': 30},
                    'stats': {'status': 'pending', 'weight': 30}
                }
            }
            self._write_progress(token, progress_data)
    
    def update_stage(self, token: str, stage: str, status: str, message: str = '') -> None:
        """Update a specific stage's status"""
        with self._lock:
            progress_data = self._read_progress(token)
            if not progress_data:
                return
            
            if stage in progress_data['stages']:
                progress_data['stages'][stage]['status'] = status
            
            progress_data['current_step'] = stage
            if message:
                progress_data['message'] = message
            
            # Recalculate percentage
            progress_data['percentage'] = self._calculate_percentage(progress_data)
            
            self._write_progress(token, progress_data)
    
    def _calculate_percentage(self, progress_data: Dict) -> int:
        """Calculate overall percentage ensuring it never decreases"""
        stages = progress_data['stages']
        total_weight = sum(stage['weight'] for stage in stages.values())
        
        completed_weight = 0
        for stage, data in stages.items():
            if data['status'] == 'completed':
                completed_weight += data['weight']
            elif data['status'] == 'in_progress':
                # Count in_progress as 50% of weight
                completed_weight += data['weight'] * 0.5
        
        new_percentage = int((completed_weight / total_weight) * 100)
        
        # Ensure percentage never decreases
        current_percentage = progress_data.get('percentage', 0)
        return max(new_percentage, current_percentage)
    
    def complete_job(self, token: str) -> None:
        """Mark job as complete"""
        with self._lock:
            progress_data = self._read_progress(token)
            if not progress_data:
                return
            
            progress_data['status'] = 'completed'
            progress_data['percentage'] = 100
            progress_data['message'] = 'Análise concluída! Gerando dashboard...'
            
            self._write_progress(token, progress_data)
    
    def fail_job(self, token: str, error: str) -> None:
        """Mark job as failed"""
        with self._lock:
            progress_data = self._read_progress(token)
            if not progress_data:
                # Create minimal failure data
                progress_data = {
                    'status': 'failed',
                    'percentage': 0,
                    'message': f'Erro: {error}',
                    'current_step': 'failed',
                    'stages': {}
                }
            else:
                progress_data['status'] = 'failed'
                progress_data['message'] = f'Erro: {error}'
            
            self._write_progress(token, progress_data)
    
    def get_progress(self, token: str) -> Optional[Dict]:
        """Get progress for a specific job - reads from file"""
        return self._read_progress(token)
    
    def cleanup_job(self, token: str) -> None:
        """Remove job progress file"""
        with self._lock:
            progress_file = self._get_progress_file(token)
            if progress_file.exists():
                progress_file.unlink()

# Global progress tracker instance
progress_tracker = ProgressTracker()
