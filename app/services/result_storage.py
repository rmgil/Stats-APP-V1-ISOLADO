"""
Result Storage Service - Read job results from Object Storage or local filesystem
===============================================================================

This service provides a unified interface for reading pipeline results that works in:
- Production (Replit Autoscale): Reads from Object Storage (cloud-based, persistent)
- Development (local): Reads from local filesystem (work/ directory)

Key Features:
- Automatic fallback from cloud to local
- JSON parsing with error handling
- Memory-efficient for large result files
"""
import os
import logging
import json
from typing import Optional, Dict, Any
from pathlib import Path
from .storage import get_storage

logger = logging.getLogger(__name__)

class ResultStorageService:
    """
    Service for reading job results from persistent storage
    
    Usage:
        result_service = ResultStorageService()
        
        # Get pipeline result JSON
        result = result_service.get_pipeline_result(token)
        
        # Get stats JSON
        stats = result_service.get_stats_json(token, "nonko_9max")
    """
    
    def __init__(self):
        self.storage = get_storage()
        self.local_work_dir = Path("work")
    
    def _read_json_from_storage(self, storage_path: str) -> Optional[Dict[str, Any]]:
        """
        Read JSON file from Object Storage
        
        Args:
            storage_path: Path in storage (e.g., "/results/abc123/pipeline_result.json")
        
        Returns:
            Parsed JSON dict or None if not found
        """
        try:
            file_data = self.storage.download_file(storage_path)
            if file_data:
                return json.loads(file_data.decode('utf-8'))
        except Exception as e:
            logger.debug(f"Could not read {storage_path} from storage: {e}")
        
        return None
    
    def _read_json_from_local(self, local_path: Path) -> Optional[Dict[str, Any]]:
        """
        Read JSON file from local filesystem
        
        Args:
            local_path: Path object to local file
        
        Returns:
            Parsed JSON dict or None if not found
        """
        try:
            if local_path.exists():
                return json.loads(local_path.read_text())
        except Exception as e:
            logger.debug(f"Could not read {local_path} from local: {e}")
        
        return None
    
    def get_pipeline_result(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get pipeline_result.json for a job
        
        Args:
            token: Job token (12 hex characters)
        
        Returns:
            Pipeline result dict or None if not found
        """
        # Try cloud storage first (production)
        storage_path = f"/results/{token}/pipeline_result.json"
        result = self._read_json_from_storage(storage_path)
        
        if result:
            logger.debug(f"Read pipeline_result for {token} from cloud storage")
            return result
        
        # Fallback to local filesystem (dev or recently completed job)
        local_path = self.local_work_dir / token / "pipeline_result.json"
        result = self._read_json_from_local(local_path)
        
        if result:
            logger.debug(f"Read pipeline_result for {token} from local filesystem")
            return result
        
        logger.warning(f"Pipeline result not found for token {token} (checked cloud and local)")
        return None
    
    def get_multi_site_manifest(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get multi_site_manifest.json for a job
        
        Args:
            token: Job token
        
        Returns:
            Manifest dict or None if not found
        """
        # Try cloud storage first
        storage_path = f"/results/{token}/multi_site_manifest.json"
        manifest = self._read_json_from_storage(storage_path)
        
        if manifest:
            return manifest
        
        # Fallback to local
        local_path = self.local_work_dir / token / "multi_site_manifest.json"
        return self._read_json_from_local(local_path)
    
    def get_group_manifest(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get group_manifest.json for a job (legacy single-site)
        
        Args:
            token: Job token
        
        Returns:
            Manifest dict or None if not found
        """
        # Try cloud storage first
        storage_path = f"/results/{token}/group_manifest.json"
        manifest = self._read_json_from_storage(storage_path)
        
        if manifest:
            return manifest
        
        # Fallback to local
        local_path = self.local_work_dir / token / "group_manifest.json"
        return self._read_json_from_local(local_path)
    
    def get_stats_json(self, token: str, group_key: str, is_multi_site: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get statistics JSON for a group
        
        Args:
            token: Job token
            group_key: Group key (e.g., "nonko_9max", "pko")
            is_multi_site: Whether this is a multi-site job (stats in combined/)
        
        Returns:
            Stats dict or None if not found
        """
        if is_multi_site:
            # Multi-site: combined/stats/{group_key}_stats.json
            storage_path = f"/results/{token}/combined/stats/{group_key}_stats.json"
            stats = self._read_json_from_storage(storage_path)
            
            if stats:
                return stats
            
            # Fallback to local
            local_path = self.local_work_dir / token / "combined" / "stats" / f"{group_key}_stats.json"
            return self._read_json_from_local(local_path)
        else:
            # Single-site: Load from pipeline_result
            result = self.get_pipeline_result(token)
            if result and 'stats' in result:
                return result.get('stats', {}).get(group_key)
        
        return None
    
    def job_exists(self, token: str) -> bool:
        """
        Check if job results exist in storage
        
        Args:
            token: Job token
        
        Returns:
            True if pipeline_result.json exists (cloud or local)
        """
        return self.get_pipeline_result(token) is not None


# Singleton instance
_result_storage_service = None

def get_result_storage() -> ResultStorageService:
    """Get singleton ResultStorageService instance"""
    global _result_storage_service
    if _result_storage_service is None:
        _result_storage_service = ResultStorageService()
    return _result_storage_service
