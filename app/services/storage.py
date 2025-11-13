"""
Unified Storage Service - Production-ready with local fallback
========================================================

This service provides a unified interface for file storage that works seamlessly in:
- Production (Replit Autoscale): Uses Object Storage (cloud-based, persistent across instances)
- Development (local): Uses local filesystem as fallback

Key Features:
- Transparent switching between Object Storage and local filesystem
- Memory-efficient streaming for large files (100MB+)
- All business logic remains untouched - only I/O layer changes

Architecture Note:
This is the ONLY file that needs to know about storage backends.
All other code (pipeline, workers, APIs) uses this unified interface.
"""
import os
import logging
from typing import Optional, BinaryIO
from pathlib import Path
import shutil
from .supabase_storage import SupabaseStorageService
import io

logger = logging.getLogger(__name__)

class StorageService:
    """
    Unified storage service with automatic fallback
    
    Usage:
        storage = get_storage()
        
        # Upload (works in both prod and dev)
        storage.upload_file(data, "/chunks/abc123.bin")
        
        # Download (works in both prod and dev)
        data = storage.download_file("/chunks/abc123.bin")
        
    The service automatically detects if running on Replit (uses Object Storage)
    or locally (uses filesystem), making deployment seamless.
    """
    
    def __init__(self, local_base_dir: str = "/tmp/storage", require_cloud: bool = False, bucket_name: str = 'poker-uploads'):
        """
        Initialize storage service
        
        Args:
            local_base_dir: Base directory for local filesystem fallback
            require_cloud: If True, fail fast if cloud storage is not available (production mode)
            bucket_name: Supabase storage bucket name
        """
        self.supabase_storage = SupabaseStorageService(bucket_name=bucket_name, use_service_role=True)
        self.local_base_dir = Path(local_base_dir)
        
        # Create local directory if it doesn't exist
        self.local_base_dir.mkdir(parents=True, exist_ok=True)
        
        self.use_cloud = self.supabase_storage.enabled
        
        # Fail fast if cloud storage required but not available
        if require_cloud and not self.use_cloud:
            raise RuntimeError(
                "Supabase Storage required but not available. "
                "Make sure SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are configured."
            )
        
        is_production = os.getenv('REPL_DEPLOYMENT') == '1'
        env_name = "PRODUCTION" if is_production else "DEVELOPMENT"
        if self.use_cloud:
            logger.info(f"Storage: Using Supabase Storage (cloud mode - {env_name}, bucket={bucket_name})")
        else:
            logger.info(f"Storage: Using local filesystem ({env_name} - {self.local_base_dir})")
    
    def _local_path(self, path: str) -> Path:
        """Convert storage path to local filesystem path"""
        # Remove leading slash and convert to Path
        clean_path = path.lstrip('/')
        return self.local_base_dir / clean_path
    
    def _cloud_path(self, path: str) -> str:
        """
        Convert storage path to Supabase storage path
        
        Supabase paths do not need bucket prefix (handled by bucket_name in client)
        """
        # Remove leading slash for Supabase (paths are relative to bucket)
        return path.lstrip('/')
    
    def upload_file(self, file_data: bytes, path: str, content_type: str = 'application/octet-stream') -> None:
        """
        Upload a file to storage
        
        Args:
            file_data: Binary data to upload
            path: Storage path (e.g., "/chunks/abc123.bin")
            content_type: MIME type
        
        Raises:
            Exception if upload fails (CRITICAL - never silent fail in production)
        """
        try:
            if self.use_cloud:
                # Upload to Supabase Storage
                cloud_path = self._cloud_path(path)
                success = self.supabase_storage.upload_data(file_data, cloud_path, content_type)
                if not success:
                    raise Exception("Supabase upload returned False")
                logger.debug(f"Uploaded to Supabase: {path}")
            else:
                # Upload to local filesystem
                local_path = self._local_path(path)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(file_data)
                logger.debug(f"Uploaded to local: {path}")
            
        except Exception as e:
            logger.error(f"CRITICAL: Failed to upload file {path}: {e}")
            raise  # NEVER silent fail - caller must handle
    
    def upload_file_stream(self, file_stream: BinaryIO, path: str, content_type: str = 'application/octet-stream') -> None:
        """
        Upload a file from stream (memory efficient for large files)
        
        Args:
            file_stream: File-like object
            path: Storage path
            content_type: MIME type
        
        Raises:
            Exception if upload fails (CRITICAL - never silent fail in production)
        """
        try:
            if self.use_cloud:
                # Upload stream to Supabase Storage
                cloud_path = self._cloud_path(path)
                success = self.supabase_storage.upload_file_from_stream(file_stream, cloud_path, content_type)
                if not success:
                    raise Exception("Supabase stream upload returned False")
                logger.debug(f"Uploaded stream to Supabase: {path}")
            else:
                # Write stream to local file
                local_path = self._local_path(path)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                with local_path.open('wb') as f:
                    shutil.copyfileobj(file_stream, f)
                
                logger.debug(f"Uploaded stream to local: {path}")
            
        except Exception as e:
            logger.error(f"CRITICAL: Failed to upload file stream {path}: {e}")
            raise  # NEVER silent fail - caller must handle
    
    def upload_fileobj(self, file_obj: BinaryIO, path: str, content_type: str = 'application/octet-stream') -> None:
        """
        Upload a file object to storage (alias for upload_file_stream for compatibility)
        
        Args:
            file_obj: Open file object (BinaryIO)
            path: Storage path
            content_type: MIME type
        
        Raises:
            Exception if upload fails
        """
        self.upload_file_stream(file_obj, path, content_type)
    
    def download_file(self, path: str) -> Optional[bytes]:
        """
        Download a file from storage
        
        Args:
            path: Storage path
        
        Returns:
            Binary data or None if not found
        """
        try:
            if self.use_cloud:
                # Download from Supabase Storage
                cloud_path = self._cloud_path(path)
                data = self.supabase_storage.download_file(cloud_path)
                if data:
                    logger.debug(f"Downloaded from Supabase: {path} ({len(data)} bytes)")
                return data
            else:
                # Read from local filesystem
                local_path = self._local_path(path)
                
                if not local_path.exists():
                    logger.warning(f"File not found locally: {path}")
                    return None
                
                data = local_path.read_bytes()
                logger.debug(f"Downloaded from local: {path} ({len(data)} bytes)")
                return data
                
        except Exception as e:
            logger.error(f"Failed to download file {path}: {e}")
            return None
    
    def download_file_stream(self, path: str) -> Optional[BinaryIO]:
        """
        Download a file as stream (memory efficient)
        
        Args:
            path: Storage path
        
        Returns:
            File-like object or None if not found
        """
        try:
            if self.use_cloud:
                # Download full file from Supabase and wrap in BytesIO
                cloud_path = self._cloud_path(path)
                data = self.supabase_storage.download_file(cloud_path)
                if data:
                    logger.debug(f"Downloaded from Supabase as stream: {path}")
                    return io.BytesIO(data)
                return None
            else:
                # Open local file for reading
                local_path = self._local_path(path)
                
                if not local_path.exists():
                    logger.warning(f"File not found locally: {path}")
                    return None
                
                stream = local_path.open('rb')
                logger.debug(f"Downloaded stream from local: {path}")
                return stream
                
        except Exception as e:
            logger.error(f"Failed to download file stream {path}: {e}")
            return None
    
    def delete_file(self, path: str) -> bool:
        """
        Delete a file from storage
        
        Args:
            path: Storage path
        
        Returns:
            True if deleted successfully
        """
        try:
            if self.use_cloud:
                # Delete from Supabase Storage
                cloud_path = self._cloud_path(path)
                result = self.supabase_storage.delete_file(cloud_path)
                logger.debug(f"Deleted from Supabase: {path}")
                return result
            else:
                # Delete from local filesystem
                local_path = self._local_path(path)
                
                if local_path.exists():
                    if local_path.is_file():
                        local_path.unlink()
                    else:
                        shutil.rmtree(local_path)
                    logger.debug(f"Deleted from local: {path}")
                    return True
                else:
                    logger.warning(f"File not found for deletion: {path}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to delete file {path}: {e}")
            return False
    
    def file_exists(self, path: str) -> bool:
        """Check if a file exists in storage"""
        try:
            if self.use_cloud:
                cloud_path = self._cloud_path(path)
                return self.supabase_storage.file_exists(cloud_path)
            else:
                local_path = self._local_path(path)
                return local_path.exists()
        except Exception as e:
            logger.error(f"Error checking file existence {path}: {e}")
            return False
    
    def list_files(self, prefix: str) -> list[str]:
        """
        List all files with given prefix
        
        Args:
            prefix: Path prefix (e.g., "/chunks/abc123")
        
        Returns:
            List of file paths
        """
        try:
            if self.use_cloud:
                # TODO: Implement cloud listing when needed
                logger.warning("Cloud storage listing not yet implemented")
                return []
            else:
                # List local files
                local_prefix = self._local_path(prefix)
                
                if not local_prefix.exists():
                    return []
                
                if local_prefix.is_file():
                    return [prefix]
                
                # List all files in directory
                files = []
                for file_path in local_prefix.rglob('*'):
                    if file_path.is_file():
                        # Convert back to storage path
                        rel_path = file_path.relative_to(self.local_base_dir)
                        files.append(f"/{rel_path}")
                
                return files
                
        except Exception as e:
            logger.error(f"Failed to list files with prefix {prefix}: {e}")
            return []
    
    def get_local_path(self, path: str) -> Optional[str]:
        """
        Get local filesystem path for a storage path
        
        IMPORTANT: This method should ONLY be used when you absolutely need
        a local file path (e.g., passing to external tools that require filesystem paths).
        
        In cloud mode, this will DOWNLOAD the file to a temp location.
        Prefer using streams (download_file_stream) whenever possible.
        
        Args:
            path: Storage path
        
        Returns:
            Local filesystem path or None if file doesn't exist
        """
        try:
            if self.use_cloud:
                # Download to temporary location
                temp_dir = Path("/tmp/storage_temp")
                temp_dir.mkdir(parents=True, exist_ok=True)
                
                temp_file = temp_dir / path.lstrip('/').replace('/', '_')
                
                # Download if not already cached
                if not temp_file.exists():
                    data = self.download_file(path)
                    if data is None:
                        return None
                    temp_file.write_bytes(data)
                
                return str(temp_file)
            else:
                # Already local
                local_path = self._local_path(path)
                return str(local_path) if local_path.exists() else None
                
        except Exception as e:
            logger.error(f"Failed to get local path for {path}: {e}")
            return None

# Singleton instance
_storage_service = None

def get_storage(require_cloud: bool = False) -> StorageService:
    """
    Get singleton storage service instance
    
    Args:
        require_cloud: If True, fail fast if Object Storage not available (production mode)
    
    CRITICAL: If require_cloud=True but existing instance is in local mode, this will
    re-instantiate to ensure production safety (prevents silent fallback after transient failures).
    """
    global _storage_service
    
    # If require_cloud=True but we have a local-mode instance, re-instantiate
    # This prevents silent fallback to local storage in production after transient startup failures
    if require_cloud and _storage_service is not None and not _storage_service.use_cloud:
        logger.warning(
            "CRITICAL: Re-instantiating StorageService because require_cloud=True "
            "but existing instance is in local mode. This may indicate a transient "
            "failure during initial startup."
        )
        _storage_service = None  # Force re-instantiation
    
    # Create new instance if needed
    if _storage_service is None:
        _storage_service = StorageService(require_cloud=require_cloud)
    
    return _storage_service
