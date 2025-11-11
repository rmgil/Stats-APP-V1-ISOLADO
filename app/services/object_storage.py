"""
Replit Object Storage Service for Python/Flask
Adapted from blueprint:javascript_object_storage for distributed autoscale deployments
"""
import os
import logging
from typing import Optional, BinaryIO
from uuid import uuid4
from google.cloud import storage
import google.auth
import requests
from flask import Response
import io
import json

logger = logging.getLogger(__name__)

# Replit sidecar endpoint for authentication
REPLIT_SIDECAR_ENDPOINT = "http://127.0.0.1:1106"

class ObjectNotFoundError(Exception):
    """Raised when an object is not found in storage"""
    pass

class ObjectStorageService:
    """
    Service for interacting with Replit Object Storage (Google Cloud Storage backend)
    
    Features:
    - Upload files to cloud storage (persistent, shared across autoscale instances)
    - Download files from cloud storage
    - Generate signed URLs for direct uploads
    - List and search for objects
    
    Environment Variables Required:
    - PRIVATE_OBJECT_DIR: Directory path for private objects (format: /bucket_name/path)
    """
    
    def __init__(self):
        self._client = None
    
    @property
    def client(self) -> Optional[storage.Client]:
        """Lazy initialize Google Cloud Storage client with Replit credentials"""
        if self._client is None:
            try:
                # Try to initialize Google Cloud Storage client
                # For Replit deployments, credentials are handled automatically via sidecar
                # For local development, this will fail gracefully and use local filesystem fallback
                
                # Verify sidecar is available
                if not self.is_available():
                    logger.warning("Replit sidecar not available - using local filesystem")
                    return None
                
                # Create client (Replit sidecar handles authentication automatically)
                self._client = storage.Client(
                    project=""  # Empty project ID for Replit
                )
                
                logger.info("Object Storage client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Object Storage client: {e}")
                logger.warning("Object Storage not available - falling back to local filesystem")
                self._client = None
        
        return self._client
    
    def is_available(self) -> bool:
        """Check if Object Storage is available (running on Replit with sidecar)"""
        # Check if Replit sidecar is available (works in dev AND production on Replit)
        try:
            response = requests.get(f"{REPLIT_SIDECAR_ENDPOINT}/credential", timeout=1)
            return response.status_code == 200
        except Exception:
            return False
    
    def _get_client(self) -> storage.Client:
        """Get the storage client, raising an error if not available"""
        client = self.client
        if client is None:
            raise RuntimeError("Object Storage not available - not running on Replit deployment")
        return client
    
    def get_private_object_dir(self) -> str:
        """Get the private object directory from environment variables"""
        dir_path = os.getenv('PRIVATE_OBJECT_DIR', '')
        if not dir_path:
            raise ValueError(
                "PRIVATE_OBJECT_DIR not set. Create a bucket in 'Object Storage' "
                "tool and set PRIVATE_OBJECT_DIR env var (format: /bucket_name/uploads)"
            )
        return dir_path
    
    def _parse_object_path(self, path: str) -> tuple[str, str]:
        """
        Parse object path into bucket name and object name
        
        Args:
            path: Full path (format: /bucket_name/object_name or bucket_name/object_name)
        
        Returns:
            Tuple of (bucket_name, object_name)
        """
        if not path.startswith('/'):
            path = f'/{path}'
        
        parts = path.split('/')
        if len(parts) < 3:
            raise ValueError(f"Invalid path: {path} - must contain bucket and object name")
        
        bucket_name = parts[1]
        object_name = '/'.join(parts[2:])
        
        return bucket_name, object_name
    
    def upload_file(self, file_data: bytes, object_path: str, content_type: str = 'application/octet-stream') -> str:
        """
        Upload a file to Object Storage
        
        Args:
            file_data: Binary data to upload
            object_path: Full path where to store (format: /bucket_name/path/to/file)
            content_type: MIME type of the file
        
        Returns:
            Full GCS URL of the uploaded file
        """
        bucket_name, object_name = self._parse_object_path(object_path)
        
        try:
            client = self._get_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Upload with content type
            blob.upload_from_string(file_data, content_type=content_type)
            
            logger.info(f"Uploaded file to Object Storage: {object_path} ({len(file_data)} bytes)")
            return f"gs://{bucket_name}/{object_name}"
            
        except Exception as e:
            logger.error(f"Failed to upload file to Object Storage: {e}")
            raise
    
    def upload_file_stream(self, file_stream: BinaryIO, object_path: str, content_type: str = 'application/octet-stream') -> str:
        """
        Upload a file from a stream to Object Storage (memory efficient for large files)
        
        Args:
            file_stream: File-like object to upload
            object_path: Full path where to store
            content_type: MIME type of the file
        
        Returns:
            Full GCS URL of the uploaded file
        """
        bucket_name, object_name = self._parse_object_path(object_path)
        
        try:
            client = self._get_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Upload from stream
            blob.upload_from_file(file_stream, content_type=content_type)
            
            logger.info(f"Uploaded file stream to Object Storage: {object_path}")
            return f"gs://{bucket_name}/{object_name}"
            
        except Exception as e:
            logger.error(f"Failed to upload file stream: {e}")
            raise
    
    def download_file(self, object_path: str) -> bytes:
        """
        Download a file from Object Storage
        
        Args:
            object_path: Full path to the file
        
        Returns:
            Binary file data
        """
        bucket_name, object_name = self._parse_object_path(object_path)
        
        try:
            client = self._get_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            if not blob.exists():
                raise ObjectNotFoundError(f"Object not found: {object_path}")
            
            data = blob.download_as_bytes()
            logger.info(f"Downloaded file from Object Storage: {object_path} ({len(data)} bytes)")
            return data
            
        except ObjectNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            raise
    
    def download_file_stream(self, object_path: str) -> BinaryIO:
        """
        Download a file as a stream (memory efficient for large files)
        
        Args:
            object_path: Full path to the file
        
        Returns:
            File-like object with the file data
        """
        bucket_name, object_name = self._parse_object_path(object_path)
        
        try:
            client = self._get_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            if not blob.exists():
                raise ObjectNotFoundError(f"Object not found: {object_path}")
            
            # Download to BytesIO stream
            stream = io.BytesIO()
            blob.download_to_file(stream)
            stream.seek(0)  # Reset to beginning
            
            return stream
            
        except ObjectNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to download file stream: {e}")
            raise
    
    def stream_to_response(self, object_path: str, response: Response, cache_ttl_sec: int = 3600) -> Response:
        """
        Stream a file from Object Storage directly to Flask response (memory efficient)
        
        Args:
            object_path: Full path to the file
            response: Flask Response object to stream to
            cache_ttl_sec: Cache TTL in seconds
        
        Returns:
            Response object with streaming data
        """
        bucket_name, object_name = self._parse_object_path(object_path)
        
        try:
            client = self._get_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            if not blob.exists():
                raise ObjectNotFoundError(f"Object not found: {object_path}")
            
            # Get metadata
            blob.reload()  # Refresh metadata
            content_type = blob.content_type or 'application/octet-stream'
            size = blob.size
            
            # Set response headers
            response.headers['Content-Type'] = content_type
            response.headers['Content-Length'] = str(size)
            response.headers['Cache-Control'] = f'public, max-age={cache_ttl_sec}'
            
            # Stream the file
            response.direct_passthrough = True
            response.response = blob.open('rb')
            
            return response
            
        except ObjectNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to stream file: {e}")
            raise
    
    def delete_file(self, object_path: str) -> bool:
        """
        Delete a file from Object Storage
        
        Args:
            object_path: Full path to the file
        
        Returns:
            True if deleted successfully
        """
        bucket_name, object_name = self._parse_object_path(object_path)
        
        try:
            client = self._get_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            if blob.exists():
                blob.delete()
                logger.info(f"Deleted file from Object Storage: {object_path}")
                return True
            else:
                logger.warning(f"File not found for deletion: {object_path}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
            return False
    
    def file_exists(self, object_path: str) -> bool:
        """Check if a file exists in Object Storage"""
        if not self.is_available():
            return False
        
        try:
            client = self._get_client()
            bucket_name, object_name = self._parse_object_path(object_path)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking file existence: {e}")
            return False
    
    def get_upload_url(self, object_path: Optional[str] = None, ttl_sec: int = 900) -> tuple[str, str]:
        """
        Generate a signed URL for direct upload to Object Storage
        
        Args:
            object_path: Optional custom path. If None, generates random UUID path in private dir
            ttl_sec: Time-to-live for the signed URL in seconds (default: 15 minutes)
        
        Returns:
            Tuple of (signed_url, final_object_path)
        """
        # Ensure client is available
        _ = self._get_client()
        
        # Generate object path if not provided
        if object_path is None:
            private_dir = self.get_private_object_dir()
            object_id = str(uuid4())
            object_path = f"{private_dir}/uploads/{object_id}"
        
        bucket_name, object_name = self._parse_object_path(object_path)
        
        try:
            # Use Replit sidecar to generate signed URL
            request_data = {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "method": "PUT",
                "expires_at": None  # Will be calculated from ttl_sec by sidecar
            }
            
            response = requests.post(
                f"{REPLIT_SIDECAR_ENDPOINT}/object-storage/signed-object-url",
                json=request_data,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            
            if not response.ok:
                raise RuntimeError(
                    f"Failed to generate signed URL: {response.status_code} - "
                    "Make sure you're running on Replit deployment"
                )
            
            result = response.json()
            signed_url = result.get('signed_url')
            
            if not signed_url:
                raise RuntimeError("No signed URL in response")
            
            logger.info(f"Generated signed upload URL for: {object_path}")
            return signed_url, object_path
            
        except Exception as e:
            logger.error(f"Failed to generate signed URL: {e}")
            raise

# Singleton instance
_object_storage_service = None

def get_object_storage() -> ObjectStorageService:
    """Get singleton Object Storage service instance"""
    global _object_storage_service
    if _object_storage_service is None:
        _object_storage_service = ObjectStorageService()
    return _object_storage_service
