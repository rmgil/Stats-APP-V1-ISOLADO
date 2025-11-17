"""
Supabase Storage service for file upload/download
"""
import os
import gzip
import logging
from pathlib import Path
from typing import Optional, BinaryIO
from supabase import create_client, Client

from app.utils.supabase_retry import with_supabase_retry

logger = logging.getLogger(__name__)

COMPRESSION_THRESHOLD_MB = 1

class SupabaseStorageService:
    """
    Service for uploading and downloading files to/from Supabase Storage
    
    Usage:
        storage = SupabaseStorageService(bucket_name='poker-uploads')
        
        # Upload file
        storage.upload_file('/path/to/file.zip', 'uploads/user123/file.zip')
        
        # Download file
        data = storage.download_file('uploads/user123/file.zip')
        
        # Check if file exists
        exists = storage.file_exists('uploads/user123/file.zip')
    """
    
    def __init__(self, bucket_name: str = 'poker-uploads', use_service_role: bool = True):
        """
        Initialize Supabase Storage client
        
        Args:
            bucket_name: Name of the storage bucket
            use_service_role: If True, use SERVICE_ROLE_KEY to bypass RLS policies (default)
        """
        supabase_url = os.getenv('SUPABASE_URL')
        
        if use_service_role:
            supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
            key_type = "service_role" if os.getenv('SUPABASE_SERVICE_ROLE_KEY') else "anon"
        else:
            supabase_key = os.getenv('SUPABASE_KEY')
            key_type = "anon"
        
        self.bucket_name = bucket_name
        self.client: Optional[Client] = None
        self.enabled = False
        
        if not supabase_url or not supabase_key:
            logger.warning("Supabase credentials not configured. Storage will not be available.")
        else:
            try:
                self.client = create_client(supabase_url, supabase_key)
                self.enabled = True
                
                # Verify bucket is accessible by trying to list it
                try:
                    with_supabase_retry(lambda: self.client.storage.from_(bucket_name).list())
                    logger.info(f"Supabase storage bucket '{bucket_name}' is accessible ({key_type} key)")
                except Exception:
                    # Try to create bucket if it doesn't exist
                    try:
                        with_supabase_retry(
                            lambda: self.client.storage.create_bucket(
                                bucket_name, options={'public': False}
                            )
                        )
                        logger.info(f"Created Supabase storage bucket: {bucket_name}")
                    except Exception:
                        # Bucket exists but may have RLS policies preventing creation
                        logger.info(f"Bucket '{bucket_name}' exists (RLS policies active, using {key_type} key)")
                
                logger.info(f"Supabase storage service initialized: bucket={bucket_name}, key_type={key_type}")
                
            except Exception as e:
                logger.error(f"Failed to initialize Supabase storage: {e}")
    
    def _should_compress(self, file_data: bytes, storage_path: str) -> bool:
        """Check if file should be compressed based on size and type"""
        size_mb = len(file_data) / (1024 * 1024)
        is_json = storage_path.lower().endswith('.json')
        return is_json and size_mb > COMPRESSION_THRESHOLD_MB
    
    def _compress_data(self, file_data: bytes) -> bytes:
        """Compress data using gzip"""
        return gzip.compress(file_data, compresslevel=6)
    
    def upload_file(
        self,
        local_path: str,
        storage_path: str,
        content_type: Optional[str] = None
    ) -> bool:
        """
        Upload a file to Supabase Storage
        
        Args:
            local_path: Path to local file
            storage_path: Path in storage bucket (e.g., 'uploads/user123/file.zip')
            content_type: MIME type (optional, auto-detected if not provided)
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self.enabled or not self.client:
            logger.warning("Supabase storage not enabled")
            return False
        
        try:
            # Auto-detect content type if not provided
            if not content_type:
                ext = Path(local_path).suffix.lower()
                content_type_map = {
                    '.zip': 'application/zip',
                    '.rar': 'application/x-rar-compressed',
                    '.txt': 'text/plain',
                    '.json': 'application/json'
                }
                content_type = content_type_map.get(ext, 'application/octet-stream')
            
            # Read file
            with open(local_path, 'rb') as f:
                file_data = f.read()
            
            original_size_mb = len(file_data) / (1024 * 1024)
            
            # Compress large JSON files
            file_options = {'content-type': content_type, 'upsert': 'true'}
            if self._should_compress(file_data, storage_path):
                logger.info(f"Compressing large JSON file: {storage_path} ({original_size_mb:.2f}MB)")
                file_data = self._compress_data(file_data)
                compressed_size_mb = len(file_data) / (1024 * 1024)
                reduction_pct = ((original_size_mb - compressed_size_mb) / original_size_mb) * 100
                logger.info(f"Compressed {original_size_mb:.2f}MB → {compressed_size_mb:.2f}MB ({reduction_pct:.1f}% reduction)")
                file_options['content-encoding'] = 'gzip'
            
            logger.info(f"Uploading to Supabase: {storage_path} ({len(file_data) / (1024*1024):.2f}MB)")

            with_supabase_retry(
                lambda: self.client.storage.from_(self.bucket_name).upload(
                    path=storage_path,
                    file=file_data,
                    file_options=file_options  # type: ignore[arg-type]
                )
            )
            
            logger.info(f"✅ Upload complete: {storage_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading file to storage: {e}", exc_info=True)
            return False
    
    def upload_file_from_stream(
        self,
        file_stream: BinaryIO,
        storage_path: str,
        content_type: Optional[str] = None
    ) -> bool:
        """
        Upload a file from a stream (e.g., uploaded file) to Supabase Storage
        
        Args:
            file_stream: File-like object with read() method
            storage_path: Path in storage bucket
            content_type: MIME type (optional)
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self.enabled or not self.client:
            logger.warning("Supabase storage not enabled")
            return False
        
        try:
            # Read stream data
            file_data = file_stream.read()
            
            # Reset stream position if possible
            if hasattr(file_stream, 'seek'):
                file_stream.seek(0)
            
            original_size_mb = len(file_data) / (1024 * 1024)
            
            # Compress large JSON files
            file_options = {'content-type': content_type or 'application/octet-stream', 'upsert': 'true'}
            if self._should_compress(file_data, storage_path):
                logger.info(f"Compressing large JSON file: {storage_path} ({original_size_mb:.2f}MB)")
                file_data = self._compress_data(file_data)
                compressed_size_mb = len(file_data) / (1024 * 1024)
                reduction_pct = ((original_size_mb - compressed_size_mb) / original_size_mb) * 100
                logger.info(f"Compressed {original_size_mb:.2f}MB → {compressed_size_mb:.2f}MB ({reduction_pct:.1f}% reduction)")
                # Add .gz extension and change content-type instead of using content-encoding header
                # (Supabase Storage doesn't handle content-encoding properly in multipart uploads)
                storage_path = storage_path + '.gz'
                file_options['content-type'] = 'application/gzip'
            
            logger.info(f"Uploading to Supabase: {storage_path} ({len(file_data) / (1024*1024):.2f}MB)")

            with_supabase_retry(
                lambda: self.client.storage.from_(self.bucket_name).upload(
                    path=storage_path,
                    file=file_data,
                    file_options=file_options  # type: ignore[arg-type]
                )
            )
            
            logger.info(f"✅ Upload complete: {storage_path}")
            return True
            
        except Exception as e:
            # Try to extract HTTP error details
            error_msg = str(e)
            if hasattr(e, 'response'):
                try:
                    # Try JSON first
                    error_details = e.response.json()
                    logger.error(f"Supabase API error: {error_details}")
                except:
                    # Fallback to text
                    try:
                        error_text = e.response.text
                        logger.error(f"Supabase error (HTTP {e.response.status_code}): {error_text}")
                    except:
                        pass
            logger.error(f"Error uploading from stream: {error_msg}")
            return False
    
    def download_file(self, storage_path: str) -> Optional[bytes]:
        """
        Download a file from Supabase Storage
        Automatically decompresses gzip-compressed files (.gz extension or gzip magic bytes)
        
        Args:
            storage_path: Path in storage bucket
            
        Returns:
            File data as bytes, or None if not found/error
        """
        if not self.enabled or not self.client:
            logger.warning("Supabase storage not enabled")
            return None
        
        try:
            # Try with .gz extension first if not already present
            if not storage_path.endswith('.gz'):
                gz_path = storage_path + '.gz'
                try:
                    response = with_supabase_retry(
                        lambda: self.client.storage.from_(self.bucket_name).download(gz_path)
                    )
                    # File found with .gz extension, decompress it
                    decompressed = gzip.decompress(response)
                    logger.info(f"Downloaded and decompressed .gz file from storage: {gz_path}")
                    return decompressed
                except:
                    # .gz file not found, try original path
                    pass
            
            # Download from original path
            response = with_supabase_retry(
                lambda: self.client.storage.from_(self.bucket_name).download(storage_path)
            )
            
            # Try to decompress if it looks like gzip data (magic bytes or .gz extension)
            if storage_path.endswith('.gz'):
                try:
                    decompressed = gzip.decompress(response)
                    logger.info(f"Downloaded and decompressed file from storage: {storage_path}")
                    return decompressed
                except:
                    # Decompress failed, return as-is
                    logger.info(f"Downloaded file from storage: {storage_path} (decompress failed)")
                    return response
            else:
                # Not a .gz file, return as-is
                logger.info(f"Downloaded file from storage: {storage_path}")
                return response
            
        except Exception as e:
            logger.error(f"Error downloading file from storage: {e}")
            return None
    
    def file_exists(self, storage_path: str) -> bool:
        """
        Check if a file exists in storage
        
        Args:
            storage_path: Path in storage bucket
            
        Returns:
            True if file exists, False otherwise
        """
        if not self.enabled or not self.client:
            return False
        
        try:
            # Try to list the file
            files = self.client.storage.from_(self.bucket_name).list(
                path=str(Path(storage_path).parent)
            )
            
            filename = Path(storage_path).name
            return any(f['name'] == filename for f in files)
            
        except Exception as e:
            logger.error(f"Error checking file existence: {e}")
            return False
    
    def upload_data(
        self,
        file_data: bytes,
        storage_path: str,
        content_type: str = 'application/octet-stream'
    ) -> bool:
        """
        Upload raw bytes to Supabase Storage
        
        Args:
            file_data: Binary data to upload
            storage_path: Path in storage bucket
            content_type: MIME type
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self.enabled or not self.client:
            logger.warning("Supabase storage not enabled")
            return False
        
        try:
            original_size_mb = len(file_data) / (1024 * 1024)
            
            # Compress large JSON files
            file_options = {'content-type': content_type, 'upsert': 'true'}
            if self._should_compress(file_data, storage_path):
                logger.info(f"Compressing large JSON file: {storage_path} ({original_size_mb:.2f}MB)")
                file_data = self._compress_data(file_data)
                compressed_size_mb = len(file_data) / (1024 * 1024)
                reduction_pct = ((original_size_mb - compressed_size_mb) / original_size_mb) * 100
                logger.info(f"Compressed {original_size_mb:.2f}MB → {compressed_size_mb:.2f}MB ({reduction_pct:.1f}% reduction)")
                file_options['content-encoding'] = 'gzip'
            
            logger.info(f"Uploading to Supabase: {storage_path} ({len(file_data) / (1024*1024):.2f}MB)")

            with_supabase_retry(
                lambda: self.client.storage.from_(self.bucket_name).upload(
                    path=storage_path,
                    file=file_data,
                    file_options=file_options  # type: ignore[arg-type]
                )
            )
            
            logger.info(f"✅ Upload complete: {storage_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading data to storage: {e}", exc_info=True)
            return False
    
    def delete_file(self, storage_path: str) -> bool:
        """
        Delete a file from storage
        
        Args:
            storage_path: Path in storage bucket
            
        Returns:
            True if deleted successfully, False otherwise
        """
        if not self.enabled or not self.client:
            logger.warning("Supabase storage not enabled")
            return False
        
        try:
            with_supabase_retry(
                lambda: self.client.storage.from_(self.bucket_name).remove([storage_path])
            )
            logger.info(f"Deleted file from storage: {storage_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            return False
    
    def delete_prefix(self, prefix: str) -> int:
        """
        Delete all files matching a prefix
        
        Args:
            prefix: Path prefix (e.g., 'results/abc123/')
            
        Returns:
            Number of files deleted
        """
        if not self.enabled or not self.client:
            logger.warning("Supabase storage not enabled")
            return 0
        
        try:
            parent_path = str(Path(prefix).parent)
            files = self.client.storage.from_(self.bucket_name).list(path=parent_path)
            
            files_to_delete = [
                f"{parent_path}/{f['name']}" for f in files 
                if f"{parent_path}/{f['name']}".startswith(prefix)
            ]
            
            if files_to_delete:
                self.client.storage.from_(self.bucket_name).remove(files_to_delete)
                logger.info(f"Deleted {len(files_to_delete)} files with prefix: {prefix}")
                return len(files_to_delete)
            
            return 0
            
        except Exception as e:
            logger.error(f"Error deleting prefix: {e}")
            return 0
    
    def get_public_url(self, storage_path: str) -> Optional[str]:
        """
        Get public URL for a file (if bucket is public)
        
        Args:
            storage_path: Path in storage bucket
            
        Returns:
            Public URL or None
        """
        if not self.enabled or not self.client:
            return None
        
        try:
            response = self.client.storage.from_(self.bucket_name).get_public_url(storage_path)
            return response
            
        except Exception as e:
            logger.error(f"Error getting public URL: {e}")
            return None
