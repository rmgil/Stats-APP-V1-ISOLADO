"""
File hash service for deduplication
"""
import hashlib
import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

class FileHashService:
    """
    Service for calculating file hashes to detect duplicates
    
    Usage:
        hash_service = FileHashService()
        
        # Calculate hash of a file
        file_hash = hash_service.calculate_hash('/path/to/file.zip')
        
        # Calculate hash from file-like object
        file_hash = hash_service.calculate_hash_from_stream(file_obj)
    """
    
    @staticmethod
    def calculate_hash(file_path: Union[str, Path], algorithm: str = 'sha256') -> str:
        """
        Calculate hash of a file
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm (default: sha256)
            
        Returns:
            Hex digest of file hash
        """
        try:
            hash_obj = hashlib.new(algorithm)
            
            with open(file_path, 'rb') as f:
                # Read in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b''):
                    hash_obj.update(chunk)
            
            return hash_obj.hexdigest()
            
        except Exception as e:
            logger.error(f"Error calculating hash for {file_path}: {e}")
            raise
    
    @staticmethod
    def calculate_hash_from_stream(file_stream, algorithm: str = 'sha256') -> str:
        """
        Calculate hash from a file stream (e.g., uploaded file)
        
        Args:
            file_stream: File-like object with read() method
            algorithm: Hash algorithm (default: sha256)
            
        Returns:
            Hex digest of file hash
            
        Note:
            This method resets the stream position to the beginning after hashing
        """
        try:
            hash_obj = hashlib.new(algorithm)
            
            # Save current position
            original_position = file_stream.tell() if hasattr(file_stream, 'tell') else 0
            
            # Reset to beginning
            if hasattr(file_stream, 'seek'):
                file_stream.seek(0)
            
            # Calculate hash
            for chunk in iter(lambda: file_stream.read(8192), b''):
                hash_obj.update(chunk)
            
            # Reset to original position
            if hasattr(file_stream, 'seek'):
                file_stream.seek(original_position)
            
            return hash_obj.hexdigest()
            
        except Exception as e:
            logger.error(f"Error calculating hash from stream: {e}")
            raise
    
    @staticmethod
    def verify_hash(file_path: Union[str, Path], expected_hash: str, algorithm: str = 'sha256') -> bool:
        """
        Verify that a file matches an expected hash
        
        Args:
            file_path: Path to file
            expected_hash: Expected hash value
            algorithm: Hash algorithm (default: sha256)
            
        Returns:
            True if hash matches, False otherwise
        """
        try:
            actual_hash = FileHashService.calculate_hash(file_path, algorithm)
            return actual_hash.lower() == expected_hash.lower()
            
        except Exception as e:
            logger.error(f"Error verifying hash: {e}")
            return False
