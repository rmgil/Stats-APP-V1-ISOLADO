import zipfile
import rarfile
import shutil
import magic
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def extract_archive(archive_path: Path, destination: Path) -> bool:
    """
    Extract various archive formats to destination directory.
    Returns True if successful, False otherwise.
    """
    try:
        # Ensure destination exists
        destination.mkdir(parents=True, exist_ok=True)
        
        # Detect file type
        mime_type = magic.from_file(str(archive_path), mime=True)
        file_extension = archive_path.suffix.lower()
        
        logger.debug(f"Extracting {archive_path.name} (MIME: {mime_type}, ext: {file_extension})")
        
        if mime_type == 'application/zip' or file_extension == '.zip':
            return extract_zip(archive_path, destination)
        elif mime_type in ['application/x-rar', 'application/x-rar-compressed'] or file_extension == '.rar':
            return extract_rar(archive_path, destination)
        else:
            logger.warning(f"Unsupported archive format: {mime_type}")
            return False
            
    except Exception as e:
        logger.error(f"Error extracting archive {archive_path}: {e}")
        return False

def extract_zip(zip_path: Path, destination: Path) -> bool:
    """Extract ZIP file to destination."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(destination)
        logger.info(f"Successfully extracted ZIP: {zip_path.name}")
        return True
    except zipfile.BadZipFile:
        logger.error(f"Corrupted ZIP file: {zip_path.name}")
        return False
    except Exception as e:
        logger.error(f"Error extracting ZIP {zip_path.name}: {e}")
        return False

def extract_rar(rar_path: Path, destination: Path) -> bool:
    """Extract RAR file to destination."""
    try:
        with rarfile.RarFile(rar_path) as rar_ref:
            rar_ref.extractall(destination)
        logger.info(f"Successfully extracted RAR: {rar_path.name}")
        return True
    except rarfile.BadRarFile:
        logger.error(f"Corrupted RAR file: {rar_path.name}")
        return False
    except rarfile.PasswordRequired:
        logger.error(f"Password required for RAR file: {rar_path.name}")
        return False
    except Exception as e:
        logger.error(f"Error extracting RAR {rar_path.name}: {e}")
        return False

def copy_directory_contents(source: Path, destination: Path) -> bool:
    """Copy contents of source directory to destination."""
    try:
        destination.mkdir(parents=True, exist_ok=True)
        
        for item in source.iterdir():
            dest_item = destination / item.name
            if item.is_file():
                shutil.copy2(item, dest_item)
            elif item.is_dir():
                shutil.copytree(item, dest_item, dirs_exist_ok=True)
        
        logger.info(f"Successfully copied directory contents: {source.name}")
        return True
    except Exception as e:
        logger.error(f"Error copying directory {source}: {e}")
        return False
