"""Safe ZIP extraction utilities."""
import zipfile
import os
from pathlib import Path


def safe_unzip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """
    Safely extract ZIP contents preventing ZipSlip attacks.
    
    Args:
        zip_path: Path to ZIP file
        dest_dir: Destination directory
        
    Returns:
        List of extracted file paths
        
    Raises:
        ValueError: If unsafe path detected
    """
    dest_dir = dest_dir.resolve()
    extracted = []
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for member in zf.namelist():
            # Skip directories
            if member.endswith('/'):
                continue
                
            # Sanitize path
            member_path = Path(member)
            
            # Reject absolute paths
            if member_path.is_absolute():
                raise ValueError(f"Unsafe ZIP: absolute path {member}")
                
            # Reject paths with ..
            if '..' in member_path.parts:
                raise ValueError(f"Unsafe ZIP: parent directory reference in {member}")
                
            # Extract only filename (flatten structure)
            safe_name = member_path.name
            if not safe_name:
                continue
                
            target_path = dest_dir / safe_name
            
            # Ensure target is within dest_dir
            if not str(target_path.resolve()).startswith(str(dest_dir)):
                raise ValueError(f"Unsafe ZIP: path escape attempt {member}")
                
            # Extract file
            with zf.open(member) as source, open(target_path, 'wb') as target:
                target.write(source.read())
                
            extracted.append(target_path)
            
    return extracted