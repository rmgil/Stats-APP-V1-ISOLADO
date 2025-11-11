#!/usr/bin/env python3
"""
Database Backup Script
Creates a backup of the current database state
"""

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_backup(database_url=None, backup_dir=None):
    """Create a database backup"""
    
    # Get database URL
    if not database_url:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            logger.error("DATABASE_URL not found in environment")
            return None
    
    # Create backup directory
    if not backup_dir:
        backup_dir = Path('backups')
    else:
        backup_dir = Path(backup_dir)
    
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate backup filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = backup_dir / f'backup_{timestamp}.sql'
    manifest_file = backup_dir / f'backup_{timestamp}_manifest.json'
    
    logger.info(f"Creating backup: {backup_file}")
    
    try:
        # Run pg_dump command
        # --no-owner: Don't output ownership commands
        # --clean: Include commands to clean (drop) objects before creating
        # --if-exists: Use IF EXISTS when dropping objects
        # --verbose: Verbose mode for logging
        result = subprocess.run(
            ['pg_dump', database_url, '--no-owner', '--clean', '--if-exists', '--verbose'],
            stdout=open(backup_file, 'w'),
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        
        # Create backup manifest
        manifest = {
            'timestamp': timestamp,
            'datetime': datetime.now().isoformat(),
            'backup_file': str(backup_file),
            'database_url': database_url.split('@')[1] if '@' in database_url else 'local',
            'file_size': backup_file.stat().st_size,
            'environment': os.environ.get('REPLIT_DEPLOYMENT', 'development')
        }
        
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"Backup created successfully: {backup_file}")
        logger.info(f"Backup size: {backup_file.stat().st_size / 1024 / 1024:.2f} MB")
        
        # Clean up old backups (keep last 10)
        cleanup_old_backups(backup_dir, keep=10)
        
        return str(backup_file)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Backup failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        if backup_file.exists():
            backup_file.unlink()
        return None
    except Exception as e:
        logger.error(f"Unexpected error during backup: {e}")
        if backup_file.exists():
            backup_file.unlink()
        return None

def cleanup_old_backups(backup_dir, keep=10):
    """Remove old backup files, keeping only the most recent ones"""
    try:
        # Find all backup files
        backup_files = sorted(backup_dir.glob('backup_*.sql'), key=lambda x: x.stat().st_mtime, reverse=True)
        manifest_files = sorted(backup_dir.glob('backup_*_manifest.json'), key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Remove old backups
        for backup_file in backup_files[keep:]:
            logger.info(f"Removing old backup: {backup_file}")
            backup_file.unlink()
        
        for manifest_file in manifest_files[keep:]:
            manifest_file.unlink()
            
    except Exception as e:
        logger.warning(f"Error cleaning up old backups: {e}")

def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Backup PostgreSQL database')
    parser.add_argument('--database-url', help='Database URL (defaults to DATABASE_URL env var)')
    parser.add_argument('--backup-dir', default='backups', help='Directory to store backups (default: backups)')
    parser.add_argument('--keep', type=int, default=10, help='Number of backups to keep (default: 10)')
    
    args = parser.parse_args()
    
    backup_file = create_backup(args.database_url, args.backup_dir)
    
    if backup_file:
        print(f"✅ Backup created: {backup_file}")
        sys.exit(0)
    else:
        print("❌ Backup failed")
        sys.exit(1)

if __name__ == '__main__':
    main()