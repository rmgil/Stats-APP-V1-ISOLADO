#!/usr/bin/env python3
"""
Database Restore Script
Restores a database from a backup file
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

def restore_backup(backup_file, database_url=None, force=False):
    """Restore database from a backup file"""
    
    # Validate backup file
    backup_path = Path(backup_file)
    if not backup_path.exists():
        logger.error(f"Backup file not found: {backup_file}")
        return False
    
    # Get database URL
    if not database_url:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            logger.error("DATABASE_URL not found in environment")
            return False
    
    # Safety check - don't restore to production without force flag
    is_production = os.environ.get('REPLIT_DEPLOYMENT') == '1'
    if is_production and not force:
        logger.error("⚠️  WARNING: Attempting to restore to PRODUCTION database!")
        logger.error("Use --force flag if you really want to restore to production")
        return False
    
    logger.info(f"Restoring from: {backup_file}")
    logger.info(f"Target database: {'PRODUCTION' if is_production else 'DEVELOPMENT'}")
    
    # Create a safety backup before restore
    if not force:
        logger.info("Creating safety backup before restore...")
        from backup_database import create_backup
        safety_backup = create_backup(database_url, Path('backups') / 'pre_restore')
        if safety_backup:
            logger.info(f"Safety backup created: {safety_backup}")
        else:
            logger.warning("Could not create safety backup, continuing anyway...")
    
    try:
        # Run psql command to restore
        # The backup file contains DROP and CREATE commands
        result = subprocess.run(
            ['psql', database_url],
            stdin=open(backup_file, 'r'),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        
        logger.info("Database restored successfully")
        
        # Run Alembic stamp to mark current migration state
        logger.info("Updating migration state...")
        stamp_result = subprocess.run(
            ['alembic', 'stamp', 'head'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False  # Don't fail if alembic has issues
        )
        
        if stamp_result.returncode == 0:
            logger.info("Migration state updated")
        else:
            logger.warning("Could not update migration state - may need manual update")
        
        # Log restore action
        log_restore(backup_file, database_url, is_production)
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Restore failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        logger.info("💡 TIP: The database might be partially restored. Check the error and consider restoring the safety backup")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during restore: {e}")
        return False

def log_restore(backup_file, database_url, is_production):
    """Log the restore operation"""
    try:
        log_dir = Path('backups') / 'restore_logs'
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'backup_file': str(backup_file),
            'environment': 'production' if is_production else 'development',
            'database': database_url.split('@')[1] if '@' in database_url else 'local',
            'success': True
        }
        
        log_file = log_dir / f'restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(log_file, 'w') as f:
            json.dump(log_entry, f, indent=2)
            
    except Exception as e:
        logger.warning(f"Could not log restore operation: {e}")

def list_available_backups(backup_dir='backups'):
    """List available backup files"""
    backup_path = Path(backup_dir)
    if not backup_path.exists():
        logger.info("No backups directory found")
        return []
    
    backups = sorted(backup_path.glob('backup_*.sql'), key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not backups:
        logger.info("No backup files found")
        return []
    
    logger.info(f"\nAvailable backups ({len(backups)} total):")
    logger.info("-" * 60)
    
    backup_list = []
    for i, backup in enumerate(backups[:10], 1):  # Show only last 10
        size_mb = backup.stat().st_size / 1024 / 1024
        mtime = datetime.fromtimestamp(backup.stat().st_mtime)
        
        # Try to load manifest for more info
        manifest_file = backup.with_suffix('.sql').parent / f"{backup.stem}_manifest.json"
        environment = "unknown"
        if manifest_file.exists():
            try:
                with open(manifest_file) as f:
                    manifest = json.load(f)
                    environment = manifest.get('environment', 'unknown')
            except:
                pass
        
        logger.info(f"{i}. {backup.name}")
        logger.info(f"   Created: {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"   Size: {size_mb:.2f} MB")
        logger.info(f"   Environment: {environment}")
        logger.info("")
        
        backup_list.append(str(backup))
    
    return backup_list

def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Restore PostgreSQL database from backup')
    parser.add_argument('backup_file', nargs='?', help='Backup file to restore from')
    parser.add_argument('--database-url', help='Database URL (defaults to DATABASE_URL env var)')
    parser.add_argument('--list', action='store_true', help='List available backups')
    parser.add_argument('--force', action='store_true', help='Force restore even to production')
    parser.add_argument('--backup-dir', default='backups', help='Directory containing backups (default: backups)')
    
    args = parser.parse_args()
    
    if args.list:
        backups = list_available_backups(args.backup_dir)
        if backups:
            print("\nTo restore a backup, run:")
            print(f"  python {sys.argv[0]} <backup_file>")
        sys.exit(0)
    
    if not args.backup_file:
        print("Error: Please specify a backup file to restore")
        print("\nTo list available backups:")
        print(f"  python {sys.argv[0]} --list")
        sys.exit(1)
    
    success = restore_backup(args.backup_file, args.database_url, args.force)
    
    if success:
        print("✅ Database restored successfully")
        sys.exit(0)
    else:
        print("❌ Restore failed")
        sys.exit(1)

if __name__ == '__main__':
    main()