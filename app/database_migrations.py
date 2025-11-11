"""
Database migration management
Handles automatic migration updates on application startup
"""

import os
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

def check_and_run_migrations():
    """Check and run pending database migrations"""
    
    # Only run migrations if we have a database URL
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.info("No DATABASE_URL found, skipping migrations")
        return False
    
    # Check if alembic is configured
    alembic_ini = Path('alembic.ini')
    if not alembic_ini.exists():
        logger.info("No alembic.ini found, skipping migrations")
        return False
    
    try:
        # Check current migration status
        logger.info("Checking database migration status...")
        
        result = subprocess.run(
            ['alembic', 'current'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            logger.warning(f"Could not check migration status: {result.stderr}")
            # Don't fail the app startup if migrations can't be checked
            return False
        
        current_status = result.stdout.strip()
        logger.info(f"Current migration status: {current_status if current_status else 'No migrations applied'}")
        
        # Check if there are pending migrations
        result = subprocess.run(
            ['alembic', 'heads'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            logger.warning(f"Could not check migration heads: {result.stderr}")
            return False
        
        heads = result.stdout.strip()
        
        # Run migrations if needed
        if 'head' not in current_status or not current_status:
            logger.info("Running database migrations...")
            
            result = subprocess.run(
                ['alembic', 'upgrade', 'head'],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                logger.info("Migrations completed successfully")
                if result.stdout:
                    logger.info(f"Migration output: {result.stdout}")
                return True
            else:
                logger.error(f"Migration failed: {result.stderr}")
                # Don't crash the app if migrations fail
                # The app might still work with the current schema
                return False
        else:
            logger.info("Database is up to date, no migrations needed")
            return True
            
    except subprocess.TimeoutExpired:
        logger.error("Migration check timed out")
        return False
    except Exception as e:
        logger.error(f"Error checking migrations: {e}")
        return False

def create_backup_before_migration():
    """Create a backup before running migrations (for safety)"""
    try:
        # Only create backup in production
        if os.environ.get('REPLIT_DEPLOYMENT') != '1':
            return True
        
        logger.info("Creating pre-migration backup...")
        
        # Import backup script
        from scripts.backup_database import create_backup
        
        backup_dir = Path('backups') / 'pre_migration'
        backup_file = create_backup(backup_dir=backup_dir)
        
        if backup_file:
            logger.info(f"Pre-migration backup created: {backup_file}")
            return True
        else:
            logger.warning("Could not create pre-migration backup")
            return False
            
    except Exception as e:
        logger.warning(f"Backup before migration failed: {e}")
        # Continue anyway - don't block migrations
        return True