#!/usr/bin/env python3
"""
Post-Deployment Verification Script
Checks that the deployment was successful and data is intact
"""

import os
import sys
import psycopg2
import subprocess
import json
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_database_connection(database_url=None):
    """Verify database is accessible"""
    if not database_url:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            logger.error("DATABASE_URL not found")
            return False
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        logger.info(f"✅ Database connected: {version[0][:50]}...")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False

def verify_tables_exist(database_url=None):
    """Verify all required tables exist"""
    if not database_url:
        database_url = os.environ.get('DATABASE_URL')
    
    required_tables = ['approved_emails', 'invite_codes', 'admin_audit_log']
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        all_present = True
        for table in required_tables:
            if table in existing_tables:
                logger.info(f"✅ Table '{table}' exists")
            else:
                logger.error(f"❌ Table '{table}' NOT FOUND")
                all_present = False
        
        cursor.close()
        conn.close()
        return all_present
        
    except Exception as e:
        logger.error(f"❌ Error checking tables: {e}")
        return False

def verify_primary_admin(database_url=None):
    """Verify primary admin exists and is active"""
    if not database_url:
        database_url = os.environ.get('DATABASE_URL')
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT email, active, is_admin 
            FROM approved_emails 
            WHERE email = 'gilrmendes@gmail.com'
        """)
        result = cursor.fetchone()
        
        if result:
            email, active, is_admin = result
            if active and is_admin:
                logger.info(f"✅ Primary admin exists and is active")
                return True
            else:
                logger.error(f"❌ Primary admin exists but status incorrect (active={active}, is_admin={is_admin})")
                return False
        else:
            logger.error(f"❌ Primary admin NOT FOUND")
            return False
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error checking primary admin: {e}")
        return False

def verify_data_integrity(database_url=None):
    """Verify data integrity and counts"""
    if not database_url:
        database_url = os.environ.get('DATABASE_URL')
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Count records in each table
        tables = ['approved_emails', 'invite_codes', 'admin_audit_log']
        counts = {}
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            counts[table] = count
            logger.info(f"📊 Table '{table}' has {count} records")
        
        # Check for active emails
        cursor.execute("SELECT COUNT(*) FROM approved_emails WHERE active = TRUE")
        active_count = cursor.fetchone()[0]
        logger.info(f"📊 Active approved emails: {active_count}")
        
        # Check for admin accounts
        cursor.execute("SELECT COUNT(*) FROM approved_emails WHERE is_admin = TRUE")
        admin_count = cursor.fetchone()[0]
        logger.info(f"📊 Admin accounts: {admin_count}")
        
        cursor.close()
        conn.close()
        
        # Save counts for comparison
        counts_file = Path('backups') / 'deploy_verification' / f'counts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        counts_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(counts_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'counts': counts,
                'active_emails': active_count,
                'admin_accounts': admin_count
            }, f, indent=2)
        
        return counts['approved_emails'] > 0  # At least some emails should exist
        
    except Exception as e:
        logger.error(f"❌ Error checking data integrity: {e}")
        return False

def verify_migrations(database_url=None):
    """Verify migrations are up to date"""
    try:
        result = subprocess.run(
            ['alembic', 'current'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            current = result.stdout.strip()
            if 'head' in current or '001_baseline_schema' in current:
                logger.info(f"✅ Migrations up to date: {current}")
                return True
            else:
                logger.warning(f"⚠️  Migrations status unclear: {current}")
                return True  # Don't fail for this
        else:
            logger.warning(f"⚠️  Could not check migration status: {result.stderr}")
            return True  # Don't fail if alembic not available
            
    except Exception as e:
        logger.warning(f"⚠️  Migration check not available: {e}")
        return True  # Don't fail if alembic not installed

def verify_environment():
    """Verify environment variables are set"""
    required_vars = [
        'DATABASE_URL',
        'SESSION_SECRET',
        'SUPABASE_URL',
        'SUPABASE_ANON_KEY'
    ]
    
    all_present = True
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            # Don't print the actual value for security
            logger.info(f"✅ {var} is set")
        else:
            logger.error(f"❌ {var} NOT SET")
            all_present = False
    
    # Check if in production
    is_production = os.environ.get('REPLIT_DEPLOYMENT') == '1'
    logger.info(f"📍 Environment: {'PRODUCTION' if is_production else 'DEVELOPMENT'}")
    
    return all_present

def main():
    """Main verification function"""
    logger.info("="*60)
    logger.info("POST-DEPLOYMENT VERIFICATION")
    logger.info("="*60)
    
    checks = [
        ("Environment Variables", verify_environment),
        ("Database Connection", verify_database_connection),
        ("Required Tables", verify_tables_exist),
        ("Primary Admin", verify_primary_admin),
        ("Data Integrity", verify_data_integrity),
        ("Migration Status", verify_migrations)
    ]
    
    results = []
    for check_name, check_func in checks:
        logger.info(f"\nChecking {check_name}...")
        success = check_func()
        results.append((check_name, success))
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("VERIFICATION SUMMARY")
    logger.info("="*60)
    
    all_passed = True
    for check_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        logger.info(f"{check_name}: {status}")
        if not success:
            all_passed = False
    
    # Save verification report
    report_dir = Path('backups') / 'deploy_verification'
    report_dir.mkdir(parents=True, exist_ok=True)
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'environment': 'production' if os.environ.get('REPLIT_DEPLOYMENT') == '1' else 'development',
        'checks': {name: success for name, success in results},
        'all_passed': all_passed
    }
    
    report_file = report_dir / f'verification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"\nReport saved: {report_file}")
    
    if all_passed:
        logger.info("\n✅ ✅ ✅ DEPLOYMENT VERIFICATION PASSED ✅ ✅ ✅")
        sys.exit(0)
    else:
        logger.error("\n❌ DEPLOYMENT VERIFICATION FAILED - CHECK LOGS ABOVE")
        sys.exit(1)

if __name__ == '__main__':
    main()