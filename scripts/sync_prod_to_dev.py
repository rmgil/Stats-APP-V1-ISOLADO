#!/usr/bin/env python3
"""
Production to Development Sync Script
Copies production data to development with privacy masking
"""

import os
import sys
import subprocess
import psycopg2
from datetime import datetime
from pathlib import Path
import json
import logging
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def mask_email(email):
    """Mask email for privacy in development"""
    if '@' not in email:
        return email
    
    # Keep domain for testing different email providers
    local, domain = email.split('@', 1)
    
    # Special emails to preserve
    if email in ['gilrmendes@gmail.com']:  # Primary admin
        return email  # Keep primary admin email for testing
    
    # Hash the local part for uniqueness
    hashed = hashlib.md5(local.encode()).hexdigest()[:8]
    
    # Create masked email
    return f"user_{hashed}@example.com"

def sync_production_to_dev(prod_url=None, dev_url=None, tables_to_sync=None):
    """Sync production data to development with masking"""
    
    # Get database URLs
    if not prod_url:
        prod_url = os.environ.get('PROD_DATABASE_URL')
        if not prod_url:
            logger.error("PROD_DATABASE_URL not found in environment")
            logger.info("Set PROD_DATABASE_URL to your production database URL")
            return False
    
    if not dev_url:
        dev_url = os.environ.get('DATABASE_URL')  # Default to current DATABASE_URL
        if not dev_url:
            logger.error("DATABASE_URL not found in environment")
            return False
    
    # Default tables to sync
    if not tables_to_sync:
        tables_to_sync = ['approved_emails', 'invite_codes', 'admin_audit_log']
    
    # Safety check - don't sync TO production
    if 'prod' in dev_url.lower() or os.environ.get('REPLIT_DEPLOYMENT') == '1':
        logger.error("⚠️  ERROR: Cannot sync TO production database!")
        logger.error("This script only syncs FROM production TO development")
        return False
    
    logger.info("Starting production to development sync...")
    logger.info(f"Tables to sync: {', '.join(tables_to_sync)}")
    
    try:
        # Connect to both databases
        logger.info("Connecting to databases...")
        prod_conn = psycopg2.connect(prod_url)
        dev_conn = psycopg2.connect(dev_url)
        
        prod_cursor = prod_conn.cursor()
        dev_cursor = dev_conn.cursor()
        
        # Create sync report
        sync_report = {
            'timestamp': datetime.now().isoformat(),
            'tables_synced': [],
            'rows_copied': {},
            'errors': []
        }
        
        # Sync each table
        for table in tables_to_sync:
            try:
                logger.info(f"\nSyncing table: {table}")
                
                # Get column information
                prod_cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table,))
                columns = prod_cursor.fetchall()
                
                if not columns:
                    logger.warning(f"Table {table} not found in production")
                    continue
                
                column_names = [col[0] for col in columns]
                column_list = ', '.join(column_names)
                
                # Fetch all data from production
                prod_cursor.execute(f"SELECT {column_list} FROM {table}")
                prod_data = prod_cursor.fetchall()
                
                logger.info(f"Found {len(prod_data)} rows in production {table}")
                
                # Clear development table
                dev_cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
                
                # Prepare masked data
                masked_data = []
                for row in prod_data:
                    masked_row = list(row)
                    
                    # Apply masking based on table and column
                    for i, col_name in enumerate(column_names):
                        if col_name in ['email', 'admin_email', 'target_email', 'created_by', 'used_by_email']:
                            if masked_row[i]:
                                # Mask email addresses
                                if ',' in str(masked_row[i]):  # Multiple emails
                                    emails = str(masked_row[i]).split(',')
                                    masked_row[i] = ', '.join([mask_email(e.strip()) for e in emails])
                                else:
                                    masked_row[i] = mask_email(str(masked_row[i]))
                        
                        elif col_name in ['notes', 'details']:
                            # Clear sensitive notes
                            if masked_row[i]:
                                masked_row[i] = '[Masked for privacy]'
                        
                        elif col_name == 'ip_address':
                            # Anonymize IP addresses
                            if masked_row[i]:
                                masked_row[i] = '127.0.0.1'
                    
                    masked_data.append(tuple(masked_row))
                
                # Insert masked data into development
                if masked_data:
                    placeholders = ', '.join(['%s'] * len(column_names))
                    insert_query = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
                    
                    dev_cursor.executemany(insert_query, masked_data)
                    logger.info(f"Inserted {len(masked_data)} masked rows into development {table}")
                
                # Update sequences if table has serial columns
                for col_name, col_type in columns:
                    if 'serial' in col_type.lower() or col_name == 'id':
                        dev_cursor.execute(f"""
                            SELECT setval(pg_get_serial_sequence('{table}', '{col_name}'), 
                                         COALESCE((SELECT MAX({col_name}) FROM {table}), 0) + 1, 
                                         false)
                        """)
                
                sync_report['tables_synced'].append(table)
                sync_report['rows_copied'][table] = len(masked_data)
                
            except Exception as table_error:
                logger.error(f"Error syncing table {table}: {table_error}")
                sync_report['errors'].append(f"{table}: {str(table_error)}")
        
        # Commit changes
        dev_conn.commit()
        
        # Close connections
        prod_cursor.close()
        dev_cursor.close()
        prod_conn.close()
        dev_conn.close()
        
        # Save sync report
        report_dir = Path('backups') / 'sync_reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = report_dir / f'sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_file, 'w') as f:
            json.dump(sync_report, f, indent=2)
        
        logger.info("\n" + "="*60)
        logger.info("SYNC COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        logger.info(f"Tables synced: {len(sync_report['tables_synced'])}")
        for table, count in sync_report['rows_copied'].items():
            logger.info(f"  - {table}: {count} rows")
        
        if sync_report['errors']:
            logger.warning(f"Errors encountered: {len(sync_report['errors'])}")
            for error in sync_report['errors']:
                logger.warning(f"  - {error}")
        
        logger.info(f"\nSync report saved: {report_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return False

def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync production data to development with privacy masking')
    parser.add_argument('--prod-url', help='Production database URL (defaults to PROD_DATABASE_URL env var)')
    parser.add_argument('--dev-url', help='Development database URL (defaults to DATABASE_URL env var)')
    parser.add_argument('--tables', nargs='+', help='Tables to sync (default: approved_emails invite_codes admin_audit_log)')
    
    args = parser.parse_args()
    
    # Warning message
    print("\n" + "="*60)
    print("⚠️  PRODUCTION TO DEVELOPMENT SYNC")
    print("="*60)
    print("This will:")
    print("1. Copy data FROM production TO development")
    print("2. Mask sensitive data (emails, notes, IPs)")
    print("3. REPLACE all data in development tables")
    print("\nPress Ctrl+C to cancel, or Enter to continue...")
    
    try:
        input()
    except KeyboardInterrupt:
        print("\nSync cancelled")
        sys.exit(0)
    
    success = sync_production_to_dev(args.prod_url, args.dev_url, args.tables)
    
    if success:
        print("\n✅ Sync completed successfully")
        sys.exit(0)
    else:
        print("\n❌ Sync failed")
        sys.exit(1)

if __name__ == '__main__':
    main()