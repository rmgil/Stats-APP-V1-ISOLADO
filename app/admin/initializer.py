"""Initialize approved emails in production environment"""
import os
import json
import logging
import psycopg2
from pathlib import Path

logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a database connection"""
    return psycopg2.connect(os.environ.get('DATABASE_URL'))

def ensure_primary_admin():
    """
    Ensure the primary admin account always exists and is active.
    This runs in both development and production.
    """
    logger.info("Running ensure_primary_admin to guarantee primary admin is active")
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First check if gilrmendes@gmail.com exists
        cursor.execute("SELECT id, active, is_admin FROM approved_emails WHERE email = %s", ('gilrmendes@gmail.com',))
        result = cursor.fetchone()
        
        if result:
            # If exists, ensure it's active and admin
            email_id, active, is_admin = result
            if not active or not is_admin:
                cursor.execute(
                    "UPDATE approved_emails SET active = TRUE, is_admin = TRUE WHERE email = %s",
                    ('gilrmendes@gmail.com',)
                )
                conn.commit()
                logger.info("Updated gilrmendes@gmail.com to be active and admin")
        else:
            # If doesn't exist, create it
            cursor.execute(
                """INSERT INTO approved_emails (email, created_by, active, notes, is_admin)
                   VALUES (%s, %s, TRUE, %s, TRUE)""",
                ('gilrmendes@gmail.com', 'system', 'Primary administrator')
            )
            conn.commit()
            logger.info("Created primary admin account: gilrmendes@gmail.com")
        
        cursor.close()
        
    except Exception as e:
        logger.error(f"Error ensuring primary admin: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def initialize_production_emails():
    """
    Initialize approved emails in production from initial_emails.json.
    This runs only in production and only if the approved_emails table is empty.
    """
    # Always ensure primary admin exists first
    ensure_primary_admin()
    
    # Check if we're in production (Replit sets REPLIT_DEPLOYMENT=1 in production)
    is_production = os.environ.get('REPLIT_DEPLOYMENT') == '1'
    
    if not is_production:
        logger.info("Skipping email initialization (not in production)")
        return
    
    # Path to initial emails file
    initial_emails_path = Path('data/initial_emails.json')
    
    if not initial_emails_path.exists():
        logger.warning(f"Initial emails file not found: {initial_emails_path}")
        return
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if approved_emails table is empty
        cursor.execute("SELECT COUNT(*) FROM approved_emails")
        count = cursor.fetchone()[0]
        
        if count > 0:
            logger.info(f"Approved emails table already has {count} entries, skipping initialization")
            return
        
        # Load initial emails
        with open(initial_emails_path, 'r') as f:
            emails_data = json.load(f)
        
        logger.info(f"Initializing {len(emails_data)} approved emails in production")
        
        # Insert initial emails
        for email_entry in emails_data:
            cursor.execute(
                """INSERT INTO approved_emails (email, created_by, active, notes, is_admin)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (email) DO NOTHING""",
                (
                    email_entry.get('email'),
                    email_entry.get('created_by', 'system'),
                    email_entry.get('active', True),
                    email_entry.get('notes', 'Auto-imported from initial_emails.json'),
                    email_entry.get('is_admin', False)
                )
            )
        
        conn.commit()
        cursor.close()
        
        logger.info("Successfully initialized approved emails in production")
        
    except Exception as e:
        logger.error(f"Error initializing production emails: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def export_current_emails_to_file():
    """
    Export current approved emails from database to data/initial_emails.json.
    This should be run in development before deployment.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all approved emails
        cursor.execute("""
            SELECT email, created_at, created_by, active, notes, is_admin 
            FROM approved_emails
            ORDER BY email
        """)
        
        emails = cursor.fetchall()
        cursor.close()
        
        # Convert to JSON-serializable format
        emails_data = []
        for email in emails:
            email_addr, created_at, created_by, active, notes, is_admin = email
            emails_data.append({
                'email': email_addr,
                'created_by': created_by or 'system',
                'active': active,
                'notes': notes or 'Exported from development',
                'is_admin': is_admin
            })
        
        # Ensure data directory exists
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True)
        
        # Write to initial_emails.json
        output_path = data_dir / 'initial_emails.json'
        with open(output_path, 'w') as f:
            json.dump(emails_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(emails_data)} emails to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting emails: {e}")
        return False
    finally:
        if conn:
            conn.close()