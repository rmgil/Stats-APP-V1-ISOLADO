"""Service for validating registration authorization"""
import psycopg2
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class AuthorizationValidator:
    """Validates if a user is authorized to register"""
    
    @staticmethod
    def get_db_connection():
        """Get a database connection"""
        return psycopg2.connect(os.environ.get('DATABASE_URL'))
    
    @staticmethod
    def check_approved_email(email):
        """Check if email is in the approved list"""
        conn = None
        cursor = None
        try:
            conn = AuthorizationValidator.get_db_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT id, active 
                FROM approved_emails 
                WHERE LOWER(email) = LOWER(%s) AND active = TRUE
            """
            cursor.execute(query, (email,))
            result = cursor.fetchone()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error checking approved email: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    @staticmethod
    def validate_invite_code(code):
        """Check if invite code is valid and not used/expired"""
        conn = None
        cursor = None
        try:
            conn = AuthorizationValidator.get_db_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT id, max_uses, times_used
                FROM invite_codes 
                WHERE code = %s 
                AND (expires_at IS NULL OR expires_at > %s)
                AND (max_uses IS NULL OR times_used < max_uses)
            """
            cursor.execute(query, (code, datetime.now()))
            result = cursor.fetchone()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error validating invite code: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    @staticmethod
    def mark_code_as_used(code, email):
        """Mark invite code as used by a specific email"""
        conn = None
        cursor = None
        try:
            conn = AuthorizationValidator.get_db_connection()
            cursor = conn.cursor()
            
            # Update the invite code usage
            query = """
                UPDATE invite_codes 
                SET times_used = times_used + 1,
                    used = CASE 
                        WHEN times_used + 1 >= max_uses THEN TRUE
                        ELSE used
                    END,
                    used_by_email = CASE
                        WHEN used_by_email IS NULL THEN %s
                        ELSE used_by_email || ', ' || %s
                    END,
                    used_at = %s
                WHERE code = %s
                AND (max_uses IS NULL OR times_used < max_uses)
            """
            cursor.execute(query, (email, email, datetime.now(), code))
            
            conn.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"Error marking code as used: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    @staticmethod
    def is_authorized_to_register(email, invite_code=None):
        """
        Check if a user is authorized to register
        Returns tuple: (is_authorized, reason)
        """
        # Check email status
        email_status = AuthorizationValidator.get_email_status(email)
        
        if email_status == 'authorized':
            # Email is pre-approved and active
            return True, "Email pré-aprovado"
        
        elif email_status == 'deactivated':
            # Email exists but is deactivated - cannot register
            return False, "Email desativado. Entre em contato com o administrador para reativar sua conta."
        
        # Email not in list (email_status == 'not_found')
        # Check if they have a valid invite code
        if invite_code and invite_code.strip():
            if AuthorizationValidator.validate_invite_code(invite_code):
                return True, "Código de convite válido"
            else:
                return False, "Código de convite inválido ou expirado"
        
        # Neither approved email nor valid code
        return False, "Email não autorizado. É necessário um código de convite válido ou ter o email pré-aprovado."
    
    @staticmethod
    def get_email_status(email):
        """
        Get detailed email status
        Returns: ('authorized', 'deactivated', 'not_found')
        """
        conn = None
        cursor = None
        try:
            conn = AuthorizationValidator.get_db_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT id, active 
                FROM approved_emails 
                WHERE LOWER(email) = LOWER(%s)
            """
            cursor.execute(query, (email,))
            result = cursor.fetchone()
            
            if result is None:
                return 'not_found'  # Email not in the list at all
            
            email_id, is_active = result
            if is_active:
                return 'authorized'  # Email is in the list and active
            else:
                return 'deactivated'  # Email is in the list but deactivated
            
        except Exception as e:
            logger.error(f"Error checking email status: {e}")
            return 'not_found'  # Default to not found on error
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()