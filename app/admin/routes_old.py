"""Admin routes for managing authorization"""
from flask import render_template, redirect, url_for, flash, request, jsonify
from flask_login import login_required, current_user
from app.admin import admin_bp
from app.services.auth_validation import AuthorizationValidator
import psycopg2
import os
import secrets
import string
from datetime import datetime, timedelta
import logging
import traceback

logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a database connection"""
    return psycopg2.connect(os.environ.get('DATABASE_URL'))

def is_admin(user):
    """Check if user is admin (you can customize this logic)"""
    # For now, first registered user or specific emails are admins
    admin_emails = ['admin@example.com', 'gilrmendes@gmail.com']
    
    # Log for debugging
    logger.info(f"Checking admin status for: {user.email}, ID type: {type(user.id)}, ID value: {user.id}")
    
    # Check by email first (more reliable)
    if user.email in admin_emails:
        logger.info(f"User {user.email} is admin by email")
        return True
    
    # Check by ID (be flexible with string comparison)
    if str(user.id) == 'dd34d92c-3731-4d20-b7a8-3a30ca3d2118':
        logger.info(f"User {user.email} is admin by ID")
        return True
        
    logger.info(f"User {user.email} is NOT admin")
    return False

def admin_required(f):
    """Decorator to require admin privileges"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Por favor, faça login primeiro.', 'warning')
            return redirect(url_for('auth.login'))
        
        # Log user details for debugging
        logger.info(f"Admin check - User email: {current_user.email}, User ID: {current_user.id}")
        
        if not is_admin(current_user):
            flash('Acesso negado. Apenas administradores podem acessar esta área.', 'danger')
            return redirect(url_for('simplified.dashboard_page'))
        return f(*args, **kwargs)
    return decorated_function

@admin_bp.route('/test')
def test():
    """Test route to debug"""
    return jsonify({
        'status': 'ok',
        'route': 'admin test',
        'authenticated': current_user.is_authenticated,
        'user': current_user.email if current_user.is_authenticated else None
    })

@admin_bp.route('/simple')
def simple_test():
    """Simple test without decorators"""
    return "Simple test works!"

@admin_bp.route('/')
@login_required
def index():
    """Admin dashboard - debug version"""
    return f"""
    <html>
    <body>
    <h1>Debug Admin Page</h1>
    <p>User authenticated: {current_user.is_authenticated}</p>
    <p>User email: {current_user.email}</p>
    <p>User ID: {current_user.id}</p>
    <p>Is Admin: {is_admin(current_user)}</p>
    <a href="/admin/emails">Manage Emails</a> | 
    <a href="/admin/codes">Manage Codes</a>
    </body>
    </html>
    """

@admin_bp.route('/emails')
@login_required
@admin_required
def manage_emails():
    """Manage approved emails"""
    conn = None
    cursor = None
    emails = []
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT id, email, created_at, created_by, active, notes
            FROM approved_emails
            ORDER BY created_at DESC
        """
        cursor.execute(query)
        emails = cursor.fetchall()
        
    except Exception as e:
        logger.error(f"Error fetching approved emails: {e}")
        flash('Erro ao carregar emails aprovados', 'danger')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return render_template('admin/emails.html', emails=emails)

@admin_bp.route('/emails/add', methods=['POST'])
@login_required
@admin_required
def add_email():
    """Add approved email"""
    email = request.form.get('email')
    notes = request.form.get('notes', '')
    
    if not email:
        flash('Email é obrigatório', 'danger')
        return redirect(url_for('admin.manage_emails'))
    
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            INSERT INTO approved_emails (email, created_by, notes)
            VALUES (%s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """
        cursor.execute(query, (email, current_user.email, notes))
        conn.commit()
        
        if cursor.rowcount > 0:
            flash(f'Email {email} adicionado com sucesso!', 'success')
        else:
            flash(f'Email {email} já existe na lista', 'warning')
            
    except Exception as e:
        logger.error(f"Error adding approved email: {e}")
        flash('Erro ao adicionar email', 'danger')
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return redirect(url_for('admin.manage_emails'))

@admin_bp.route('/emails/toggle/<int:email_id>', methods=['POST'])
@login_required
@admin_required
def toggle_email(email_id):
    """Toggle email active status"""
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            UPDATE approved_emails 
            SET active = NOT active
            WHERE id = %s
        """
        cursor.execute(query, (email_id,))
        conn.commit()
        
        flash('Status atualizado com sucesso!', 'success')
        
    except Exception as e:
        logger.error(f"Error toggling email status: {e}")
        flash('Erro ao atualizar status', 'danger')
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return redirect(url_for('admin.manage_emails'))

@admin_bp.route('/codes')
@login_required
@admin_required
def manage_codes():
    """Manage invite codes"""
    conn = None
    cursor = None
    codes = []
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT id, code, used, used_by_email, used_at, expires_at, 
                   created_at, created_by, max_uses, times_used, notes
            FROM invite_codes
            ORDER BY created_at DESC
        """
        cursor.execute(query)
        codes = cursor.fetchall()
        
    except Exception as e:
        logger.error(f"Error fetching invite codes: {e}")
        flash('Erro ao carregar códigos de convite', 'danger')
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return render_template('admin/codes.html', codes=codes)

@admin_bp.route('/codes/generate', methods=['POST'])
@login_required
@admin_required
def generate_code():
    """Generate new invite code"""
    max_uses = int(request.form.get('max_uses', 1))
    days_valid = int(request.form.get('days_valid', 30))
    notes = request.form.get('notes', '')
    
    # Generate random code
    code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        expires_at = datetime.now() + timedelta(days=days_valid)
        
        query = """
            INSERT INTO invite_codes (code, expires_at, created_by, max_uses, notes)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (code, expires_at, current_user.email, max_uses, notes))
        conn.commit()
        
        flash(f'Código {code} gerado com sucesso!', 'success')
        
    except Exception as e:
        logger.error(f"Error generating invite code: {e}")
        flash('Erro ao gerar código', 'danger')
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return redirect(url_for('admin.manage_codes'))

@admin_bp.route('/codes/delete/<int:code_id>', methods=['POST'])
@login_required
@admin_required
def delete_code(code_id):
    """Delete invite code"""
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "DELETE FROM invite_codes WHERE id = %s"
        cursor.execute(query, (code_id,))
        conn.commit()
        
        flash('Código removido com sucesso!', 'success')
        
    except Exception as e:
        logger.error(f"Error deleting invite code: {e}")
        flash('Erro ao remover código', 'danger')
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return redirect(url_for('admin.manage_codes'))