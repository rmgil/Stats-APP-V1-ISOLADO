"""Custom decorators for authentication and authorization"""
from functools import wraps
from flask import redirect, url_for, flash, request
from flask_login import current_user
from app.services.supabase_client import supabase_service
import logging
import os

logger = logging.getLogger(__name__)

def email_confirmation_required(f):
    """
    Decorator to ensure user has confirmed their email before accessing a route.
    Must be used after @login_required.
    
    In development/preview mode (.replit.dev), email confirmation is bypassed
    to avoid redirect loops. In production (.replit.app), full verification is enforced.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Por favor, faça login primeiro.', 'warning')
            return redirect(url_for('auth.login'))
        
        # Check if we're in Replit preview/development environment
        is_replit_preview = (
            '.replit.dev' in request.host or 
            'localhost' in request.host or
            os.environ.get('REPLIT_DEV_DOMAIN') or
            (os.environ.get('REPL_ID') and not os.environ.get('DEPLOYMENT'))
        )
        
        # Skip email confirmation check in development/preview
        if is_replit_preview:
            logger.debug("Skipping email confirmation check in development environment")
            return f(*args, **kwargs)
        
        # Production environment - enforce email confirmation
        try:
            user_response = supabase_service.get_user()
            if user_response and user_response.user:
                # Check if email is confirmed
                if not user_response.user.email_confirmed_at:
                    # Store email in session for confirmation page
                    from flask import session
                    session['pending_confirmation_email'] = user_response.user.email
                    
                    flash('Por favor, confirme o seu email antes de continuar. Verifique a sua caixa de entrada.', 'warning')
                    return redirect(url_for('auth.email_confirmation_pending'))
            else:
                # No valid session, redirect to login
                flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
                return redirect(url_for('auth.login'))
        except Exception as e:
            logger.error(f"Error checking email confirmation status: {e}")
            flash('Erro ao verificar estado de confirmação. Por favor, faça login novamente.', 'danger')
            return redirect(url_for('auth.login'))
        
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """
    Decorator to ensure user is an admin.
    Must be used after @login_required.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Por favor, faça login primeiro.', 'warning')
            return redirect(url_for('auth.login'))
        
        # Only gilrmendes@gmail.com is admin
        admin_emails = ['gilrmendes@gmail.com']
        if current_user.email not in admin_emails:
            flash('Acesso negado. Apenas administradores.', 'danger')
            return redirect(url_for('simplified.dashboard_page'))
        
        return f(*args, **kwargs)
    return decorated_function