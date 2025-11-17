"""Authentication routes"""
from flask import render_template, redirect, url_for, flash, request, session as flask_session
from flask_login import login_user, logout_user, login_required, current_user
from app.auth import auth_bp
from app.auth.forms import LoginForm, RegistrationForm
from app.services.supabase_client import supabase_service
from app.services.auth_validation import AuthorizationValidator
from app.models.user import User
import logging
import os

logger = logging.getLogger(__name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Login page"""
    if current_user.is_authenticated:
        return redirect(url_for('simplified.dashboard_page'))
    
    form = LoginForm()
    if form.validate_on_submit():
        email = form.email.data
        password = form.password.data
        
        # Authenticate with Supabase
        response, error = supabase_service.sign_in(email, password)
        
        if error:
            flash(f'Erro no login: {error}', 'danger')
            logger.error(f"Login error: {error}")
            return redirect(url_for('auth.login'))
        
        if response and response.user:
            # Check email authorization status
            email_status = AuthorizationValidator.get_email_status(email)
            
            if email_status == 'not_found':
                # Email not in the approved list at all
                supabase_service.sign_out()
                flash('Email não autorizado. É necessário ter o email pré-aprovado ou usar um código de convite para se registar.', 'danger')
                logger.warning(f"Login attempt from unauthorized email: {email}")
                return redirect(url_for('auth.login'))
            
            elif email_status == 'deactivated':
                # Email is in the list but deactivated
                supabase_service.sign_out()
                flash('A sua conta foi desativada temporariamente. Entre em contacto com o administrador.', 'warning')
                logger.warning(f"Login attempt from deactivated email: {email}")
                return redirect(url_for('auth.login'))
            
            # If we get here, email_status == 'authorized'
            
            # Check if email is confirmed
            if not response.user.email_confirmed_at:
                # Sign out immediately - user cannot access without email confirmation
                supabase_service.sign_out()
                
                # Store email temporarily for confirmation page
                flask_session['pending_confirmation_email'] = email
                
                flash('Por favor, confirme o seu email antes de fazer login. Verifique a sua caixa de entrada.', 'warning')
                return redirect(url_for('auth.email_confirmation_pending'))
            
            # Create User object from Supabase response
            user = User(response.user.dict())
            
            # Store user data in session for Flask-Login
            flask_session['user_data'] = user.to_dict()
            flask_session['supabase_session'] = response.session.dict() if response.session else None
            
            # Log in user with Flask-Login
            login_user(user, remember=form.remember_me.data)

            flash('Login realizado com sucesso!', 'success')
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('simplified.dashboard_page'))
    
    return render_template('auth/login.html', form=form)

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    """Registration page"""
    if current_user.is_authenticated:
        return redirect(url_for('simplified.dashboard_page'))
    
    form = RegistrationForm()
    if form.validate_on_submit():
        email = form.email.data
        password = form.password.data
        username = form.username.data
        invite_code = form.invite_code.data
        
        # Check if user is authorized to register
        is_authorized, reason = AuthorizationValidator.is_authorized_to_register(email, invite_code)
        
        if not is_authorized:
            flash(reason, 'danger')
            logger.warning(f"Unauthorized registration attempt: {email}, reason: {reason}")
            return redirect(url_for('auth.register'))
        
        # Register with Supabase
        metadata = {'username': username}
        response, error = supabase_service.sign_up(email, password, metadata)
        
        if error:
            flash(f'Erro no registo: {error}', 'danger')
            logger.error(f"Registration error: {error}")
            return redirect(url_for('auth.register'))
        
        if response and response.user:
            # If invite code was used, mark it as used
            if invite_code and invite_code.strip():
                AuthorizationValidator.mark_code_as_used(invite_code, email)
                logger.info(f"Invite code '{invite_code}' used by {email}")
            
            # Create User object from Supabase response
            user = User(response.user.dict())
            
            # DO NOT log in user automatically - they must confirm email first
            # Store email in session temporarily for the confirmation page
            flask_session['pending_confirmation_email'] = email
            
            flash('Conta criada com sucesso! Por favor, verifique o seu email para confirmar a sua conta.', 'success')
            return redirect(url_for('auth.email_confirmation_pending'))
    
    return render_template('auth/register.html', form=form)

@auth_bp.route('/logout')
@login_required
def logout():
    """Logout user"""
    # Sign out from Supabase
    supabase_service.sign_out()
    
    # Clear session
    flask_session.pop('user_data', None)
    flask_session.pop('supabase_session', None)
    
    # Logout from Flask-Login
    logout_user()
    
    flash('Saiu com sucesso!', 'info')
    return redirect(url_for('auth.login'))

@auth_bp.route('/email-confirmation-pending')
def email_confirmation_pending():
    """Page shown when user needs to confirm their email"""
    email = flask_session.get('pending_confirmation_email', None)
    return render_template('auth/email_confirmation_pending.html', email=email)

@auth_bp.route('/resend-confirmation', methods=['POST'])
def resend_confirmation():
    """Resend confirmation email"""
    email = flask_session.get('pending_confirmation_email', None)
    
    if not email:
        flash('Sessão expirada. Por favor, faça login novamente.', 'warning')
        return redirect(url_for('auth.login'))
    
    # Note: Supabase doesn't provide a direct resend API in the Python client
    # User will need to try logging in again to trigger a new email
    flash('Por favor, tente fazer login novamente para receber um novo email de confirmação.', 'info')
    return redirect(url_for('auth.login'))

@auth_bp.route('/profile')
@login_required
def profile():
    """User profile page"""
    return render_template('auth/profile.html', user=current_user)