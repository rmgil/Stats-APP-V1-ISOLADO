"""Simple Admin Panel - No templates, just working HTML"""
from flask import redirect, url_for, request, flash, jsonify, Response
from flask_login import login_required, current_user
from app.admin import admin_bp
import psycopg2
import os
import secrets
import string
from datetime import datetime, timedelta
import logging
import json

from app.services.supabase_history import SupabaseHistoryService
from app.services.result_storage import ResultStorageService

logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a database connection"""
    return psycopg2.connect(os.environ.get('DATABASE_URL'))

def log_admin_action(admin_email, action_type, target_email=None, details=None):
    """Log admin action to database for audit trail"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get IP address from request
        ip_address = request.remote_addr if request else None
        
        cursor.execute(
            """INSERT INTO admin_audit_log (admin_email, action_type, target_email, details, ip_address)
               VALUES (%s, %s, %s, %s, %s)""",
            (admin_email, action_type, target_email, details, ip_address)
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error logging admin action: {e}")
    finally:
        if conn:
            conn.close()

def is_admin(user):
    """Check if user is admin - checks database for approved admins"""
    # Always consider gilrmendes@gmail.com as admin
    if user.email == 'gilrmendes@gmail.com':
        return True
    
    # Check database for admin status
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT is_admin FROM approved_emails WHERE email = %s AND active = TRUE",
            (user.email,)
        )
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0]:
            return True
    except Exception as e:
        logger.error(f"Error checking admin status: {e}")
    finally:
        if conn:
            conn.close()
    
    return False

def render_admin_page(content, title="Admin Panel"):
    """Render a simple admin page with inline HTML"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{title}</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 0;
                padding: 20px;
                background: #f5f5f5;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            h1, h2 {{
                color: #333;
            }}
            .nav {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 15px;
                margin: -20px -20px 20px -20px;
                border-radius: 10px 10px 0 0;
            }}
            .nav a {{
                color: white;
                text-decoration: none;
                margin-right: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }}
            th, td {{
                text-align: left;
                padding: 12px;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background: #f8f9fa;
                font-weight: 600;
            }}
            .btn {{
                padding: 8px 16px;
                border-radius: 5px;
                text-decoration: none;
                border: none;
                cursor: pointer;
                font-size: 14px;
                display: inline-block;
                margin: 2px;
            }}
            .btn-sm {{
                padding: 5px 10px;
                font-size: 12px;
            }}
            .btn-primary {{
                background: #667eea;
                color: white;
            }}
            .btn-danger {{
                background: #dc3545;
                color: white;
            }}
            .btn-success {{
                background: #28a745;
                color: white;
            }}
            .alert {{
                padding: 15px;
                margin: 20px 0;
                border-radius: 5px;
            }}
            .alert-success {{
                background: #d4edda;
                color: #155724;
                border: 1px solid #c3e6cb;
            }}
            .alert-danger {{
                background: #f8d7da;
                color: #721c24;
                border: 1px solid #f5c6cb;
            }}
            .form-group {{
                margin: 15px 0;
            }}
            input, select {{
                padding: 8px;
                border: 1px solid #ddd;
                border-radius: 5px;
                margin-right: 10px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="nav">
                <h1 style="margin: 0; display: inline;">🎰 Admin Panel</h1>
                <div style="float: right;">
                    <a href="/dashboard">Dashboard</a>
                    <a href="/admin/emails">Emails</a>
                    <a href="/admin/codes">Códigos</a>
                    <a href="/admin/uploads">Uploads</a>
                    <a href="/auth/logout">Sair</a>
                </div>
                <div style="clear: both;"></div>
            </div>
            {content}
        </div>
    </body>
    </html>
    """

@admin_bp.route('/')
@admin_bp.route('/index')
@login_required
def index():
    """Admin dashboard - redirect to emails page"""
    if not is_admin(current_user):
        flash('Acesso negado. Apenas administradores.', 'danger')
        return redirect(url_for('simplified.dashboard_page'))
    
    # Redirect directly to emails page since that's the main admin page
    return redirect(url_for('admin.manage_emails'))

@admin_bp.route('/uploads')
@login_required
def uploads():
    """Admin view listing all user uploads."""

    if not is_admin(current_user):
        flash('Acesso negado. Apenas administradores.', 'danger')
        return redirect(url_for('simplified.dashboard_page'))

    history_service = SupabaseHistoryService()
    result_storage = ResultStorageService()

    messages = []

    action = request.args.get('action')
    token_to_delete = request.args.get('token')

    if action == 'delete' and token_to_delete:
        if history_service.enabled:
            success = history_service.delete_processing(token_to_delete)
            if success:
                cleanup_stats = result_storage.delete_processing_results(token_to_delete)
                msg_parts = ['Upload removido com sucesso.']
                if cleanup_stats.get('storage_deleted'):
                    msg_parts.append(f"{cleanup_stats['storage_deleted']} ficheiros apagados do storage")
                if cleanup_stats.get('local_deleted'):
                    msg_parts.append('dados locais removidos')
                messages.append(('success', ' | '.join(msg_parts)))
            else:
                messages.append(('danger', 'Não foi possível remover o upload.'))
        else:
            messages.append(('danger', 'Serviço de histórico não está configurado.'))

    try:
        page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        page = 1

    try:
        per_page = max(10, min(100, int(request.args.get('per_page', 25))))
    except ValueError:
        per_page = 25

    offset = (page - 1) * per_page
    search_term = request.args.get('search', '').strip()
    status_filter = request.args.get('status', '').strip()
    status_filter = status_filter if status_filter in {'completed', 'processing', 'failed', 'cancelled'} else ''
    status_param = status_filter if status_filter else None

    uploads = []
    if history_service.enabled:
        uploads = history_service.get_all_history(
            limit=per_page,
            offset=offset,
            search=search_term or None,
            status=status_param,
        )
    else:
        messages.append(('danger', 'Serviço de histórico não está configurado.'))

    alerts = ''
    for category, text in messages:
        class_name = 'alert-success' if category == 'success' else 'alert-danger'
        alerts += f'<div class="alert {class_name}">{text}</div>'

    status_options = ''
    for value, label in [
        ('', 'Todos os estados'),
        ('completed', 'Concluído'),
        ('processing', 'Em processamento'),
        ('failed', 'Falhou'),
        ('cancelled', 'Cancelado'),
    ]:
        selected = 'selected' if status_filter == value else ''
        status_options += f'<option value="{value}" {selected}>{label}</option>'

    search_form = f'''
        <form method="get" style="margin-bottom: 20px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
            <input type="text" name="search" value="{search_term}" placeholder="Filtrar por email" style="flex: 1; min-width: 220px; padding: 8px;">
            <select name="status" style="padding: 8px;">
                {status_options}
            </select>
            <input type="hidden" name="per_page" value="{per_page}">
            <button type="submit" class="btn btn-primary btn-sm">Filtrar</button>
            <a href="/admin/uploads" class="btn btn-sm">Limpar</a>
        </form>
    '''

    table_rows = ''
    for item in uploads:
        token = item.get('token', '')
        user_id = item.get('user_id', '—')
        filename = item.get('filename', '—')
        status = item.get('status', '—')
        total_hands = item.get('total_hands', 0) or 0
        overall_score = item.get('overall_score')
        months_summary = item.get('months_summary') or {}
        total_months = months_summary.get('total_months', '-')
        created_at = item.get('created_at') or ''

        try:
            if created_at:
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
        except Exception:
            pass

        score_display = f"{overall_score:.1f}" if overall_score is not None else '—'
        delete_link = f"/admin/uploads?action=delete&token={token}&page={page}&per_page={per_page}&search={search_term}&status={status_filter}"

        table_rows += f"""
            <tr>
                <td>{created_at}</td>
                <td>{user_id}</td>
                <td>{filename}</td>
                <td>{status}</td>
                <td>{total_hands}</td>
                <td>{score_display}</td>
                <td>{total_months}</td>
                <td>{token}</td>
                <td><a href="{delete_link}" class="btn btn-danger btn-sm" onclick="return confirm('Remover este upload?');">Apagar</a></td>
            </tr>
        """

    if not table_rows:
        table_rows = '<tr><td colspan="9" style="text-align:center; color:#777;">Sem uploads encontrados.</td></tr>'

    table_html = f'''
        <table>
            <thead>
                <tr>
                    <th>Criado em</th>
                    <th>Utilizador</th>
                    <th>Ficheiro</th>
                    <th>Estado</th>
                    <th>Mãos</th>
                    <th>Score</th>
                    <th>Meses</th>
                    <th>Token</th>
                    <th>Ações</th>
                </tr>
            </thead>
            <tbody>{table_rows}</tbody>
        </table>
    '''

    pagination_html = f'''
        <div style="margin-top: 20px; display: flex; justify-content: space-between; align-items: center;">
            <div>Página {page}</div>
            <div>
                <a class="btn btn-sm" href="/admin/uploads?page={max(1, page-1)}&per_page={per_page}&search={search_term}&status={status_filter}">Anterior</a>
                <a class="btn btn-sm" href="/admin/uploads?page={page+1}&per_page={per_page}&search={search_term}&status={status_filter}">Seguinte</a>
            </div>
        </div>
    '''

    content = f"""
        <h2>📁 Uploads de Utilizadores</h2>
        <p>Lista de todos os processamentos associados aos utilizadores. Apenas administradores podem remover uploads.</p>
        {alerts}
        {search_form}
        {table_html}
        {pagination_html}
    """

    return render_admin_page(content, title="Uploads dos Utilizadores")


@admin_bp.route('/emails', methods=['GET', 'POST'])
@login_required
def manage_emails():
    """Manage approved emails"""
    if not is_admin(current_user):
        flash('Acesso negado.', 'danger')
        return redirect(url_for('simplified.dashboard_page'))
    
    # Get messages for display
    messages = []
    
    # Handle bulk upload via POST
    if request.method == 'POST' and request.form.get('action') == 'bulk_add':
        emails_text = request.form.get('emails_list', '')
        if emails_text:
            # Process emails - split by newlines, commas, or semicolons
            import re
            emails_list = re.split('[,;\n\r]+', emails_text)
            
            added_count = 0
            skipped_count = 0
            conn = None
            
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                for email in emails_list:
                    email = email.strip()
                    if email and '@' in email:  # Basic email validation
                        try:
                            cursor.execute(
                                "INSERT INTO approved_emails (email, created_by, active) VALUES (%s, %s, TRUE) ON CONFLICT (email) DO NOTHING",
                                (email.lower(), current_user.email)
                            )
                            if cursor.rowcount > 0:
                                added_count += 1
                                # Log the action
                                log_admin_action(current_user.email, 'EMAIL_ADDED', email.lower(), 'Bulk add')
                            else:
                                skipped_count += 1
                        except Exception as e:
                            logger.error(f"Error adding email {email}: {e}")
                
                conn.commit()
                
                if added_count > 0:
                    messages.append(('success', f'✅ {added_count} emails adicionados com sucesso!'))
                if skipped_count > 0:
                    messages.append(('info', f'ℹ️ {skipped_count} emails já existentes foram ignorados.'))
                
            except Exception as e:
                messages.append(('danger', f'Erro ao adicionar emails: {str(e)}'))
            finally:
                if conn:
                    conn.close()
    
    # Handle single email submission (GET)
    if request.args.get('action') == 'add':
        email = request.args.get('email')
        if email:
            conn = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO approved_emails (email, created_by, active) VALUES (%s, %s, TRUE) ON CONFLICT DO NOTHING",
                    (email, current_user.email)
                )
                conn.commit()
                
                if cursor.rowcount > 0:
                    # Log the action
                    log_admin_action(current_user.email, 'EMAIL_ADDED', email, 'Single add')
                    messages.append(('success', f'Email {email} adicionado com sucesso!'))
                else:
                    messages.append(('info', f'Email {email} já existe'))
            except Exception as e:
                messages.append(('danger', f'Erro ao adicionar: {str(e)}'))
            finally:
                if conn:
                    conn.close()
    
    # Handle toggle action
    if request.args.get('action') == 'toggle':
        email_id = request.args.get('id')
        if email_id:
            conn = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                # First get the email and current status
                cursor.execute("SELECT email, active FROM approved_emails WHERE id = %s", (email_id,))
                result = cursor.fetchone()
                if result:
                    email, was_active = result
                    cursor.execute(
                        "UPDATE approved_emails SET active = NOT active WHERE id = %s",
                        (email_id,)
                    )
                    conn.commit()
                    # Log the action
                    action_type = 'EMAIL_DEACTIVATED' if was_active else 'EMAIL_ACTIVATED'
                    log_admin_action(current_user.email, action_type, email, f'Toggled status')
                    messages.append(('success', 'Status atualizado!'))
            except Exception as e:
                messages.append(('danger', f'Erro: {str(e)}'))
            finally:
                if conn:
                    conn.close()
    
    # Handle toggle admin action
    if request.args.get('action') == 'toggle_admin':
        email_id = request.args.get('id')
        if email_id:
            conn = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                # First get the email and current admin status
                cursor.execute("SELECT email, is_admin FROM approved_emails WHERE id = %s", (email_id,))
                result = cursor.fetchone()
                if result:
                    email, was_admin = result
                    # Don't allow removing admin from gilrmendes@gmail.com
                    if email == 'gilrmendes@gmail.com' and was_admin:
                        messages.append(('warning', 'Não é possível remover admin do usuário principal!'))
                    else:
                        cursor.execute(
                            "UPDATE approved_emails SET is_admin = NOT is_admin WHERE id = %s",
                            (email_id,)
                        )
                        conn.commit()
                        # Log the action
                        action_type = 'ADMIN_GRANTED' if not was_admin else 'ADMIN_REMOVED'
                        log_admin_action(current_user.email, action_type, email, f'Toggled admin status')
                        messages.append(('success', f'Permissão admin {"concedida" if not was_admin else "removida"}!'))
            except Exception as e:
                messages.append(('danger', f'Erro: {str(e)}'))
            finally:
                if conn:
                    conn.close()
    
    # Handle delete action
    if request.args.get('action') == 'delete':
        email_id = request.args.get('id')
        if email_id:
            conn = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                # First get the email for deletion from Supabase
                cursor.execute("SELECT email FROM approved_emails WHERE id = %s", (email_id,))
                result = cursor.fetchone()
                
                if result:
                    email_to_delete = result[0]
                    
                    # Don't allow deleting gilrmendes@gmail.com
                    if email_to_delete == 'gilrmendes@gmail.com':
                        messages.append(('warning', 'Não é possível remover o administrador principal!'))
                    else:
                        # Delete from approved_emails
                        cursor.execute("DELETE FROM approved_emails WHERE id = %s", (email_id,))
                        conn.commit()
                        
                        # Try to delete user from Supabase Auth
                        try:
                            from app.services.supabase_client import supabase_service
                            
                            logger.info(f"Attempting to delete user {email_to_delete} from Supabase Auth")
                            
                            # Find user by email
                            user = supabase_service.admin_get_user_by_email(email_to_delete)
                            if user:
                                logger.info(f"Found user {email_to_delete} with ID {user.id} in Supabase")
                                # Delete the user from Supabase
                                delete_response = supabase_service.admin_delete_user(user.id)
                                if delete_response:
                                    logger.info(f"Successfully deleted user {email_to_delete} from Supabase Auth")
                                    messages.append(('info', f'Usuário {email_to_delete} removido do sistema de autenticação'))
                                else:
                                    logger.warning(f"Could not delete user {email_to_delete} from Supabase Auth - may need service role key")
                                    messages.append(('warning', 'Usuário pode precisar se registrar novamente'))
                            else:
                                logger.info(f"User {email_to_delete} not found in Supabase Auth (already deleted or never existed)")
                        except Exception as supabase_error:
                            logger.error(f"Error deleting user from Supabase: {supabase_error}")
                            messages.append(('warning', 'Não foi possível remover do sistema de autenticação'))
                            # Continue even if Supabase deletion fails
                        
                        # Log the action
                        log_admin_action(current_user.email, 'EMAIL_DELETED', email_to_delete, 'User and email removed')
                        messages.append(('success', f'Email {email_to_delete} removido permanentemente!'))
                else:
                    messages.append(('warning', 'Email não encontrado'))
                    
            except Exception as e:
                messages.append(('danger', f'Erro ao remover: {str(e)}'))
                logger.error(f"Error deleting email: {e}")
            finally:
                if conn:
                    conn.close()
    
    # Get all emails
    emails = []
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, email, active, created_at, created_by, notes, is_admin FROM approved_emails ORDER BY created_at DESC"
        )
        emails = cursor.fetchall()
    except Exception as e:
        messages.append(('danger', f'Erro ao carregar emails: {str(e)}'))
    finally:
        if conn:
            conn.close()
    
    # Build messages HTML
    messages_html = ""
    for msg_type, msg_text in messages:
        messages_html += f'<div class="alert alert-{msg_type}">{msg_text}</div>'
    
    # Build emails table
    emails_html = """
        <table>
            <thead>
                <tr>
                    <th>Email</th>
                    <th>Status</th>
                    <th>Admin</th>
                    <th>Criado em</th>
                    <th>Criado por</th>
                    <th>Notas</th>
                    <th>Ações</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for email in emails:
        email_id, email_addr, active, created_at, created_by, notes, user_is_admin = email
        status = "✅ Ativo" if active else "❌ Inativo"
        admin_status = "👑 Admin" if user_is_admin else "👤 Usuário"
        created_at_str = created_at.strftime('%d/%m/%Y %H:%M') if created_at else '-'
        
        # Special styling for admin users
        row_style = 'background: #f0f8ff;' if user_is_admin else ''
        
        # Build onclick attribute
        onclick_attr = 'onclick="return confirm(\'Tem certeza que deseja remover permissão admin?\')"' if user_is_admin else ''
        
        emails_html += f"""
            <tr style="{row_style}">
                <td>{email_addr}</td>
                <td>{status}</td>
                <td>{admin_status}</td>
                <td>{created_at_str}</td>
                <td>{created_by or 'sistema'}</td>
                <td>{notes or '-'}</td>
                <td>
                    <a href="/admin/emails?action=toggle&id={email_id}" class="btn btn-primary btn-sm">
                        {'Desativar' if active else 'Ativar'}
                    </a>
                    <a href="/admin/emails?action=toggle_admin&id={email_id}" 
                       class="btn {'btn-danger' if user_is_admin else 'btn-success'} btn-sm"
                       {onclick_attr}>
                        {'Remover Admin' if user_is_admin else 'Dar Admin'}
                    </a>
                    <a href="/admin/emails?action=delete&id={email_id}" 
                       class="btn btn-danger btn-sm"
                       onclick="return confirm('ATENÇÃO: Esta ação irá remover permanentemente o email e o usuário precisará se registrar novamente. Tem certeza?')">
                        🗑️ Remover
                    </a>
                </td>
            </tr>
        """
    
    emails_html += "</tbody></table>"
    
    content = f"""
        <h2>Emails Aprovados</h2>
        
        {messages_html}
        
        <!-- Seção de Migração de Dados -->
        <div style="background: #fff3cd; padding: 20px; border-radius: 10px; margin: 20px 0; border: 1px solid #ffecb5;">
            <h3>🔄 Migração de Dados (Desenvolvimento → Produção)</h3>
            
            <!-- Botão de Preparação Automática para Deploy -->
            <div style="background: #d4edda; padding: 15px; border-radius: 8px; margin-bottom: 20px; border: 1px solid #c3e6cb;">
                <h4>🚀 Preparação Automática para Deploy</h4>
                <p>Salva automaticamente todos os emails no arquivo que será usado em produção</p>
                <a href="/admin/prepare-deploy" class="btn btn-success" style="width: 100%; font-size: 1.1em; padding: 12px;">
                    🚀 Preparar Deploy Automático
                </a>
                <small style="color: #155724; display: block; margin-top: 10px;">
                    <strong>✅ Recomendado:</strong> Clique aqui antes de fazer deploy. Os emails serão automaticamente carregados em produção.
                </small>
            </div>
            
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                <div>
                    <h4>📥 Exportar Manual</h4>
                    <p>Baixe emails para backup</p>
                    <a href="/admin/export-emails" class="btn btn-primary" style="width: 100%;">
                        📥 Baixar Emails (JSON)
                    </a>
                </div>
                <div>
                    <h4>📤 Importar Manual</h4>
                    <p>Importe emails de arquivo</p>
                    <form method="POST" action="/admin/import-emails" enctype="multipart/form-data">
                        <input type="file" name="emails_file" accept=".json" required 
                               style="margin-bottom: 10px; width: 100%;">
                        <button type="submit" class="btn btn-warning" style="width: 100%;">
                            📤 Importar Emails
                        </button>
                    </form>
                </div>
            </div>
            <small style="color: #856404; display: block; margin-top: 15px;">
                <strong>⚠️ Migração Manual (alternativa):</strong><br>
                1. Exporte os emails usando "Baixar Emails"<br>
                2. Faça o deploy da aplicação<br>
                3. Em produção, use "Importar Emails" com o arquivo baixado<br>
                4. Os emails serão sincronizados (duplicados são ignorados)
            </small>
        </div>
        
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0;">
            <div style="background: #f8f9fa; padding: 20px; border-radius: 10px;">
                <h3>➕ Adicionar Email Individual</h3>
                <form method="GET" action="/admin/emails">
                    <input type="hidden" name="action" value="add">
                    <input type="email" name="email" placeholder="email@exemplo.com" required 
                           style="width: calc(100% - 100px);">
                    <button type="submit" class="btn btn-success">Adicionar</button>
                </form>
            </div>
            
            <div style="background: #e8f5e9; padding: 20px; border-radius: 10px;">
                <h3>📋 Adicionar Lista de Emails</h3>
                <form method="POST" action="/admin/emails">
                    <input type="hidden" name="action" value="bulk_add">
                    <textarea name="emails_list" rows="5" 
                              placeholder="Cole aqui a lista de emails&#10;Um por linha ou separados por vírgula&#10;&#10;Exemplo:&#10;user1@example.com&#10;user2@example.com&#10;user3@example.com" 
                              style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 5px; margin-bottom: 10px;"></textarea>
                    <button type="submit" class="btn btn-success" style="width: 100%;">
                        Adicionar Lista
                    </button>
                </form>
                <small style="color: #666; display: block; margin-top: 10px;">
                    💡 Dica: Você pode colar emails separados por vírgula, ponto-e-vírgula ou um por linha.
                </small>
            </div>
        </div>
        
        <h3>📧 Emails Cadastrados ({len(emails)} total)</h3>
        {emails_html if emails else '<p>Nenhum email cadastrado.</p>'}
    """
    
    return render_admin_page(content, "Gerenciar Emails")

@admin_bp.route('/codes')
@login_required
def manage_codes():
    """Manage invite codes"""
    if not is_admin(current_user):
        flash('Acesso negado.', 'danger')
        return redirect(url_for('simplified.dashboard_page'))
    
    messages = []
    
    # Handle generate action
    if request.args.get('action') == 'generate':
        conn = None
        try:
            code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
            max_uses = int(request.args.get('max_uses', 1))
            days_valid = int(request.args.get('days_valid', 30))
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            expires_at = datetime.now() + timedelta(days=days_valid)
            
            cursor.execute(
                "INSERT INTO invite_codes (code, expires_at, created_by, max_uses) VALUES (%s, %s, %s, %s)",
                (code, expires_at, current_user.email, max_uses)
            )
            conn.commit()
            # Log the action
            log_admin_action(current_user.email, 'CODE_GENERATED', None, f'Code: {code}, Max uses: {max_uses}, Valid for: {days_valid} days')
            messages.append(('success', f'Código {code} gerado com sucesso!'))
        except Exception as e:
            messages.append(('danger', f'Erro ao gerar código: {str(e)}'))
        finally:
            if conn:
                conn.close()
    
    # Handle delete action  
    if request.args.get('action') == 'delete':
        code_id = request.args.get('id')
        if code_id:
            conn = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                # First get the code for logging
                cursor.execute("SELECT code FROM invite_codes WHERE id = %s", (code_id,))
                result = cursor.fetchone()
                if result:
                    code = result[0]
                    cursor.execute("DELETE FROM invite_codes WHERE id = %s", (code_id,))
                    conn.commit()
                    # Log the action
                    log_admin_action(current_user.email, 'CODE_DELETED', None, f'Code: {code}')
                    messages.append(('success', 'Código removido!'))
            except Exception as e:
                messages.append(('danger', f'Erro: {str(e)}'))
            finally:
                if conn:
                    conn.close()
    
    # Get all codes
    codes = []
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """SELECT id, code, used, used_by_email, expires_at, created_at, 
               created_by, max_uses, times_used, notes 
               FROM invite_codes ORDER BY created_at DESC"""
        )
        codes = cursor.fetchall()
    except Exception as e:
        messages.append(('danger', f'Erro ao carregar códigos: {str(e)}'))
    finally:
        if conn:
            conn.close()
    
    # Build messages HTML
    messages_html = ""
    for msg_type, msg_text in messages:
        messages_html += f'<div class="alert alert-{msg_type}">{msg_text}</div>'
    
    # Build codes table
    codes_html = """
        <table>
            <thead>
                <tr>
                    <th>Código</th>
                    <th>Status</th>
                    <th>Usos</th>
                    <th>Usado por</th>
                    <th>Expira em</th>
                    <th>Ações</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for code in codes:
        code_id, code_str, used, used_by, expires_at, created_at, created_by, max_uses, times_used, notes = code
        
        if used:
            status = "❌ Usado"
        elif expires_at and expires_at < datetime.now():
            status = "⏰ Expirado"
        else:
            status = "✅ Ativo"
        
        expires_str = expires_at.strftime('%d/%m/%Y') if expires_at else 'Sem prazo'
        
        codes_html += f"""
            <tr>
                <td><code style="background:#f0f0f0; padding:5px; border-radius:3px;">{code_str}</code></td>
                <td>{status}</td>
                <td>{times_used}/{max_uses or '∞'}</td>
                <td>{used_by or '-'}</td>
                <td>{expires_str}</td>
                <td>
                    <a href="/admin/codes?action=delete&id={code_id}" 
                       onclick="return confirm('Remover este código?')" 
                       class="btn btn-danger">Remover</a>
                </td>
            </tr>
        """
    
    codes_html += "</tbody></table>"
    
    content = f"""
        <h2>Códigos de Convite</h2>
        
        {messages_html}
        
        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
            <h3>Gerar Novo Código</h3>
            <form method="GET" action="/admin/codes">
                <input type="hidden" name="action" value="generate">
                <label>Máximo de usos: <input type="number" name="max_uses" value="1" min="1"></label>
                <label>Dias de validade: <input type="number" name="days_valid" value="30" min="1"></label>
                <button type="submit" class="btn btn-success">Gerar Código</button>
            </form>
        </div>
        
        <h3>Códigos Cadastrados</h3>
        {codes_html if codes else '<p>Nenhum código cadastrado.</p>'}
    """
    
    return render_admin_page(content, "Gerenciar Códigos")

@admin_bp.route('/export-emails')
@login_required
def export_emails():
    """Export all approved emails to JSON"""
    if not is_admin(current_user):
        return redirect(url_for('home'))
    
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
            email_addr, created_at, created_by, active, notes, user_is_admin = email
            emails_data.append({
                'email': email_addr,
                'created_at': created_at.isoformat() if created_at else None,
                'created_by': created_by,
                'active': active,
                'notes': notes,
                'is_admin': user_is_admin
            })
        
        # Create JSON response
        json_data = json.dumps(emails_data, indent=2, ensure_ascii=False)
        
        # Log the export action
        log_admin_action(
            current_user.email, 
            'EXPORT_EMAILS', 
            details=f"Exported {len(emails_data)} emails"
        )
        
        return Response(
            json_data,
            mimetype='application/json',
            headers={
                'Content-Disposition': 'attachment; filename=approved_emails.json'
            }
        )
        
    except Exception as e:
        logger.error(f"Error exporting emails: {e}")
        flash('Erro ao exportar emails', 'danger')
        return redirect(url_for('admin.manage_emails'))
    finally:
        if conn:
            conn.close()

@admin_bp.route('/import-emails', methods=['POST'])
@login_required
def import_emails():
    """Import approved emails from JSON"""
    if not is_admin(current_user):
        return redirect(url_for('home'))
    
    # Check if file was uploaded
    if 'emails_file' not in request.files:
        flash('Nenhum arquivo enviado', 'danger')
        return redirect(url_for('admin.manage_emails'))
    
    file = request.files['emails_file']
    
    if file.filename == '':
        flash('Nenhum arquivo selecionado', 'danger')
        return redirect(url_for('admin.manage_emails'))
    
    conn = None
    try:
        # Read JSON data
        json_content = file.read().decode('utf-8')
        emails_data = json.loads(json_content)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        imported_count = 0
        skipped_count = 0
        
        for email_entry in emails_data:
            email = email_entry.get('email')
            if not email:
                continue
            
            # Check if email already exists
            cursor.execute(
                "SELECT id FROM approved_emails WHERE email = %s",
                (email,)
            )
            
            if cursor.fetchone():
                skipped_count += 1
                continue
            
            # Insert new email
            cursor.execute(
                """INSERT INTO approved_emails (email, created_by, active, notes, is_admin)
                   VALUES (%s, %s, %s, %s, %s)""",
                (
                    email,
                    email_entry.get('created_by', current_user.email),
                    email_entry.get('active', True),
                    email_entry.get('notes', 'Importado'),
                    email_entry.get('is_admin', False)
                )
            )
            imported_count += 1
        
        conn.commit()
        cursor.close()
        
        # Log the import action
        log_admin_action(
            current_user.email,
            'IMPORT_EMAILS',
            details=f"Imported {imported_count} emails, skipped {skipped_count} duplicates"
        )
        
        flash(f'✅ Importados {imported_count} emails ({skipped_count} duplicados ignorados)', 'success')
        
    except json.JSONDecodeError:
        flash('Arquivo JSON inválido', 'danger')
    except Exception as e:
        logger.error(f"Error importing emails: {e}")
        flash(f'Erro ao importar emails: {str(e)}', 'danger')
    finally:
        if conn:
            conn.close()
    
    return redirect(url_for('admin.manage_emails'))

@admin_bp.route('/prepare-deploy')
@login_required
def prepare_deploy():
    """Export current emails to initial_emails.json for deployment"""
    if not is_admin(current_user):
        return redirect(url_for('home'))
    
    from app.admin.initializer import export_current_emails_to_file
    
    if export_current_emails_to_file():
        flash('✅ Emails exportados para data/initial_emails.json com sucesso! Pronto para deploy.', 'success')
        
        # Log the action
        log_admin_action(
            current_user.email,
            'PREPARE_DEPLOY',
            details="Exported emails to initial_emails.json for deployment"
        )
    else:
        flash('❌ Erro ao exportar emails para deploy', 'danger')
    
    return redirect(url_for('admin.manage_emails'))