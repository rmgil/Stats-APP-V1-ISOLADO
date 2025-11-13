"""
Admin API endpoints for cleanup operations
"""
import logging
from flask import Blueprint, jsonify
from flask_login import login_required, current_user
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

cleanup_admin_bp = Blueprint('cleanup_admin', __name__)

def is_admin(user):
    """Check if user is admin (basic check - enhance as needed)"""
    if hasattr(user, 'is_admin'):
        return user.is_admin
    if hasattr(user, 'email'):
        return user.email in ['admin@example.com']
    return False

@cleanup_admin_bp.route('/api/admin/cleanup/work-stats', methods=['GET'])
@login_required
def get_work_stats():
    """
    Get statistics about work directory
    """
    try:
        work_dir = Path('work')
        
        if not work_dir.exists():
            return jsonify({
                'success': True,
                'stats': {
                    'total_directories': 0,
                    'total_size_bytes': 0,
                    'total_size_gb': 0,
                    'message': 'Diretório work/ não existe'
                }
            }), 200
        
        total_dirs = 0
        total_size = 0
        
        for token_dir in work_dir.iterdir():
            if token_dir.is_dir():
                total_dirs += 1
                
                # Calculate directory size
                for item in token_dir.rglob('*'):
                    if item.is_file():
                        try:
                            total_size += item.stat().st_size
                        except Exception:
                            pass
        
        return jsonify({
            'success': True,
            'stats': {
                'total_directories': total_dirs,
                'total_size_bytes': total_size,
                'total_size_gb': round(total_size / (1024**3), 2),
                'total_size_mb': round(total_size / (1024**2), 2)
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting work stats: {e}", exc_info=True)
        return jsonify({'error': f'Erro ao obter estatísticas: {str(e)}'}), 500

@cleanup_admin_bp.route('/api/admin/cleanup/work-all', methods=['POST'])
@login_required
def cleanup_all_work():
    """
    Delete all files in work directory (DANGEROUS!)
    Requires admin permission
    """
    try:
        # Admin check
        if not is_admin(current_user):
            return jsonify({'error': 'Permissão negada - apenas administradores'}), 403
        
        work_dir = Path('work')
        
        if not work_dir.exists():
            return jsonify({
                'success': True,
                'message': 'Diretório work/ não existe',
                'deleted_count': 0,
                'freed_bytes': 0
            }), 200
        
        deleted_count = 0
        freed_bytes = 0
        
        # Delete all subdirectories
        for token_dir in list(work_dir.iterdir()):
            if token_dir.is_dir():
                # Calculate size before deleting
                dir_size = 0
                for item in token_dir.rglob('*'):
                    if item.is_file():
                        try:
                            dir_size += item.stat().st_size
                        except Exception:
                            pass
                
                # Delete directory
                shutil.rmtree(token_dir)
                deleted_count += 1
                freed_bytes += dir_size
                
                logger.info(f"Deleted: {token_dir.name} ({dir_size / (1024**2):.2f} MB)")
        
        message = f'✅ Limpeza completa: {deleted_count} diretórios apagados, {freed_bytes / (1024**3):.2f} GB libertados'
        logger.info(message)
        
        return jsonify({
            'success': True,
            'message': message,
            'deleted_count': deleted_count,
            'freed_bytes': freed_bytes,
            'freed_gb': round(freed_bytes / (1024**3), 2)
        }), 200
        
    except Exception as e:
        logger.error(f"Error cleaning up work directory: {e}", exc_info=True)
        return jsonify({'error': f'Erro na limpeza: {str(e)}'}), 500

@cleanup_admin_bp.route('/api/admin/cleanup/work-old', methods=['POST'])
@login_required
def cleanup_old_work():
    """
    Delete work files older than 7 days
    Requires admin permission
    """
    try:
        # Admin check
        if not is_admin(current_user):
            return jsonify({'error': 'Permissão negada - apenas administradores'}), 403
        
        from app.services.cleanup_service import CleanupService
        
        # Use existing cleanup service
        deleted_count = CleanupService.cleanup_local_work_directory(days=7)
        
        message = f'✅ Ficheiros antigos apagados: {deleted_count} diretórios (>7 dias)'
        logger.info(message)
        
        return jsonify({
            'success': True,
            'message': message,
            'deleted_count': deleted_count
        }), 200
        
    except Exception as e:
        logger.error(f"Error cleaning up old work files: {e}", exc_info=True)
        return jsonify({'error': f'Erro na limpeza: {str(e)}'}), 500
