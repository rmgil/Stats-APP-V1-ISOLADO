"""
API endpoints for querying processing history from Supabase
"""
import logging
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from app.services.supabase_history import SupabaseHistoryService

logger = logging.getLogger(__name__)

history_bp = Blueprint('history', __name__)

@history_bp.route('/api/history/my', methods=['GET'])
@login_required
def get_my_history():
    """
    Get processing history for current user
    
    Query parameters:
        - limit: Maximum number of records (default: 50)
        - offset: Number of records to skip (default: 0)
    """
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
        # Validate parameters
        limit = min(max(1, limit), 100)
        offset = max(0, offset)
        
        history_service = SupabaseHistoryService()
        
        if not history_service.enabled:
            return jsonify({
                'error': 'Histórico não disponível',
                'message': 'Serviço de histórico não está configurado'
            }), 503
        
        user_id = current_user.email if hasattr(current_user, 'email') else str(current_user.id)
        
        history = history_service.get_user_history(
            user_id=user_id,
            limit=limit,
            offset=offset
        )
        
        return jsonify({
            'success': True,
            'history': history,
            'count': len(history),
            'limit': limit,
            'offset': offset
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching user history: {e}", exc_info=True)
        return jsonify({'error': f'Erro ao buscar histórico: {str(e)}'}), 500

@history_bp.route('/api/history/details/<token>', methods=['GET'])
@login_required
def get_processing_details(token):
    """
    Get full details of a specific processing including stats
    
    Path parameters:
        - token: Processing token
    """
    try:
        history_service = SupabaseHistoryService()
        
        if not history_service.enabled:
            return jsonify({
                'error': 'Histórico não disponível',
                'message': 'Serviço de histórico não está configurado'
            }), 503
        
        details = history_service.get_processing_details(token)
        
        if not details:
            return jsonify({'error': 'Processamento não encontrado'}), 404
        
        # Check if user has access to this processing
        user_id = current_user.email if hasattr(current_user, 'email') else str(current_user.id)
        
        if details.get('user_id') != user_id:
            return jsonify({'error': 'Acesso negado'}), 403
        
        return jsonify({
            'success': True,
            'details': details
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching processing details: {e}", exc_info=True)
        return jsonify({'error': f'Erro ao buscar detalhes: {str(e)}'}), 500

@history_bp.route('/api/history/stats', methods=['GET'])
@login_required
def get_user_stats():
    """
    Get aggregated statistics for current user
    
    Returns summary statistics like total uploads, total hands processed, etc.
    """
    try:
        history_service = SupabaseHistoryService()
        
        if not history_service.enabled:
            return jsonify({
                'error': 'Histórico não disponível',
                'message': 'Serviço de histórico não está configurado'
            }), 503
        
        user_id = current_user.email if hasattr(current_user, 'email') else str(current_user.id)
        
        # Get all user history to calculate stats
        all_history = history_service.get_user_history(
            user_id=user_id,
            limit=1000,
            offset=0
        )
        
        # Calculate aggregated stats
        total_uploads = len(all_history)
        total_hands = sum(h.get('total_hands', 0) for h in all_history)
        total_sites = sum(h.get('total_sites', 0) for h in all_history)
        
        completed_count = sum(1 for h in all_history if h.get('status') == 'completed')
        failed_count = sum(1 for h in all_history if h.get('status') == 'failed')
        
        # Get most recent upload
        most_recent = all_history[0] if all_history else None
        
        return jsonify({
            'success': True,
            'stats': {
                'total_uploads': total_uploads,
                'total_hands': total_hands,
                'total_sites': total_sites,
                'completed_count': completed_count,
                'failed_count': failed_count,
                'most_recent': most_recent
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error calculating user stats: {e}", exc_info=True)
        return jsonify({'error': f'Erro ao calcular estatísticas: {str(e)}'}), 500
