"""Dashboard routes for displaying analysis results"""

from flask import Blueprint, render_template, abort, jsonify, request
from flask_login import login_required
import os
import json
import re
from pathlib import Path
from app.api_dashboard import build_dashboard_payload

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/dashboard/<token>')
@login_required
def show_dashboard(token):
    """Display the dashboard with analysis results
    
    Query parameters:
        month: Optional YYYY-MM format to view month-specific data.
               If not provided, shows aggregate data.
    """
    # Get optional month parameter from query string (outside try to avoid LSP warning)
    month = request.args.get('month')
    
    # Validate month format if provided
    if month and not re.match(r'^\d{4}-\d{2}$', month):
        abort(400, description="Mês deve estar no formato YYYY-MM")
    
    try:
        
        # Load the analysis data from Supabase Storage
        data = build_dashboard_payload(token, month=month)
        
        # Check if data was successfully loaded
        if not data or not data.get('groups'):
            abort(404, description=f"Não foi encontrado nenhum resultado para o token '{token}'.")
        
        # Render the dashboard template with the data
        return render_template(
            'dashboard_tabs.html',
            token=token,
            month=month,
            data=json.dumps(data),
            dashboard_api_mode='token',
            available_months=[],
        )
    
    except FileNotFoundError:
        month_msg = f" para o mês {month}" if month else ""
        abort(404, description=f"Não foi encontrado nenhum resultado{month_msg} para o token '{token}'.")
    except Exception as e:
        abort(500, description=f"Erro ao carregar dashboard: {str(e)}")

@dashboard_bp.route('/dashboard/<token>/download')
@login_required
def download_results(token):
    """Download the results as a zip file"""
    try:
        from app.api.download import download_result
        return download_result(token)
    except Exception as e:
        abort(500, description=f"Erro ao fazer download: {str(e)}")