"""Dashboard routes for displaying analysis results"""

from flask import Blueprint, render_template, abort, jsonify
from flask_login import login_required
import os
import json
from pathlib import Path
from app.api_dashboard import build_dashboard_payload

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/dashboard/<token>')
@login_required
def show_dashboard(token):
    """Display the dashboard with analysis results"""
    try:
        # Check if results exist locally
        work_dir = Path('work') / token
        if not work_dir.exists():
            abort(404, description=f"Não foi encontrado nenhum resultado para o token '{token}'.")
        
        # Load the analysis data
        data = build_dashboard_payload(token)
        
        # Render the dashboard template with the data
        return render_template('dashboard_tabs.html',
                             token=token,
                             data=json.dumps(data))
    
    except FileNotFoundError:
        abort(404, description=f"Não foi encontrado nenhum resultado para o token '{token}'.")
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