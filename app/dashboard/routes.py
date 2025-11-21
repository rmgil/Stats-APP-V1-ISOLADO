"""Dashboard routes for displaying analysis results"""

from flask import Blueprint, render_template, abort, request
from flask_login import login_required
import json
from app.api_dashboard import build_dashboard_payload

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/dashboard/<token>')
@login_required
def show_dashboard(token):
    """Display the dashboard with analysis results, supporting month filters."""

    try:
        selected_month = request.args.get('month')

        # Load the analysis data from Supabase Storage (global + months metadata)
        data = build_dashboard_payload(token, month=selected_month, include_months=True)

        # Check if data was successfully loaded
        if not data or not data.get('groups'):
            abort(404, description=f"Não foi encontrado nenhum resultado para o token '{token}'.")

        months_manifest = (data.get('months_manifest') or {}).get('months') or []
        available_months = data.get('months') or []

        # Render the dashboard template with the data
        return render_template(
            'dashboard_tabs.html',
            token=token,
            month=selected_month,
            data=json.dumps(data),
            dashboard_api_mode='token',
            available_months=available_months,
            available_months_info=months_manifest,
        )

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
