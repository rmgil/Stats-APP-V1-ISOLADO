# app/dashboard/api.py
from __future__ import annotations
from flask import Blueprint, request, jsonify
import os, re, tempfile
from .aggregate import build_overview
from app.api_dashboard import build_dashboard_payload
from app.services.result_storage import ResultStorageService

bp_dashboard = Blueprint("bp_dashboard", __name__, url_prefix="/api/dashboard")

SAFE_RE = re.compile(r"^[a-zA-Z0-9_\-]{8,64}$")

def _job_dir(job: str) -> str:
    # todos os jobs vão para /tmp/mtt_jobs/<job>
    base = os.environ.get("MTT_JOBS_DIR", os.path.join(tempfile.gettempdir(), "mtt_jobs"))
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, job)

@bp_dashboard.get("/overview")
def api_overview():
    job = request.args.get("job", "").strip()
    if not SAFE_RE.match(job):
        return jsonify({"error":"invalid job id"}), 400
    try:
        data = build_overview(_job_dir(job))
        return jsonify({"ok": True, "data": data})
    except FileNotFoundError as e:
        return jsonify({"ok": False, "error": "missing_artifact", "detail": str(e)}), 404
    except Exception as e:
        return jsonify({"ok": False, "error": "unexpected", "detail": str(e)}), 500

@bp_dashboard.get("/<token>")
def api_dashboard_with_token(token):
    """Get dashboard data for a specific token
    
    Query parameters:
        month: Optional YYYY-MM format to load month-specific data.
               If not provided, loads aggregate data.
    """
    try:
        # Get optional month parameter from query string
        month = request.args.get('month')
        
        # Validate month format if provided
        if month:
            import re
            if not re.match(r'^\d{4}-\d{2}$', month):
                return jsonify({
                    "ok": False, 
                    "error": "invalid_month", 
                    "detail": "Month must be in YYYY-MM format"
                }), 400
        
        # Use the build_dashboard_payload function from api_dashboard
        data = build_dashboard_payload(token, month=month)
        return jsonify({"ok": True, "data": data})
    except FileNotFoundError as e:
        return jsonify({"ok": False, "error": "missing_artifact", "detail": str(e)}), 404
    except Exception as e:
        return jsonify({"ok": False, "error": "unexpected", "detail": str(e)}), 500

@bp_dashboard.get("/<token>/months")
def api_months_manifest(token):
    """Get months manifest for a multi-month upload"""
    try:
        result_service = ResultStorageService()
        manifest = result_service.get_months_manifest(token)
        
        if manifest is None:
            return jsonify({
                "ok": False, 
                "error": "not_found", 
                "detail": "No months manifest found for this upload. This may be a single-month upload."
            }), 404
        
        return jsonify({"ok": True, "data": manifest})
    except Exception as e:
        return jsonify({"ok": False, "error": "unexpected", "detail": str(e)}), 500