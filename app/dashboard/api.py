# app/dashboard/api.py
from __future__ import annotations
from flask import Blueprint, request, jsonify
import os, re, tempfile
from .aggregate import build_overview
from app.api_dashboard import build_dashboard_payload

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
    """Get dashboard data for a specific token"""
    try:
        # Use the build_dashboard_payload function from api_dashboard
        data = build_dashboard_payload(token)
        return jsonify({"ok": True, "data": data})
    except FileNotFoundError as e:
        return jsonify({"ok": False, "error": "missing_artifact", "detail": str(e)}), 404
    except Exception as e:
        return jsonify({"ok": False, "error": "unexpected", "detail": str(e)}), 500