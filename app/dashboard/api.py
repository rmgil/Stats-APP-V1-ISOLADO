# app/dashboard/api.py
from __future__ import annotations
from flask import Blueprint, Response, jsonify, request
from flask_login import current_user, login_required
import os, re, tempfile, json
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from .aggregate import build_overview
from app.api_dashboard import build_dashboard_payload
from app.services.upload_service import UploadService
from app.services.job_service import JobService
from app.services.result_storage import ResultStorageService
from app.services.user_main_dashboard_service import get_user_main_month_weights
from app.services.user_months_service import UserMonthsService
from app.services.storage import get_storage
from app.stats.hand_collector import HandCollector
from app.services.db_pool import DatabasePool

bp_dashboard = Blueprint("bp_dashboard", __name__, url_prefix="/api/dashboard")

logger = logging.getLogger(__name__)

SAFE_RE = re.compile(r"^[a-zA-Z0-9_\-]{8,64}$")


def _read_storage_or_local(storage, storage_path: str, local_path: Path) -> Optional[bytes]:
    try:
        data = storage.download_file(storage_path)
        if data:
            return data
    except Exception as exc:  # noqa: BLE001 - best-effort fallback to local
        logger.debug("[MAIN_SAMPLE] Storage read failed for %s: %s", storage_path, exc)

    try:
        if local_path.exists():
            return local_path.read_bytes()
    except Exception as exc:  # noqa: BLE001 - optional fallback
        logger.debug("[MAIN_SAMPLE] Local read failed for %s: %s", local_path, exc)

    return None


def _load_metadata(storage, base_storage_prefix: str, base_local_dir: Path) -> Dict[str, Any]:
    metadata_bytes = _read_storage_or_local(
        storage,
        f"{base_storage_prefix}/metadata.json",
        base_local_dir / "metadata.json",
    )

    if not metadata_bytes:
        return {}

    try:
        return json.loads(metadata_bytes.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - guard against malformed metadata
        logger.warning("[MAIN_SAMPLE] Failed to parse metadata at %s: %s", base_storage_prefix, exc)
        return {}


def _load_stat_hands(
    storage,
    token: str,
    month: str,
    group: str,
    stat_key: str,
    stat_filename: str,
) -> List[Tuple[str, str]]:
    base_storage_prefix = f"/results/{token}/months/{month}/hands_by_stat/{group}"
    base_local_dir = Path("work") / token / "months" / month / "hands_by_stat" / group

    metadata = _load_metadata(storage, base_storage_prefix, base_local_dir)
    hand_ids = (metadata.get("hand_ids") or {}).get(stat_key)
    if not hand_ids:
        return []

    data_bytes = _read_storage_or_local(
        storage,
        f"{base_storage_prefix}/{stat_filename}",
        base_local_dir / stat_filename,
    )

    if not data_bytes:
        return []

    try:
        content = data_bytes.decode("utf-8")
    except UnicodeDecodeError:
        content = data_bytes.decode("latin-1", errors="ignore")

    hands = [segment.strip() for segment in re.split(r"\n{2,}", content) if segment.strip()]

    if len(hand_ids) != len(hands):
        logger.warning(
            "[MAIN_SAMPLE] Mismatch between hand_ids (%s) and hands (%s) for %s/%s/%s",
            len(hand_ids),
            len(hands),
            token,
            month,
            stat_filename,
        )
        pair_count = min(len(hand_ids), len(hands))
        hand_ids = hand_ids[:pair_count]
        hands = hands[:pair_count]

    return list(zip(hand_ids, hands))

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
    logger = logging.getLogger(__name__)
    try:
        result_service = ResultStorageService()

        months_list = []
        if hasattr(result_service, "list_available_months"):
            try:
                months_list = result_service.list_available_months(token)
            except Exception as exc:
                logger.warning(
                    "[DASHBOARD] Failed to list available months for %s: %s",
                    token,
                    exc,
                )

        manifest: Optional[Dict[str, Any]] = None
        if not months_list:
            manifest = result_service.get_months_manifest(token)
            if manifest and isinstance(manifest, dict):
                months_list = manifest.get("months", [])

        if not months_list:
            return jsonify({
                "ok": False,
                "error": "not_found",
                "detail": "No month data found for this upload.",
            }), 404

        return jsonify({"ok": True, "data": {"months": months_list}})
    except Exception as e:
        return jsonify({"ok": False, "error": "unexpected", "detail": str(e)}), 500


@bp_dashboard.get("/current")
@login_required
def api_current_dashboard():
    """Devolve o token de dashboard mais recente (ou master) para o utilizador autenticado."""

    conn = None
    try:
        user_identifier = str(getattr(current_user, "id", ""))
        user_debug = {
            "id": getattr(current_user, "id", None),
            "email": getattr(current_user, "email", None),
        }

        uploads_debug: List[Dict[str, Any]] = []
        jobs_debug: List[Dict[str, Any]] = []

        conn = DatabasePool.get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, user_id, filename, is_master, created_at
                FROM uploads
                WHERE user_id = %s
                ORDER BY created_at DESC
                """,
                (user_identifier,),
            )
            upload_columns = [desc[0] for desc in cur.description]
            uploads: List[Dict[str, Any]] = []
            for row in cur.fetchall() or []:
                upload_entry = dict(zip(upload_columns, row))
                if isinstance(upload_entry.get("created_at"), datetime):
                    upload_entry["created_at"] = upload_entry["created_at"].isoformat()
                uploads_debug.append(upload_entry.copy())
                uploads.append(upload_entry)

            cur.execute(
                """
                SELECT id, user_id, upload_id, status, created_at
                FROM jobs
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 20
                """,
                (user_identifier,),
            )
            job_columns = [desc[0] for desc in cur.description]
            for row in cur.fetchall() or []:
                job_entry = dict(zip(job_columns, row))
                if isinstance(job_entry.get("created_at"), datetime):
                    job_entry["created_at"] = job_entry["created_at"].isoformat()
                jobs_debug.append(job_entry)

            if not uploads:
                data = {"has_data": False}
            else:
                master_upload = next((u for u in uploads if u.get("is_master")), None)
                candidate_uploads = []
                if master_upload:
                    candidate_uploads.append(master_upload)
                candidate_uploads.extend(
                    [u for u in uploads if not master_upload or u.get("id") != master_upload.get("id")]
                )

                token = None
                for upload in candidate_uploads:
                    cur.execute(
                        """
                        SELECT id
                        FROM jobs
                        WHERE upload_id = %s AND status = 'done'
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (upload.get("id"),),
                    )
                    row = cur.fetchone()
                    if row:
                        token = row[0]
                        break

                data: Dict[str, Any] = {"has_data": bool(token)}
                if token:
                    data["token"] = token

        return jsonify(
            {
                "success": True,
                "data": data,
                "debug": {"user": user_debug, "uploads": uploads_debug, "jobs": jobs_debug},
            }
        )
    except Exception as e:
        logger.exception(
            "Erro em /api/dashboard/current (debug) para user %s",
            getattr(current_user, "id", None),
        )

        tb = traceback.format_exc()

        return jsonify(
            {
                "success": False,
                "error": "internal_error",
                "debug_exception": str(e),
                "debug_traceback": tb,
            }
        )
    finally:
        if conn:
            DatabasePool.return_connection(conn)


@bp_dashboard.get("/main")
@login_required
def api_user_main_dashboard():
    """Return the main dashboard payload for the authenticated user using a robust fallback."""

    try:
        upload = UploadService.get_master_or_latest_upload_for_user(str(current_user.id))
        if not upload:
            return jsonify({"success": True, "data": None, "message": "no_upload_for_user"})

        token = upload.get("client_upload_token") or upload.get("job_id") or upload.get("token")
        if not token:
            logger.error(
                "MAIN DASHBOARD: upload sem token para user_id=%s upload=%s",
                current_user.id,
                upload.get("id"),
            )
            return jsonify({"success": False, "error": "upload_without_token"})

        payload = build_dashboard_payload(token, month=None)
        if not payload:
            logger.warning(
                "MAIN DASHBOARD: payload vazio para user_id=%s token=%s",
                current_user.id,
                token,
            )
            return jsonify({"success": True, "data": None, "message": "no_payload_for_token"})

        logger.debug(
            "MAIN DASHBOARD OK: user_id=%s token=%s keys=%s",
            current_user.id,
            token,
            list(payload.keys()),
        )

        return jsonify(
            {"success": True, "data": payload, "mode": "single_token_fallback", "token": token}
        )

    except Exception:
        logger.exception("MAIN DASHBOARD: erro inesperado para user_id=%s", current_user.id)
        return jsonify({"success": False, "error": "internal_error_main_dashboard"})


@bp_dashboard.get("/main/sample/<group>/<stat_key>")
@login_required
def api_user_main_sample(group: str, stat_key: str):
    """Return a merged TXT with unique hands for a stat across main page months."""

    allowed_groups = {"nonko_9max", "nonko_6max", "pko", "postflop_all"}
    if group not in allowed_groups:
        return jsonify({"success": False, "error": "invalid_group"}), 400

    stat_filename = HandCollector.stat_filenames.get(stat_key)
    if not stat_filename:
        return jsonify({"success": False, "error": "invalid_stat"}), 404

    storage = get_storage()
    months_service = UserMonthsService()
    months_map = months_service.get_user_months_map(str(current_user.id))

    month_weights = get_user_main_month_weights(str(current_user.id))
    if not month_weights:
        return jsonify({"success": False, "error": "no_months"}), 404

    seen_ids = set()
    collected_hands: List[str] = []

    for entry in month_weights:
        month = entry.get("month")
        if not month:
            continue

        for token in months_map.get(month, []):
            for hand_id, hand_text in _load_stat_hands(storage, token, month, group, stat_key, stat_filename):
                if hand_id in seen_ids:
                    continue
                seen_ids.add(hand_id)
                collected_hands.append(hand_text.strip())

    if not collected_hands:
        return jsonify({"success": False, "error": "no_hands"}), 404

    payload = "\n\n\n".join(collected_hands)
    if payload and not payload.endswith("\n"):
        payload += "\n"

    return Response(payload, mimetype="text/plain")


if __name__ == "__main__":
    # Substitui por um user_id real com uploads para testar rapidamente o fallback.
    test_user_id = "<USER_ID_DE_TESTE>"
    upload = UploadService.get_master_or_latest_upload_for_user(test_user_id)
    print("UPLOAD:", upload.get("id") if upload else None)
    if upload:
        token = upload.get("client_upload_token") or upload.get("job_id") or upload.get("token")
        print("TOKEN:", token)
        payload = build_dashboard_payload(token, month=None)
        print("HAS_PAYLOAD:", bool(payload))
