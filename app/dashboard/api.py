# app/dashboard/api.py
from __future__ import annotations
from flask import Blueprint, Response, jsonify, request
from flask_login import current_user, login_required
import os, re, tempfile, json
import logging
import traceback
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
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
bp_dashboard_debug = Blueprint(
    "bp_dashboard_debug", __name__ + "_debug", url_prefix="/api/debug/dashboard"
)

logger = logging.getLogger(__name__)

SAFE_RE = re.compile(r"^[a-zA-Z0-9_\-]{8,64}$")


@lru_cache()
def _get_engine() -> Engine:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable not set")

    return create_engine(database_url)


@lru_cache()
def _get_tables() -> tuple[Engine, Table, Table]:
    engine = _get_engine()
    metadata = MetaData()
    uploads_table = Table("uploads", metadata, autoload_with=engine)
    jobs_table = Table("jobs", metadata, autoload_with=engine)
    return engine, uploads_table, jobs_table


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
        
        # DEBUG NOTE:
        # O token da dashboard é o próprio jobs.id (gerado em JobService.create_job
        # com secrets.token_hex(6)). /api/dashboard/<token> passa esse valor direto
        # para build_dashboard_payload(token, ...), que usa ResultStorageService
        # para ler /results/<token>/pipeline_result_global.json e as variantes
        # mensais (pipeline_result_<YYYY-MM>.json ou months/<YYYY-MM>/pipeline_result.json).
        # Assim, o diretório de storage base é sempre /results/<jobs.id>/.

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
        if not user_identifier:
            return jsonify({"success": True, "data": {"has_data": False}})

        conn = DatabasePool.get_connection()
        with conn.cursor() as cur:
            token: Optional[str] = None

            # Prefer a finished job associated with a master upload when the column exists
            try:
                cur.execute(
                    """
                    SELECT j.id
                    FROM uploads u
                    JOIN jobs j ON j.upload_id = u.id
                    WHERE u.user_id = %s
                      AND u.is_master = true
                      AND j.status = 'done'
                    ORDER BY j.created_at DESC
                    LIMIT 1
                    """,
                    (user_identifier,),
                )
                row = cur.fetchone()
                if row:
                    token = row[0]
            except Exception:
                logger.debug(
                    "[DASHBOARD CURRENT] Master upload lookup failed; falling back to latest job",
                    exc_info=True,
                )

            if token is None:
                cur.execute(
                    """
                    SELECT j.id
                    FROM uploads u
                    JOIN jobs j ON j.upload_id = u.id
                    WHERE u.user_id = %s
                      AND j.status = 'done'
                    ORDER BY j.created_at DESC
                    LIMIT 1
                    """,
                    (user_identifier,),
                )
                row = cur.fetchone()
                if row:
                    token = row[0]

            if not token:
                return jsonify({"success": True, "data": {"has_data": False}})

        return jsonify({"success": True, "data": {"has_data": True, "token": str(token)}})
    except Exception:
        logger.exception(
            "Erro em /api/dashboard/current para user %s",
            getattr(current_user, "id", None),
        )
        return jsonify({"success": False, "error": "internal_error"})
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


@bp_dashboard_debug.get("/state")
@login_required
def api_debug_dashboard_state():
    """Mapeia uploads → jobs → tokens de forma resiliente usando ORM/reflexão."""

    try:
        user_identifier = str(getattr(current_user, "id", ""))
        if not user_identifier:
            return jsonify({"success": False, "error": "missing_user_id"}), 400

        engine, uploads_table, jobs_table = _get_tables()

        with Session(engine) as session:
            uploads_query = (
                select(uploads_table)
                .where(uploads_table.c.user_id == user_identifier)
            )
            if "created_at" in uploads_table.c:
                uploads_query = uploads_query.order_by(uploads_table.c.created_at.desc())

            upload_rows = session.execute(uploads_query).mappings().all()
            upload_ids = [row.get("id") for row in upload_rows if row.get("id") is not None]

            jobs_rows: list[dict] = []
            if upload_ids:
                jobs_query = select(jobs_table).where(jobs_table.c.upload_id.in_(upload_ids))
                if "created_at" in jobs_table.c:
                    jobs_query = jobs_query.order_by(jobs_table.c.created_at.desc())

                jobs_rows = session.execute(jobs_query).mappings().all()

        jobs_by_upload: dict[Any, list[dict[str, Any]]] = {}
        debug_jobs: list[dict[str, Any]] = []

        for job in jobs_rows:
            token = job.get("id")
            has_payload = False
            error = None

            if token:
                try:
                    payload = build_dashboard_payload(str(token), month=None)
                    has_payload = bool(payload)
                except Exception as exc:  # noqa: BLE001 - debug endpoint deve expor erros
                    error = str(exc)

            job_data = {
                "id": job.get("id"),
                "upload_id": job.get("upload_id"),
                "status": job.get("status"),
                "created_at": job.get("created_at"),
                "job_id": job.get("job_id"),
                "result_path": job.get("result_path"),
                "token": str(token) if token is not None else None,
                "has_payload": has_payload,
                "error": error,
            }

            debug_jobs.append(job_data)
            jobs_by_upload.setdefault(job.get("upload_id"), []).append(job_data)

        def _serialize_upload(upload: dict[str, Any]) -> dict[str, Any]:
            return {
                "id": upload.get("id"),
                "user_id": upload.get("user_id"),
                "filename": upload.get("filename") or upload.get("file_name"),
                "is_master": upload.get("is_master"),
                "created_at": upload.get("created_at"),
                "upload_token": upload.get("token") or upload.get("client_upload_token"),
                "storage_path": upload.get("storage_path"),
                "jobs": jobs_by_upload.get(upload.get("id"), []),
            }

        return jsonify(
            {
                "success": True,
                "data": {
                    "user_identifier_used_in_uploads": user_identifier,
                    "uploads": [_serialize_upload(u) for u in upload_rows],
                    "jobs": debug_jobs,
                },
            }
        )
    except Exception as e:  # noqa: BLE001 - endpoint de debug deve devolver rastreio
        logger.exception("Erro em /api/debug/dashboard/state")
        import traceback

        return jsonify(
            {
                "success": False,
                "error": "internal_error",
                "debug_exception": str(e),
                "debug_traceback": traceback.format_exc(),
            }
        )


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
