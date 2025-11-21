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
from sqlalchemy.ext.automap import automap_base
from .aggregate import build_overview
from app.api_dashboard import build_dashboard_payload, build_user_month_dashboard_payload
from app.services import user_main_dashboard_service
from app.services.upload_service import UploadService
from app.services.job_service import JobService
from app.services.result_storage import ResultStorageService
from app.services.user_main_dashboard_service import get_user_main_month_weights
from app.services.user_months_service import LATEST_UPLOAD_KEY, UserMonthsService
from app.services.storage import get_storage
from app.stats.hand_collector import HandCollector
from app.services.db_pool import DatabasePool

bp_dashboard = Blueprint("bp_dashboard", __name__, url_prefix="/api/dashboard")
bp_dashboard_debug = Blueprint(
    "bp_dashboard_debug", __name__ + "_debug", url_prefix="/api/debug/dashboard"
)
bp_dashboard_internal = Blueprint(
    "bp_dashboard_internal",
    __name__ + "_internal",
    url_prefix="/api/internal",
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


@lru_cache()
def _get_orm_base():
    engine = _get_engine()
    Base = automap_base()
    Base.prepare(autoload_with=engine)
    return Base


def _get_orm_models():
    Base = _get_orm_base()
    upload_model = getattr(Base.classes, "uploads", None)
    job_model = getattr(Base.classes, "jobs", None)
    return upload_model, job_model


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


def _extract_global_counts(result: Dict[str, Any]) -> Dict[str, int]:
    classification = result.get("classification") if isinstance(result, dict) else {}
    discards = classification.get("discarded_hands") if isinstance(classification, dict) else {}

    if not isinstance(discards, dict):
        discards = result.get("aggregated_discards") if isinstance(result, dict) else {}
    if not isinstance(discards, dict):
        discards = result.get("discarded_hands") if isinstance(result, dict) else {}

    total = classification.get("total_hands") if isinstance(classification, dict) else result.get("total_hands", 0)
    valid = classification.get("valid_hands") if isinstance(classification, dict) else result.get("valid_hands", 0)

    try:
        total_val = int(total or 0)
    except Exception:
        total_val = 0

    try:
        valid_val = int(valid or 0)
    except Exception:
        valid_val = 0

    if isinstance(discards, dict) and not valid_val:
        valid_val = total_val - int(discards.get("total", 0))

    return {
        "total": total_val,
        "valid": valid_val,
        "mystery": int((discards or {}).get("mystery", 0)),
        "lt4": int((discards or {}).get("less_than_4_players", 0)),
    }


def _load_global_dashboard_payload(token: str) -> Dict[str, Any]:
    """Load the classic global dashboard payload or raise FileNotFoundError."""

    payload = build_dashboard_payload(token, month=None, include_months=False)
    if not payload or not isinstance(payload, dict):
        raise FileNotFoundError(f"Dashboard payload not found for {token}")

    groups = payload.get("groups")
    if not isinstance(groups, dict) or not groups:
        raise FileNotFoundError(f"Dashboard payload missing groups for {token}")

    return payload

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


@bp_dashboard.get("/<token>/global-counts")
def api_global_counts(token: str):
    """Lightweight helper to inspect global counters for a token."""

    storage = ResultStorageService()
    try:
        result = storage.get_pipeline_result(token)
    except FileNotFoundError:
        return jsonify({"ok": False, "error": "missing_artifact"}), 404
    except Exception as exc:  # noqa: BLE001 - keep response resilient
        logger.exception("Failed to load pipeline_result for %s: %s", token, exc)
        return jsonify({"ok": False, "error": "unexpected"}), 500

    if not result:
        return jsonify({"ok": False, "error": "missing_artifact"}), 404

    counts = _extract_global_counts(result)
    return jsonify({"ok": True, "data": counts})

@bp_dashboard.get("/<token>/global")
def api_dashboard_global(token: str):
    """Return only the global dashboard payload for a token."""

    try:
        payload = _load_global_dashboard_payload(token)
        return jsonify({"ok": True, "data": payload})
    except FileNotFoundError as exc:
        return (
            jsonify({"ok": False, "error": "not_found", "detail": str(exc)}),
            404,
        )
    except Exception as exc:  # noqa: BLE001 - keep response resilient
        logger.exception("Failed to build global dashboard payload for %s: %s", token, exc)
        return jsonify({"ok": False, "error": "unexpected"}), 500


@bp_dashboard.get("/<token>")
def api_dashboard_with_token(token):
    """Get dashboard data for a specific token (global view or filtered by month)."""

    month = (request.args.get("month") or "").strip()
    month = month if month else None
    """Get dashboard data for a specific token (global or monthly view)."""

    month = request.args.get("month")
    if month:
        month = month.strip()

    if month and not re.fullmatch(r"\d{4}-\d{2}", month):
        return jsonify({"ok": False, "error": "invalid_month"}), 400

    try:
        data = build_dashboard_payload(token, month=month, include_months=True)
        if month and not data:
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({"ok": True, "data": data})
        if month:
            payload = build_dashboard_payload(token, month=month, include_months=True)
            if not payload or payload.get("month_not_found"):
                return jsonify({"ok": False, "error": "not_found"}), 404
        else:
            payload = _load_global_dashboard_payload(token)

        return jsonify({"ok": True, "data": payload})
    except FileNotFoundError as exc:
        return (
            jsonify({"ok": False, "error": "not_found", "detail": str(exc)}),
            404,
        )
    except Exception as exc:
        logger.exception("Unexpected error building dashboard for %s: %s", token, exc)
        return jsonify({"ok": False, "error": "unexpected", "detail": str(exc)}), 500


@bp_dashboard.get("/user-month")
@login_required
def api_user_month_dashboard():
    """Return the monthly dashboard payload using the authenticated user's history."""

    month = (request.args.get("month") or "").strip()
    if not month or (month != LATEST_UPLOAD_KEY and not re.match(r"^\d{4}-\d{2}$", month)):
        return jsonify({"success": False, "error": "invalid_month"}), 400

    user_identifier = str(getattr(current_user, "id", "") or "")
    if not user_identifier:
        return jsonify({"success": False, "error": "missing_user"}), 401

    result_storage = ResultStorageService()
    months_service = UserMonthsService()
    uploads_repo = UploadService()

    has_data = user_main_dashboard_service.user_has_data(
        user_id=user_identifier,
        result_storage=result_storage,
        user_months_service=months_service,
        uploads_repo=uploads_repo,
    )

    if not has_data:
        return jsonify(
            {
                "success": False,
                "error": "no_data_for_user",
                "help": "Please upload a file first",
            }
        )

    try:
        payload = build_user_month_dashboard_payload(
            user_identifier, month, use_cache=False, result_storage=result_storage
        )
    except Exception:  # noqa: BLE001 - keep response stable for frontend
        logger.exception(
            "Erro em /api/dashboard/user-month para user %s mes %s", user_identifier, month
        )
        return jsonify({"success": False, "error": "internal_error"}), 500

    month_not_found = not payload or payload.get("month_not_found")
    if month_not_found:
        logger.exception(
            "[USER_MONTH] No dashboard payload for user %s month %s despite detected data",
            user_identifier,
            month,
        )
        return jsonify(
            {
                "success": False,
                "error": "no_data_for_month",
                "help": f"No monthly dashboard could be built for {month} even though the user has pipeline results.",
            }
        )

    return jsonify({"success": True, "data": payload, "has_data": True})

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
    """Devolve uploads recentes para o utilizador autenticado."""

    conn = None
    try:
        user_identifier = str(getattr(current_user, "id", ""))
        if not user_identifier:
            return jsonify({"success": True, "data": {"has_data": False}})

        conn = DatabasePool.get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.id, u.token, u.filename, u.status,
                       u.uploaded_at, u.processed_at, u.error_message
                FROM uploads AS u
                INNER JOIN jobs AS j ON j.upload_id = u.id
                WHERE j.user_id = %s
                ORDER BY u.uploaded_at DESC
                LIMIT 5
                """,
                (user_identifier,),
            )

            rows = cur.fetchall() or []
            columns = [desc[0] for desc in cur.description]
            uploads = [dict(zip(columns, row)) for row in rows]

        def _to_iso(value):
            if not value:
                return None
            iso_value = value.isoformat()
            return iso_value[:-6] + "Z" if iso_value.endswith("+00:00") else iso_value

        recent_uploads = [
            {
                "id": str(entry.get("id")),
                "token": entry.get("token"),
                "filename": entry.get("filename"),
                "status": entry.get("status"),
                "uploaded_at": _to_iso(entry.get("uploaded_at")),
                "processed_at": _to_iso(entry.get("processed_at")),
                "error_message": entry.get("error_message"),
            }
            for entry in uploads
        ]

        has_data = len(recent_uploads) > 0

        payload = {
            "has_data": has_data,
            "recent_uploads": recent_uploads,
            "latest_token": recent_uploads[0].get("token") if recent_uploads else None,
        }

        return jsonify({"success": True, "data": payload})
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
        return jsonify(
            {
                "success": True,
                "data": {"has_data": False, "message": "Main page em manutenção. Usa a dashboard global após upload."},
                "mode": "maintenance",
            }
        )

        upload = UploadService.get_master_or_latest_upload_for_user(str(current_user.id))
        if not upload:
            return jsonify({"success": True, "data": None, "message": "no_upload_for_user"})

        token = upload.get("token") or upload.get("job_id")
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
                "upload_token": upload.get("token"),
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


@bp_dashboard_internal.get("/debug-dashboard-state")
@login_required
def api_internal_debug_dashboard_state():
    """
    Endpoint interno de debug.
    NÃO tenta carregar dashboards, NÃO usa tokens,
    NÃO mexe em ficheiros, NÃO chama build_dashboard_payload.

    Só devolve:
      - uploads do utilizador
      - jobs associados a cada upload
    """

    try:
        user_identifier = getattr(current_user, "id", None)

        if user_identifier is None:
            return jsonify({"success": True, "data": []})

        upload_model, job_model = _get_orm_models()
        if not upload_model or not job_model:
            return jsonify(
                {
                    "success": False,
                    "error": "missing_models",
                    "debug_exception": "ORM models for uploads/jobs not found",
                }
            )

        engine = _get_engine()

        result: list[dict[str, Any]] = []
        with Session(engine) as db:
            uploads_query = db.query(upload_model)
            if user_identifier is not None:
                uploads_query = uploads_query.filter(upload_model.user_id == user_identifier)
            if hasattr(upload_model, "created_at"):
                uploads_query = uploads_query.order_by(upload_model.created_at.desc())

            uploads = uploads_query.all()

            for u in uploads:
                jobs_query = db.query(job_model).filter(job_model.upload_id == getattr(u, "id", None))
                if hasattr(job_model, "created_at"):
                    jobs_query = jobs_query.order_by(job_model.created_at.desc())

                jobs = jobs_query.all()
                job_list = []
                for j in jobs:
                    job_list.append(
                        {
                            "id": getattr(j, "id", None),
                            "job_id": getattr(j, "job_id", None),
                            "status": getattr(j, "status", None),
                            "created_at": str(getattr(j, "created_at", None)),
                            "upload_id": getattr(j, "upload_id", None),
                        }
                    )

                result.append(
                    {
                        "upload_id": getattr(u, "id", None),
                        "user_id": getattr(u, "user_id", None),
                        "filename": getattr(u, "filename", None)
                        or getattr(u, "file_name", None),
                        "is_master": getattr(u, "is_master", None),
                        "created_at": str(getattr(u, "created_at", None)),
                        "jobs": job_list,
                    }
                )

        return jsonify({"success": True, "data": result})

    except Exception as e:  # noqa: BLE001 - endpoint de debug deve devolver rastreio
        logger.exception("Erro em /api/internal/debug-dashboard-state")
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
        token = upload.get("token") or upload.get("job_id")
        print("TOKEN:", token)
        payload = build_dashboard_payload(token, month=None)
        print("HAS_PAYLOAD:", bool(payload))
