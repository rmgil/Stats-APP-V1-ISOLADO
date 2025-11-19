"""FastAPI router responsible for the simple upload workflow."""

from __future__ import annotations

import logging
import re
import secrets
import shutil
from pathlib import Path
from typing import Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile
from werkzeug.utils import secure_filename

from app.pipeline.multi_site_runner import run_multi_site_pipeline
from app.services.file_hash import FileHashService
from app.services.storage import get_storage
from app.services.supabase_history import SupabaseHistoryService
from app.services.supabase_storage import SupabaseStorageService
from app.services.upload_service import UploadService
from app.services.master_result_builder import rebuild_user_master_results
from app.api.auth_dependencies import get_current_user
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/upload", tags=["upload"])

MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB
ALLOWED_EXTENSIONS = {".zip", ".rar"}

# Track processing progress during the background job
PROCESSING_STATUS: Dict[str, Dict[str, object]] = {}

# Temporary storage for uploads that still need to be processed in the background
PENDING_UPLOADS: Dict[str, Dict[str, object]] = {}


def allowed_file(filename: str) -> bool:
    """Validate archive extensions."""

    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


def cleanup_temp_files(token: str) -> None:
    """Remove temporary working directories associated with a token."""

    try:
        storage = get_storage()

        tmp_paths = [Path(f"/tmp/{token}"), Path(f"/tmp/processing_{token}")]
        for path in tmp_paths:
            if path.exists():
                shutil.rmtree(path)
                logger.info("Cleaned up: %s", path)

        if storage.use_cloud:
            work_path = Path(f"work/{token}")
            if work_path.exists():
                shutil.rmtree(work_path)
                logger.info("Cleaned up: %s (cloud storage active)", work_path)
        else:
            logger.info("Keeping work/%s - contains results (local storage mode)", token)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Cleanup error for %s: %s", token, exc)


@router.post("/simple")
async def upload_file(
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    file: UploadFile = File(...),
):
    """Handle archive reception, registration and pipeline scheduling."""

    if not file.filename:
        raise HTTPException(status_code=400, detail="empty_filename")

    filename = secure_filename(file.filename)
    if not allowed_file(filename):
        raise HTTPException(status_code=400, detail="invalid_extension")

    logger.info(f"FASTAPI AUTH USER = {current_user.id}")
    user_id = current_user.get_id() or current_user.id
    if not user_id:
        raise HTTPException(status_code=401, detail="user_not_authenticated")
    user_id = str(user_id)
    token = secrets.token_hex(6)
    upload_dir = Path("/tmp") / token
    upload_dir.mkdir(parents=True, exist_ok=True)
    file_path = upload_dir / filename

    logger.info("Processing upload: filename=%s user_id=%s token=%s", filename, user_id, token)

    bytes_written = 0
    chunk_size = 1024 * 1024
    with open(file_path, "wb") as handle:
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            handle.write(chunk)
            bytes_written += len(chunk)
            if bytes_written > MAX_FILE_SIZE:
                cleanup_temp_files(token)
                raise HTTPException(status_code=413, detail="file_too_large")

    await file.close()

    if bytes_written == 0:
        cleanup_temp_files(token)
        raise HTTPException(status_code=400, detail="empty_file")

    file_hash = FileHashService.calculate_hash(file_path)
    logger.info("File hash calculated: %s...", file_hash[:16])

    history_service = SupabaseHistoryService()
    if history_service.enabled:
        existing = history_service.find_by_file_hash(file_hash, user_id=user_id)
        if existing:
            cleanup_temp_files(token)
            logger.info("Duplicate detected. Reusing token %s", existing.get("token"))
            return {
                "success": True,
                "message": "Ficheiro já processado anteriormente! A reutilizar resultados.",
                "token": existing.get("token"),
                "download_url": f"/api/download/result/{existing.get('token')}",
                "dashboard_url": f"/dashboard/{existing.get('token')}",
                "duplicate": True,
                "original_date": existing.get("created_at"),
                "total_hands": existing.get("total_hands", 0),
            }

    upload_service = UploadService()
    try:
        upload_id = upload_service.create_upload(
            user_id=user_id,
            token=token,
            filename=filename,
            archive_sha256=file_hash,
            status="uploaded",
            hand_count=0,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("ERROR REGISTERING UPLOAD IN DB")
        cleanup_temp_files(token)
        raise HTTPException(status_code=500, detail="failed_to_register_upload") from exc

    if not upload_id:
        cleanup_temp_files(token)
        raise HTTPException(status_code=500, detail="failed_to_register_upload")

    storage_path: Optional[str] = None
    try:
        supabase_storage = SupabaseStorageService()
        if supabase_storage.enabled:
            storage_path = f"uploads/{user_id}/{token}/{filename}"
            with open(file_path, "rb") as src:
                upload_success = supabase_storage.upload_file_from_stream(src, storage_path)
            if not upload_success:
                storage_path = None
                logger.warning("Supabase storage upload skipped for %s", token)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Storage upload failed (non-critical): %s", exc)
        storage_path = None

    PROCESSING_STATUS[token] = {
        "status": "queued",
        "progress": 0,
        "message": "Na fila de processamento",
        "total_hands": 0,
        "processed_hands": 0,
        "user_id": user_id,
    }

    PENDING_UPLOADS[token] = {
        "file_path": str(file_path),
        "filename": filename,
        "file_hash": file_hash,
        "bytes_written": bytes_written,
        "storage_path": storage_path,
    }

    background_tasks.add_task(run_pipeline_background, token, upload_id, user_id)

    return {"success": True, "token": token}


def run_pipeline_background(token: str, upload_id: str, user_id: str) -> None:
    """Execute the heavy pipeline outside of the HTTP request lifecycle."""

    context = PENDING_UPLOADS.pop(token, None)
    if not context:
        logger.error("No pending context found for token %s", token)
        cleanup_temp_files(token)
        return

    upload_service = UploadService()
    history_service = SupabaseHistoryService()

    file_path = Path(context["file_path"])
    filename = context["filename"]
    file_hash = context["file_hash"]
    bytes_written = context["bytes_written"]
    storage_path = context.get("storage_path")

    work_base = Path(f"/tmp/processing_{token}")
    work_base.mkdir(parents=True, exist_ok=True)

    PROCESSING_STATUS[token] = {
        "status": "processing",
        "progress": 0,
        "message": "A inicializar processamento...",
        "total_hands": 0,
        "processed_hands": 0,
        "user_id": user_id,
    }

    def progress_callback(percent: int, message: str) -> None:
        logger.info("[%s] Progress %s%%: %s", token, percent, message)
        status = PROCESSING_STATUS.get(token)
        if status is None:
            return
        status["progress"] = percent
        status["message"] = message

        if "mãos" in message.lower():
            numbers = re.findall(r"\d+", message)
            if len(numbers) >= 2:
                status["processed_hands"] = int(numbers[0])
                status["total_hands"] = int(numbers[-1])

    try:
        upload_service.update_upload_status(upload_id, status="processing")

        success, message, pipeline_result = run_multi_site_pipeline(
            archive_path=str(file_path),
            work_root=str(work_base),
            token=token,
            progress_callback=progress_callback,
            user_id=user_id,
        )

        if not success:
            raise RuntimeError(f"Pipeline failed: {message}")

        total_hands = None
        if isinstance(pipeline_result, dict):
            total_hands = pipeline_result.get("total_hands") or pipeline_result.get("valid_hands")

        upload_service.update_upload_status(
            upload_id,
            status="processed",
            processed=True,
            hand_count=total_hands,
            error_message=None,
        )

        pipeline_output_dir = work_base / token
        if not pipeline_output_dir.exists():
            raise RuntimeError("Pipeline output directory not found")

        storage = get_storage()
        if storage.use_cloud:
            storage_prefix = f"results/{token}"
            uploaded_files = 0
            for result_file in pipeline_output_dir.rglob("*.json"):
                if result_file.is_file():
                    relative_path = result_file.relative_to(pipeline_output_dir)
                    storage_key = f"{storage_prefix}/{relative_path}".replace("\\", "/")
                    with open(result_file, "rb") as src:
                        storage.upload_fileobj(src, storage_key)
                    uploaded_files += 1
            logger.info("Uploaded %s JSON result files to Object Storage", uploaded_files)
        else:
            work_output_dir = Path("work") / token
            work_output_dir.parent.mkdir(parents=True, exist_ok=True)
            if work_output_dir.exists():
                shutil.rmtree(work_output_dir)
            shutil.copytree(pipeline_output_dir, work_output_dir)
            logger.info("Results copied to local storage")

        if history_service.enabled:
            try:
                history_service.save_processing(
                    token=token,
                    filename=filename,
                    pipeline_result=pipeline_result,
                    user_id=user_id,
                    file_size_bytes=bytes_written,
                    file_hash=file_hash,
                    storage_path=storage_path,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to save to Supabase history: %s", exc)

        try:
            logger.info("Rebuilding consolidated results for user %s", user_id)
            rebuild_user_master_results(user_id)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to rebuild master results for user %s: %s", user_id, exc)

        logger.info("Pipeline completed for %s", token)
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Processing error for %s: %s", token, exc, exc_info=True)
        upload_service.update_upload_status(
            upload_id,
            status="error",
            processed=False,
            error_message=str(exc),
        )
        PROCESSING_STATUS[token] = {
            "status": "error",
            "progress": PROCESSING_STATUS.get(token, {}).get("progress", 0),
            "message": str(exc),
            "total_hands": 0,
            "processed_hands": 0,
            "user_id": user_id,
        }
    finally:
        status = PROCESSING_STATUS.get(token)
        if not status or status.get("status") != "error":
            PROCESSING_STATUS.pop(token, None)
        cleanup_temp_files(token)


@router.get("/status/{token}")
async def get_status(token: str, current_user: User = Depends(get_current_user)):
    """Return the current status for a token, checking storage on completion."""

    user_id = current_user.get_id() or current_user.id
    if not user_id:
        raise HTTPException(status_code=401, detail="user_not_authenticated")
    user_id = str(user_id)

    status_info = PROCESSING_STATUS.get(token)
    if status_info:
        status_owner = status_info.get("user_id")
        if status_owner and status_owner != user_id:
            raise HTTPException(status_code=404, detail="upload_nao_encontrado")

        payload = {
            "success": status_info.get("status") != "error",
            "token": token,
            "status": status_info.get("status", "processing"),
            "progress": status_info.get("progress", 0),
            "message": status_info.get("message", "A processar..."),
            "total_hands": status_info.get("total_hands", 0),
            "processed_hands": status_info.get("processed_hands", 0),
        }
        if not payload["success"]:
            payload["error_message"] = status_info.get("message") or "Erro no processamento"
        return payload

    upload_service = UploadService()
    upload = upload_service.get_upload_by_token(user_id=user_id, token=token)
    if not upload:
        raise HTTPException(status_code=404, detail="upload_nao_encontrado")

    upload_status = (upload.get("status") or "uploaded").lower()
    if upload_status == "error":
        error_message = upload.get("error_message") or "Erro no processamento"
        return {
            "success": False,
            "token": token,
            "status": "error",
            "progress": 0,
            "message": error_message,
            "error_message": error_message,
        }

    if upload_status in {"processed", "done"}:
        storage = get_storage()
        if storage.use_cloud:
            test_file = f"results/{token}/pipeline_result.json"
            exists = storage.download_file(test_file) is not None
        else:
            exists = (Path("work") / token).exists()

        if exists:
            return {
                "success": True,
                "token": token,
                "status": "completed",
                "progress": 100,
                "message": "Processamento concluído",
                "redirect_url": "/main",
            }

    if upload_status == "processing":
        return {
            "success": True,
            "token": token,
            "status": "processing",
            "progress": 50,
            "message": "A processar...",
        }

    return {
        "success": True,
        "token": token,
        "status": "queued",
        "progress": 0,
        "message": "Na fila de processamento",
    }
