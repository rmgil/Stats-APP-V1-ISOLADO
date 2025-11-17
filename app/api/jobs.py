"""Job-centric upload and status endpoints."""

import os
import secrets
import tempfile
from pathlib import Path

from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user

from app.services.job_service import JobService
from app.services.upload_service import UploadService
from app.services.storage import get_storage
from app.services.jobs_background_worker import ensure_jobs_worker
from app.services.file_hash import FileHashService
from app.utils.supabase_retry import with_supabase_retry


bp_jobs = Blueprint("bp_jobs", __name__, url_prefix="/api/jobs")


@bp_jobs.post("/upload")
@login_required
def upload_job():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "missing_file"}), 400

    file = request.files["file"]
    filename = Path(file.filename or "").name
    if not filename:
        return jsonify({"ok": False, "error": "empty_filename"}), 400

    suffix = Path(filename).suffix
    if suffix.lower() not in {".zip", ".rar"}:
        return jsonify({"ok": False, "error": "invalid_extension"}), 400

    storage = get_storage()
    job_service = JobService()
    upload_service = UploadService()

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        file.save(tmp.name)
        temp_path = Path(tmp.name)

    try:
        file_hash = FileHashService.calculate_hash(temp_path)
        user_id = str(current_user.id)
        client_token = secrets.token_hex(6)

        upload_id = upload_service.create_upload(
            user_id=user_id,
            client_upload_token=client_token,
            file_name=filename,
            file_hash=file_hash,
            is_master=False,
        )

        if not upload_id:
            return jsonify({"ok": False, "error": "upload_not_created"}), 500

        storage_path = f"uploads/{user_id}/{upload_id}/input{suffix}"

        def _upload():
            with open(temp_path, "rb") as handle:
                storage.upload_fileobj(handle, storage_path)

        if storage.use_cloud:
            with_supabase_retry(_upload)
        else:
            _upload()

        job_id = job_service.create_job(
            user_id=user_id,
            upload_id=upload_id,
            input_path=storage_path,
        )

        if not job_id:
            return jsonify({"ok": False, "error": "job_not_created"}), 500

        ensure_jobs_worker()

        return jsonify({"ok": True, "job_id": job_id, "upload_id": upload_id})
    finally:
        try:
            os.unlink(temp_path)
        except Exception:
            pass


@bp_jobs.get("/<job_id>")
@login_required
def job_status(job_id: str):
    job_service = JobService()
    job = job_service.get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "not_found"}), 404

    position = None
    if job.get("status") == "pending":
        position = job_service.get_queue_position(job_id)

    response = {
        "ok": True,
        "job": {
            "status": job.get("status"),
            "progress": job.get("progress"),
            "position_in_queue": position,
            "result_path": job.get("result_path"),
            "error_message": job.get("error_message"),
        }
    }
    return jsonify(response)
