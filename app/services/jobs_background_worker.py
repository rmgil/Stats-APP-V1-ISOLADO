"""Background worker that processes pending jobs with concurrency control."""

import gc
import logging
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict

from app.services.job_service import JobService
from app.services.storage import get_storage
from app.utils.supabase_retry import with_supabase_retry


logger = logging.getLogger(__name__)


class JobsBackgroundWorker:
    def __init__(self, max_concurrent: int = 2, poll_interval: float = 2.0):
        self.max_concurrent = max_concurrent
        self.poll_interval = poll_interval
        self.running = False
        self.thread: threading.Thread | None = None
        self.active: Dict[str, threading.Thread] = {}
        self.lock = threading.Lock()
        self.job_service = JobService()

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        logger.info("Jobs background worker started (max_concurrent=%s)", self.max_concurrent)

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)

    def _loop(self):
        while self.running:
            try:
                with self.lock:
                    active = len(self.active)
                capacity = self.max_concurrent - active
                if capacity > 0:
                    jobs = self.job_service.claim_pending_jobs(capacity)
                    for job in jobs:
                        worker = threading.Thread(
                            target=self._process_job,
                            args=(job,),
                            daemon=True,
                        )
                        with self.lock:
                            self.active[job["id"]] = worker
                        worker.start()
                time.sleep(self.poll_interval)
            except Exception as exc:  # noqa: BLE001
                logger.error("Worker loop error: %s", exc, exc_info=True)
                time.sleep(self.poll_interval)

    def _cleanup(self, token: str):
        for path in [Path(f"/tmp/processing_{token}"), Path(f"/tmp/{token}"), Path("work") / token]:
            try:
                if path.exists():
                    shutil.rmtree(path)
            except Exception:
                logger.warning("Failed to cleanup %s", path)

    def _process_job(self, job: Dict):
        job_id = job["id"]
        try:
            logger.info("Processing job %s", job_id)
            storage = get_storage()

            # Download input to temporary location
            raw_data = storage.download_file(job["input_path"])
            if not raw_data:
                raise RuntimeError(f"Input file missing at {job['input_path']}")

            temp_dir = Path(tempfile.mkdtemp(prefix=f"job_{job_id}_"))
            archive_path = temp_dir / "input.zip"
            archive_path.write_bytes(raw_data)

            work_root = Path(f"/tmp/processing_{job_id}")
            work_root.mkdir(parents=True, exist_ok=True)

            def progress_cb(percent: int, message: str):
                self.job_service.update_progress(job_id, percent)

            from app.pipeline.multi_site_runner import run_multi_site_pipeline

            success, message, pipeline_result = run_multi_site_pipeline(
                archive_path=str(archive_path),
                work_root=str(work_root),
                token=job_id,
                progress_callback=progress_cb,
                user_id=str(job.get("user_id")),
            )

            if not success:
                raise RuntimeError(message or "Pipeline returned failure")

            pipeline_output = work_root / job_id
            if not pipeline_output.exists():
                raise RuntimeError(f"Pipeline output not found at {pipeline_output}")

            dashboard_path = pipeline_output / "dashboard.json"
            dashboard_path.write_text(
                (pipeline_output / "pipeline_result.json").read_text(),
                encoding="utf-8",
            )

            storage_prefix = f"results/{job_id}"
            result_path = f"{storage_prefix}/dashboard.json"

            def _upload():
                for file_path in pipeline_output.rglob("*.json"):
                    relative = file_path.relative_to(pipeline_output)
                    dest = f"{storage_prefix}/{relative}".replace("\\", "/")
                    with open(file_path, "rb") as handle:
                        storage.upload_fileobj(handle, dest)

            if storage.use_cloud:
                with_supabase_retry(_upload)
            else:
                _upload()

            self.job_service.mark_done(job_id, result_path=result_path)
            logger.info("Job %s finished", job_id)

        except Exception as exc:  # noqa: BLE001
            logger.error("Job %s failed: %s", job_id, exc, exc_info=True)
            self.job_service.mark_error(job_id, error_message=str(exc))
        finally:
            with self.lock:
                self.active.pop(job_id, None)
            try:
                self._cleanup(job_id)
            except Exception:
                pass
            gc.collect()


_worker_instance: JobsBackgroundWorker | None = None


def ensure_jobs_worker(max_concurrent: int = 2) -> JobsBackgroundWorker:
    global _worker_instance
    if _worker_instance is None:
        _worker_instance = JobsBackgroundWorker(max_concurrent=max_concurrent)
        _worker_instance.start()
    return _worker_instance
