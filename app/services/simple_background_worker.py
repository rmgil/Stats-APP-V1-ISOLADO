"""Simplified background worker using JobQueueService"""
import threading
import time
import os
import shutil
import logging
import gc
import json
from pathlib import Path

from app.services.job_queue_service import JobQueueService
from app.services.storage import get_storage
from app.services.metrics import ResourceMetrics

logger = logging.getLogger(__name__)

class SimpleBackgroundWorker:
    """Simplified background worker that processes upload jobs from SQLite queue"""
    
    def __init__(self):
        self.worker_id = f"worker-{os.getpid()}"
        self.running = False
        self.thread = None
        self.job_queue = JobQueueService()
        logger.info(f"Initialized SimpleBackgroundWorker: {self.worker_id}")
    
    def start(self):
        """Start the background worker thread"""
        if self.running:
            logger.warning("Worker already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
        logger.info(f"SimpleBackgroundWorker started: {self.worker_id}")
    
    def stop(self):
        """Stop the background worker"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info(f"SimpleBackgroundWorker stopped: {self.worker_id}")
    
    def _worker_loop(self):
        """Main worker loop - process jobs FIFO"""
        logger.info(f"Worker loop started: {self.worker_id}")
        
        while self.running:
            try:
                job = self.job_queue.claim_next_job()
                
                if not job:
                    time.sleep(2)
                    continue
                
                self._process_job(job)
                
            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
                time.sleep(5)
        
        logger.info(f"Worker loop ended: {self.worker_id}")
    
    def _cleanup_job_artifacts(self, token):
        """Clean up temporary artifacts after job completion"""
        cleanup_count = 0
        freed_mb = 0
        
        try:
            upload_dir = Path(f"/tmp/{token}")
            if upload_dir.exists():
                dir_size_mb = sum(f.stat().st_size for f in upload_dir.rglob('*') if f.is_file()) / (1024 * 1024)
                shutil.rmtree(upload_dir)
                cleanup_count += 1
                freed_mb += dir_size_mb
                logger.info(f"✓ Cleaned /tmp/{token} ({dir_size_mb:.1f}MB)")
            
            temp_dir = Path(f"/tmp/processing_{token}")
            if temp_dir.exists():
                dir_size_mb = sum(f.stat().st_size for f in temp_dir.rglob('*') if f.is_file()) / (1024 * 1024)
                shutil.rmtree(temp_dir)
                cleanup_count += 1
                freed_mb += dir_size_mb
                logger.info(f"✓ Cleaned /tmp/processing_{token} ({dir_size_mb:.1f}MB)")
            
            work_dir = Path(f"work/{token}")
            if work_dir.exists():
                dir_size_mb = sum(f.stat().st_size for f in work_dir.rglob('*') if f.is_file()) / (1024 * 1024)
                shutil.rmtree(work_dir)
                cleanup_count += 1
                freed_mb += dir_size_mb
                logger.info(f"✓ Cleaned work/{token} ({dir_size_mb:.1f}MB)")
            
            if cleanup_count > 0:
                logger.info(f"🧹 Cleanup complete for {token}: removed {cleanup_count} directories, freed {freed_mb:.1f}MB")
                
        except Exception as e:
            logger.warning(f"Cleanup failed for {token}: {e}")
    
    def _upload_directory_to_storage(self, storage, directory_path, storage_prefix):
        """Recursively upload a directory to Object Storage"""
        uploaded_files = []
        directory_path = Path(directory_path)
        
        for file_path in directory_path.rglob('*'):
            if not file_path.is_file():
                continue
            
            relative_path = file_path.relative_to(directory_path)
            storage_key = f"{storage_prefix}/{relative_path}".replace('\\', '/')
            
            logger.debug(f"Uploading {file_path} to {storage_key}")
            
            with open(file_path, 'rb') as f:
                storage.upload_fileobj(f, storage_key)
            
            uploaded_files.append(storage_key)
        
        logger.info(f"✅ Uploaded {len(uploaded_files)} files to {storage_prefix}/")
        return uploaded_files
    
    def _process_job(self, job):
        """Process a single job"""
        token = job['token']
        start_time = time.time()
        start_mb = ResourceMetrics.get_process_memory_mb()
        
        logger.info(f"🔵 Processing job: {token} (file: {job['filename']})")
        
        try:
            self.job_queue.update_progress(token, 10, 'Inicializando processamento...')
            
            file_path = Path(job['payload_path'])
            
            if not file_path.exists():
                raise Exception(f"Uploaded file not found: {file_path}")
            
            self.job_queue.update_progress(token, 30, 'Extraindo arquivos...')
            
            work_base = Path(f"/tmp/processing_{token}")
            work_base.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Processing file with multi-site pipeline: {file_path}")
            
            from app.pipeline.multi_site_runner import run_multi_site_pipeline
            
            def progress_update(percent, message):
                """Callback for pipeline progress updates"""
                self.job_queue.update_progress(token, percent, message)
                logger.info(f"Progress {percent}%: {message}")
            
            success, message, pipeline_result = run_multi_site_pipeline(
                archive_path=str(file_path),
                work_root=str(work_base),
                token=token,
                progress_callback=progress_update,
                user_id=job.get('user_email'),
            )
            
            if not success:
                raise Exception(f'Pipeline failed: {message}')
            
            logger.info(f"Pipeline completed successfully: {message}")
            
            pipeline_output_dir = work_base / token
            
            if not pipeline_output_dir.exists():
                raise Exception(f'Pipeline output directory not found: {pipeline_output_dir}')
            
            self.job_queue.update_progress(token, 85, 'Enviando resultados para armazenamento...')
            
            storage = get_storage()
            use_cloud = storage.use_cloud
            
            if use_cloud:
                logger.info(f"☁️ Uploading results to Object Storage for {token}")
                
                storage_prefix = f"results/{token}"
                self._upload_directory_to_storage(storage, pipeline_output_dir, storage_prefix)
                
                logger.info(f"✅ Results uploaded to Object Storage: {storage_prefix}")
            else:
                logger.info(f"💾 Copying results to local work/ directory for {token}")
                
                work_output_dir = Path('work') / token
                work_output_dir.parent.mkdir(parents=True, exist_ok=True)
                
                if work_output_dir.exists():
                    shutil.rmtree(work_output_dir)
                
                shutil.copytree(pipeline_output_dir, work_output_dir)
                logger.info(f"✅ Results copied to work/{token}")
            
            self.job_queue.update_progress(token, 95, 'Finalizando...')
            
            result_data = {
                'success': True,
                'message': message,
                'dashboard_token': token,
                'download_url': f'/api/download/result/{token}'
            }
            
            self.job_queue.mark_completed(token, json.dumps(result_data))
            
            logger.info(f"✅ Job completed successfully: {token}")
            
            self._cleanup_job_artifacts(token)
            gc.collect()
            
            end_mb = ResourceMetrics.get_process_memory_mb()
            duration = time.time() - start_time
            ResourceMetrics.log_job_summary(token, start_mb, end_mb, duration)
            
        except Exception as e:
            import traceback
            full_traceback = traceback.format_exc()
            
            logger.error(f"❌ Error processing job {token}: {e}")
            logger.error(f"Full traceback:\n{full_traceback}")
            
            error_message = f"{str(e)}\n\nTraceback:\n{full_traceback}"
            self.job_queue.mark_failed(token, error_message)
            
            self._cleanup_job_artifacts(token)
            gc.collect()


_worker_instance = None

def start_simple_background_worker():
    """Start the simple background worker (singleton)"""
    global _worker_instance
    
    if _worker_instance is None:
        _worker_instance = SimpleBackgroundWorker()
        _worker_instance.start()
        logger.info("Simple background worker started")
    else:
        logger.warning("Simple background worker already running")
    
    return _worker_instance


def get_simple_worker_instance():
    """Get the simple worker instance (for status checks)"""
    return _worker_instance
