"""
Simple upload API - synchronous version
Processes files immediately without job queue
"""
import os
import logging
import shutil
import json
import secrets
import traceback
from pathlib import Path
from typing import Optional
from flask import Blueprint, request, jsonify, send_file
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
import io
from app.services.upload_service import UploadService

logger = logging.getLogger(__name__)

simple_upload_bp = Blueprint('simple_upload', __name__)

MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB
ALLOWED_EXTENSIONS = {'.zip', '.rar'}

# Global dictionary to track processing progress
# Format: {token: {"status": str, "progress": int, "message": str, "total_hands": int, "processed_hands": int}}
PROCESSING_STATUS = {}

def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed"""
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS

def cleanup_temp_files(token: str):
    """Clean up all temporary files for a given token"""
    try:
        from app.services.storage import get_storage
        storage = get_storage()
        
        # Clean up /tmp directories always
        tmp_paths = [
            Path(f'/tmp/{token}'),
            Path(f'/tmp/processing_{token}')
        ]
        
        for path in tmp_paths:
            if path.exists():
                shutil.rmtree(path)
                logger.info(f"Cleaned up: {path}")
        
        # IMPORTANT: Only clean work/{token} if using cloud storage
        # When using local storage, work/{token} contains the actual results!
        if storage.use_cloud:
            work_path = Path(f'work/{token}')
            if work_path.exists():
                shutil.rmtree(work_path)
                logger.info(f"Cleaned up: {work_path} (cloud storage active)")
        else:
            logger.info(f"Keeping work/{token} - contains results (local storage mode)")
                
    except Exception as e:
        logger.warning(f"Cleanup error for {token}: {e}")

@simple_upload_bp.route('/api/upload/simple', methods=['POST'])
@login_required
def upload_file():
    """
    Synchronous upload and processing endpoint
    Processes file immediately and returns results
    """
    token = None
    upload_record_id: Optional[str] = None
    upload_service = UploadService()
    
    try:
        # Validate file
        if 'file' not in request.files:
            return jsonify({'error': 'Nenhum arquivo enviado'}), 400
        
        file = request.files['file']
        filename = file.filename
        
        if not filename or filename == '':
            return jsonify({'error': 'Nome de arquivo vazio'}), 400
        
        if not allowed_file(filename):
            return jsonify({'error': 'Tipo de arquivo não suportado. Use ZIP ou RAR.'}), 400
        
        # Generate token
        token = secrets.token_hex(6)
        filename = secure_filename(filename)
        
        # Create upload directory
        upload_dir = Path('/tmp') / token
        upload_dir.mkdir(parents=True, exist_ok=True)
        file_path = upload_dir / filename
        
        logger.info(f"Processing upload: {filename} (user: {current_user.email}, token: {token})")
        
        # Save file with size validation
        bytes_written = 0
        chunk_size = 8192
        
        try:
            with open(file_path, 'wb') as f:
                while True:
                    chunk = file.stream.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_written += len(chunk)
                    
                    if bytes_written > MAX_FILE_SIZE:
                        raise ValueError(f"Arquivo muito grande (máximo {MAX_FILE_SIZE // (1024*1024)}MB)")
                        
        except Exception as e:
            cleanup_temp_files(token)
            raise
        
        if bytes_written == 0:
            cleanup_temp_files(token)
            return jsonify({'error': 'Arquivo vazio recebido'}), 400
        
        logger.info(f"File saved: {filename} ({bytes_written} bytes)")
        
        # Calculate file hash for deduplication
        from app.services.file_hash import FileHashService
        from app.services.supabase_history import SupabaseHistoryService
        from app.services.supabase_storage import SupabaseStorageService
        
        file_hash = FileHashService.calculate_hash(file_path)
        logger.info(f"File hash calculated: {file_hash[:16]}...")
        
        # Check if file has been processed before (deduplication)
        history_service = SupabaseHistoryService()
        user_id = current_user.email if hasattr(current_user, 'email') else str(current_user.id)
        
        if history_service.enabled:
            existing = history_service.find_by_file_hash(file_hash, user_id=user_id)

            if existing:
                old_token = existing.get('token')
                logger.info(f"🔄 Duplicate file detected! Returning results from {old_token}")
                
                cleanup_temp_files(token)
                
                return jsonify({
                    'success': True,
                    'message': f'Ficheiro já processado anteriormente! A reutilizar resultados.',
                    'token': old_token,
                    'download_url': f'/api/download/result/{old_token}',
                    'dashboard_url': f'/dashboard/{old_token}',
                    'duplicate': True,
                    'original_date': existing.get('created_at'),
                    'total_hands': existing.get('total_hands', 0)
                }), 200

        try:
            logger.info(
                "REGISTERING UPLOAD: user_id=%s, filename=%s, token=%s",
                user_id,
                filename,
                token,
            )
            upload_record_id = upload_service.create_upload(
                user_id=user_id,
                token=token,
                filename=filename,
                archive_sha256=file_hash,
                status='uploaded',
                hand_count=0,
            )
        except Exception:
            logger.exception("ERROR REGISTERING UPLOAD IN DB")
            cleanup_temp_files(token)
            return jsonify({'success': False, 'error': 'failed_to_register_upload'}), 500

        if not upload_record_id:
            logger.error(
                "Failed to register upload metadata for user_id=%s filename=%s token=%s",
                user_id,
                filename,
                token,
            )
            cleanup_temp_files(token)
            return jsonify({'success': False, 'error': 'failed_to_register_upload'}), 500

        logger.info(
            "UPLOAD REGISTERED IN DB: upload_id=%s, user_id=%s, token=%s",
            upload_record_id,
            user_id,
            token,
        )

        # Process file immediately (synchronously)
        try:
            from app.pipeline.multi_site_runner import run_multi_site_pipeline
            from app.services.storage import get_storage
            
            # Initialize processing status
            PROCESSING_STATUS[token] = {
                'status': 'processing',
                'progress': 0,
                'message': 'A inicializar processamento...',
                'total_hands': 0,
                'processed_hands': 0
            }
            
            # Create work directory
            work_base = Path(f"/tmp/processing_{token}")
            work_base.mkdir(parents=True, exist_ok=True)
            
            # Progress callback - updates global status
            def progress_callback(percent, message):
                logger.info(f"[{token}] Progress {percent}%: {message}")
                
                # Update processing status for real-time feedback
                if token in PROCESSING_STATUS:
                    PROCESSING_STATUS[token]['progress'] = percent
                    PROCESSING_STATUS[token]['message'] = message
                    
                    # Parse message for hand counts if available
                    if 'mãos' in message.lower():
                        import re
                        numbers = re.findall(r'\d+', message)
                        if len(numbers) >= 2:
                            PROCESSING_STATUS[token]['processed_hands'] = int(numbers[0])
                            PROCESSING_STATUS[token]['total_hands'] = int(numbers[-1])
            
            # Run pipeline
            logger.info(f"Starting pipeline for {token}")
            if upload_record_id:
                upload_service.update_upload_status(upload_record_id, status='processing')
            success, message, pipeline_result = run_multi_site_pipeline(
                archive_path=str(file_path),
                work_root=str(work_base),
                token=token,
                progress_callback=progress_callback,
                user_id=user_id,
            )

            if not success:
                raise Exception(f'Pipeline failed: {message}')

            if upload_record_id:
                total_hands = None
                if isinstance(pipeline_result, dict):
                    total_hands = pipeline_result.get('total_hands') or pipeline_result.get('valid_hands')
                upload_service.update_upload_status(
                    upload_record_id,
                    status='processed',
                    processed=True,
                    hand_count=total_hands,
                    error_message=None,
                )
            
            logger.info(f"Pipeline completed: {message}")
            
            # Check for results
            pipeline_output_dir = work_base / token
            if not pipeline_output_dir.exists():
                raise Exception(f'Pipeline output directory not found')
            
            # Upload to storage (Object Storage or local)
            storage = get_storage()
            
            if storage.use_cloud:
                logger.info(f"Uploading to Object Storage: {token}")
                storage_prefix = f"results/{token}"
                
                # Upload only JSON result files to cloud (not extracted TXT files)
                json_files_uploaded = 0
                for result_file in pipeline_output_dir.rglob('*.json'):
                    if result_file.is_file():
                        relative_path = result_file.relative_to(pipeline_output_dir)
                        storage_key = f"{storage_prefix}/{relative_path}".replace('\\', '/')
                        
                        with open(result_file, 'rb') as f:
                            storage.upload_fileobj(f, storage_key)
                        json_files_uploaded += 1
                            
                logger.info(f"✅ Uploaded {json_files_uploaded} JSON result files to Object Storage")
            else:
                # Copy to local work directory
                logger.info(f"Copying to local storage: work/{token}")
                work_output_dir = Path('work') / token
                work_output_dir.parent.mkdir(parents=True, exist_ok=True)
                
                if work_output_dir.exists():
                    shutil.rmtree(work_output_dir)
                    
                shutil.copytree(pipeline_output_dir, work_output_dir)
                logger.info(f"✅ Results copied to local storage")
            
            # Upload original file to Supabase Storage (non-blocking)
            storage_path = None
            try:
                supabase_storage = SupabaseStorageService()
                if supabase_storage.enabled:
                    storage_path = f"uploads/{user_id}/{token}/{filename}"
                    
                    # Non-blocking upload attempt
                    with open(file_path, 'rb') as f:
                        upload_success = supabase_storage.upload_file_from_stream(f, storage_path)
                        
                    if upload_success:
                        logger.info(f"✅ Uploaded original file to Supabase Storage: {storage_path}")
                    else:
                        logger.warning(f"⚠️  Storage upload skipped (RLS policies may need adjustment)")
                        storage_path = None  # Don't save path if upload failed
            except Exception as e:
                logger.warning(f"⚠️  Storage upload failed (non-critical): {e}")
                storage_path = None
            
            # Save to Supabase history
            try:
                history_service.save_processing(
                    token=token,
                    filename=filename,
                    pipeline_result=pipeline_result,
                    user_id=user_id,
                    file_size_bytes=bytes_written,
                    file_hash=file_hash,
                    storage_path=storage_path
                )
                logger.info(f"✅ Saved processing history to Supabase for token={token}")
            except Exception as e:
                logger.warning(f"Failed to save to Supabase (non-critical): {e}")
            
            # Clear processing status (job is complete)
            if token in PROCESSING_STATUS:
                del PROCESSING_STATUS[token]
            
            # Prepare response
            result = {
                'success': True,
                'message': message,
                'token': token,
                'download_url': f'/api/download/result/{token}',
                'dashboard_url': f'/dashboard/{token}'
            }
            
            return jsonify(result), 200
            
        except Exception as e:
            # Clear processing status on error
            if token in PROCESSING_STATUS:
                del PROCESSING_STATUS[token]

            logger.error(f"Processing error for {token}: {e}")
            logger.error(traceback.format_exc())
            if upload_record_id:
                upload_service.update_upload_status(
                    upload_record_id,
                    status='error',
                    processed=False,
                    error_message=str(e),
                )
            return jsonify({'error': f'Erro no processamento: {str(e)}'}), 500

    except Exception as e:
        logger.error(f"Upload error: {e}")
        logger.error(traceback.format_exc())
        if upload_record_id:
            upload_service.update_upload_status(
                upload_record_id,
                status='error',
                processed=False,
                error_message=str(e),
            )
        return jsonify({'error': f'Erro no upload: {str(e)}'}), 500
        
    finally:
        # ALWAYS clean up temp files
        if token:
            cleanup_temp_files(token)
            logger.info(f"✅ Temp files cleaned for {token}")

@simple_upload_bp.route('/api/upload/status/<token>', methods=['GET'])
@login_required
def get_status(token):
    """
    Get status of a processing job
    Returns real-time progress information during processing
    """
    try:
        # Check if token is in processing status
        if token in PROCESSING_STATUS:
            status_info = PROCESSING_STATUS[token]
            return jsonify({
                'token': token,
                'status': status_info.get('status', 'processing'),
                'progress': status_info.get('progress', 0),
                'message': status_info.get('message', 'A processar...'),
                'total_hands': status_info.get('total_hands', 0),
                'processed_hands': status_info.get('processed_hands', 0)
            }), 200
        
        # Otherwise check if results exist (completed jobs)
        from app.services.storage import get_storage
        
        storage = get_storage()
        
        if storage.use_cloud:
            # Check cloud storage
            test_file = f"results/{token}/pipeline_result.json"
            exists = storage.download_file(test_file) is not None
        else:
            # Check local storage
            work_dir = Path('work') / token
            exists = work_dir.exists()
        
        if exists:
            return jsonify({
                'token': token,
                'status': 'completed',
                'progress': 100,
                'message': 'Processamento concluído',
                'download_url': f'/api/download/result/{token}'
            }), 200
        else:
            return jsonify({
                'token': token,
                'status': 'not_found',
                'message': 'Resultado não encontrado'
            }), 404
            
    except Exception as e:
        logger.error(f"Status check error: {e}")
        return jsonify({'error': f'Erro ao verificar status: {str(e)}'}), 500