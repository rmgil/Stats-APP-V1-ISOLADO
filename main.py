import os
import shutil
import re
import tempfile
import zipfile
import uuid
import logging
import csv
import io
import time
import sqlite3
import json
import traceback
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, request, send_file, abort, flash, redirect, url_for, Response, jsonify, session, stream_with_context, make_response
from werkzeug.utils import secure_filename
import magic
import rarfile
import chardet
from flask_login import LoginManager, login_required

# Import partition module
from app.partition.runner import build_partitions

# Import stats module
from app.stats.engine import run_stats

# Import hands API blueprint
from app.hands.api import bp as hands_api_bp
from app.api_dashboard import build_dashboard_payload
from app.dashboard.api import bp_dashboard
from app.dashboard.routes import dashboard_bp

# Import simplified workflow blueprint
from app.api.simplified import bp as simplified_bp

# Import authentication blueprint
from app.auth import auth_bp
from app.auth.routes import *
from app.models.user import User
from app.services.supabase_client import supabase_service

# Import admin blueprint
from app.admin import admin_bp
from app.admin.routes import *
from app.admin.initializer import initialize_production_emails, ensure_primary_admin

# Import simple upload API (simplified synchronous version)
from app.api.simple_upload import simple_upload_bp

# Import database pool only (no background worker needed)
from app.services.db_pool import DatabasePool

# Import database migrations
from app.database_migrations import check_and_run_migrations

# Import tmp cleanup service
from app.services.tmp_cleanup import run_startup_cleanup

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Run /tmp cleanup at startup to prevent disk quota issues
run_startup_cleanup()

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")

# Configure Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Por favor, faça login para acessar esta página.'
login_manager.login_message_category = 'info'

@login_manager.user_loader
def load_user(user_id):
    """Load user from session data"""
    # Try to get user from session
    user_data = session.get('user_data')
    if user_data and str(user_data.get('id')) == str(user_id):
        return User(user_data)
    
    # Try to get user from Supabase if session exists
    supabase_session = session.get('supabase_session')
    if supabase_session:
        user = supabase_service.get_user()
        if user and user.user:
            return User(user.user.dict())
    
    return None

# Configure Flask for deployment
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB limit for MTT uploads
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # Disable caching for downloads

# Global dictionary to track chunked uploads with persistent storage
CHUNKED_UPLOADS = {}

# Persistent storage file for upload tracking
UPLOAD_TRACKING_FILE = Path('/tmp/upload_tracking.json')

# Database for persistent result tracking
DB_PATH = '/tmp/results.db'

def init_db():
    """Initialize the SQLite database for result tracking."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            upload_id TEXT PRIMARY KEY,
            result_path TEXT NOT NULL,
            zip_filename TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def save_result_to_db(upload_id: str, result_path: str, zip_filename: str):
    """Save result information to database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO results (upload_id, result_path, zip_filename) VALUES (?, ?, ?)',
            (upload_id, result_path, zip_filename)
        )
        conn.commit()
        conn.close()
        app.logger.info(f"Saved result to database: {upload_id}")
        return True
    except Exception as e:
        app.logger.error(f"Failed to save result to database: {e}")
        return False

def get_result_from_db(upload_id: str):
    """Get result information from database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT result_path, zip_filename FROM results WHERE upload_id = ?', (upload_id,))
        result = cursor.fetchone()
        conn.close()

        if result:
            app.logger.info(f"Found result in database: {upload_id}")
            return result[0], result[1]  # result_path, zip_filename
        return None, None
    except Exception as e:
        app.logger.error(f"Failed to get result from database: {e}")
        return None, None

def create_download_response(file_path: Path, filename: str):
    """Create a streaming download response that works in deployment."""
    try:
        # Convert to Path if string
        if isinstance(file_path, str):
            file_path = Path(file_path)

        # Verify file exists and is readable
        if not file_path.exists():
            app.logger.error(f"File not found for download: {file_path}")
            abort(404, 'File not found')

        file_size = file_path.stat().st_size
        app.logger.info(f"Creating download response for {filename} ({file_size} bytes) from {file_path}")

        def generate():
            total_sent = 0
            try:
                with open(file_path, 'rb') as f:
                    # Dynamic chunk size based on file size
                    if file_size > 100 * 1024 * 1024:  # 100MB+
                        chunk_size = 1024 * 1024  # 1MB chunks for large files
                    elif file_size > 10 * 1024 * 1024:  # 10MB+
                        chunk_size = 512 * 1024   # 512KB chunks
                    else:
                        chunk_size = 256 * 1024   # 256KB chunks for small files
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        total_sent += len(chunk)
                        yield chunk
                app.logger.info(f"Completed streaming {filename} ({total_sent} bytes)")
            except IOError as e:
                app.logger.error(f"IO Error streaming {filename}: {e}")
                # Continue yielding empty to complete response
                yield b''
            except Exception as e:
                app.logger.error(f"Error streaming {filename}: {e}")
                # Continue yielding empty to complete response
                yield b''

        # Create response for deployment compatibility
        try:
            response = Response(
                stream_with_context(generate()),
                mimetype='application/zip',
                direct_passthrough=True
            )

            # Set headers after Response creation
            response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
            response.headers['Content-Type'] = 'application/zip'
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['X-Accel-Buffering'] = 'no'

            app.logger.info(f"Response created successfully for {filename}")
            return response

        except Exception as resp_error:
            app.logger.error(f"Error creating Response object: {resp_error}")
            # Fallback to simple response
            return Response(
                generate(),
                mimetype='application/zip',
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"'
                }
            )

    except Exception as e:
        app.logger.error(f"Error in create_download_response: {e}")
        import traceback
        app.logger.error(f"Full traceback: {traceback.format_exc()}")
        # Return error response instead of abort
        return jsonify({'success': False, 'error': 'Error preparing download'}), 500

# Regex patterns for filtering
WORD_MYSTERY = re.compile(r'\b(mystery|mysteries)\b', re.I)
WORD_PKO = re.compile(r'\b(bounty|bounties|progressive|pko|ko|knockout)\b', re.I)

# Enhanced detection patterns for specific networks
POKERSTARS_PKO_PATTERN = re.compile(r'[€$£]\d+(?:\.\d+)?\s*\+\s*[€$£]\d+(?:\.\d+)?\s*\+')  # X+Y+fee format
GG_BOUNTY_HUNTERS_PATTERN = re.compile(r'Bounty Hunter', re.I)
GG_MYSTERY_PATTERN = re.compile(r'Mystery Bounty', re.I)

# Try to bypass Replit's proxy limits by allowing unlimited size
app.config['MAX_CONTENT_LENGTH'] = None

# Additional Flask configuration for large files
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['UPLOAD_FOLDER'] = '/tmp'

# Additional settings for deployment environment
app.config['PROPAGATE_EXCEPTIONS'] = True
app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = False

# Initialize database on startup
init_db()

def save_upload_state():
    """Save upload state to persistent storage"""
    try:
        # Convert Path objects to strings for JSON serialization
        serializable_state = {}
        for upload_id, info in CHUNKED_UPLOADS.items():
            serializable_state[upload_id] = {
                'upload_dir': str(info['upload_dir']),
                'file_name': info['file_name'],
                'total_chunks': info['total_chunks'],
                'received_chunks': {k: str(v) for k, v in info['received_chunks'].items()},
                'created_at': info['created_at']
            }
            # Add result path if it exists
            if 'result_path' in info:
                serializable_state[upload_id]['result_path'] = str(info['result_path'])
            if 'zip_filename' in info:
                serializable_state[upload_id]['zip_filename'] = info['zip_filename']

        with open(UPLOAD_TRACKING_FILE, 'w') as f:
            import json
            json.dump(serializable_state, f)
    except Exception as e:
        app.logger.warning(f"Failed to save upload state: {e}")

def load_upload_state():
    """Load upload state from persistent storage"""
    try:
        if UPLOAD_TRACKING_FILE.exists():
            with open(UPLOAD_TRACKING_FILE, 'r') as f:
                import json
                serializable_state = json.load(f)

            # Convert back to Path objects
            for upload_id, info in serializable_state.items():
                CHUNKED_UPLOADS[upload_id] = {
                    'upload_dir': Path(info['upload_dir']),
                    'file_name': info['file_name'],
                    'total_chunks': info['total_chunks'],
                    'received_chunks': {int(k): Path(v) for k, v in info['received_chunks'].items()},
                    'created_at': info['created_at']
                }
                # Add result path if it exists
                if 'result_path' in info:
                    CHUNKED_UPLOADS[upload_id]['result_path'] = Path(info['result_path'])
                if 'zip_filename' in info:
                    CHUNKED_UPLOADS[upload_id]['zip_filename'] = info['zip_filename']

    except Exception as e:
        app.logger.warning(f"Failed to load upload state: {e}")

# Load existing upload state on startup
load_upload_state()

def extract_single_archive(archive_path: Path, dest_dir: Path):
    """
    Extract a single archive file (ZIP or RAR) to destination directory.
    Returns True if successful.
    """
    try:
        # Validate file exists and size
        if not archive_path.exists():
            app.logger.error(f"Archive file not found: {archive_path}")
            return False

        file_size = archive_path.stat().st_size
        app.logger.info(f"Extracting archive: {archive_path.name} ({file_size} bytes)")

        # Detect MIME type with error handling
        try:
            mime = magic.from_file(str(archive_path), mime=True)
            app.logger.debug(f"Detected MIME type: {mime} for file: {archive_path.name}")
        except Exception as mime_error:
            app.logger.warning(f"Failed to detect MIME type: {mime_error}")
            mime = None

        # Try ZIP extraction first
        if mime == 'application/zip' or archive_path.suffix.lower() == '.zip' or mime is None:
            try:
                with zipfile.ZipFile(archive_path, 'r') as z:
                    # Basic zip bomb protection
                    total_size = sum(info.file_size for info in z.infolist())
                    if total_size > 2 * 1024 * 1024 * 1024:  # 2GB limit
                        app.logger.error(f"Archive too large when uncompressed: {total_size} bytes")
                        return False

                    z.extractall(dest_dir)
                    app.logger.info(f"Extracted ZIP file: {archive_path.name} ({len(z.namelist())} files)")
                    return True
            except zipfile.BadZipFile:
                app.logger.debug("Not a valid ZIP file, trying RAR...")
            except Exception as zip_error:
                app.logger.debug(f"ZIP extraction failed: {zip_error}")

        # Try RAR extraction
        if mime in ('application/x-rar', 'application/x-rar-compressed') or archive_path.suffix.lower() == '.rar':
            try:
                with rarfile.RarFile(archive_path) as r:
                    r.extractall(dest_dir)
                    app.logger.info(f"Extracted RAR file: {archive_path.name} ({len(r.namelist())} files)")
                    return True
            except rarfile.BadRarFile:
                app.logger.debug("Not a valid RAR file")
            except Exception as rar_error:
                app.logger.debug(f"RAR extraction failed: {rar_error}")

        return False

    except Exception as e:
        app.logger.error(f"Error extracting {archive_path}: {e}")
        return False

def unpack_any(archive_path: Path, dest_dir: Path, max_depth: int = 5):
    """
    Extracts ZIP or RAR files recursively, handling nested archives.
    If it's a regular directory, copies it.
    max_depth prevents infinite recursion.
    """
    try:
        if archive_path.is_dir():
            # If it's already a directory, copy its contents
            for item in archive_path.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest_dir)
                elif item.is_dir():
                    shutil.copytree(item, dest_dir / item.name, dirs_exist_ok=True)
            return True

        # Extract the main archive
        temp_extract_dir = dest_dir / f"_temp_extract_{archive_path.stem}"
        temp_extract_dir.mkdir(parents=True, exist_ok=True)

        if not extract_single_archive(archive_path, temp_extract_dir):
            # If it's not an archive, try to copy as regular file
            try:
                shutil.copy2(archive_path, dest_dir)
                app.logger.info(f"Copied regular file: {archive_path.name}")
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                return True
            except Exception as copy_error:
                app.logger.error(f"Failed to copy file: {copy_error}")
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                return False

        # Move all files from temp directory to destination first
        for item in temp_extract_dir.rglob('*'):
            if item.is_file():
                # Create relative path structure in destination
                rel_path = item.relative_to(temp_extract_dir)
                dest_path = dest_dir / rel_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, dest_path)

        # Clean up temp directory
        shutil.rmtree(temp_extract_dir, ignore_errors=True)

        # Now recursively extract any nested archives in the destination directory
        extracted_count = extract_nested_archives(dest_dir, max_depth)
        app.logger.info(f"Extracted {extracted_count} nested archives from {archive_path.name}")

        return True

    except Exception as e:
        app.logger.error(f"Error unpacking {archive_path}: {e}")
        return False

def extract_nested_archives(directory: Path, max_depth: int = 5, current_depth: int = 0):
    """
    Recursively extract any ZIP or RAR files found in the directory.
    Returns the count of archives extracted.
    """
    if current_depth >= max_depth:
        app.logger.warning(f"Maximum extraction depth ({max_depth}) reached")
        return 0

    extracted_count = 0

    # Find all potential archive files
    archive_extensions = {'.zip', '.rar'}
    potential_archives = []

    for ext in archive_extensions:
        found = list(directory.rglob(f'*{ext}'))
        app.logger.debug(f"Found {len(found)} {ext} files at depth {current_depth}")
        potential_archives.extend(found)

    app.logger.info(f"Total archives to process at depth {current_depth}: {len(potential_archives)}")

    for archive_file in potential_archives:
        try:
            app.logger.info(f"Found nested archive at depth {current_depth}: {archive_file.name}")

            # Create extraction directory next to the archive
            extract_dir = archive_file.parent / f"_extracted_{archive_file.stem}"
            extract_dir.mkdir(parents=True, exist_ok=True)

            # Extract the archive
            if extract_single_archive(archive_file, extract_dir):
                extracted_count += 1

                # Recursively check for more nested archives
                nested_count = extract_nested_archives(extract_dir, max_depth, current_depth + 1)
                extracted_count += nested_count

                # Move TXT and XML files to parent directory
                for item in extract_dir.rglob('*'):
                    if item.is_file():
                        # Process .txt and .xml files
                        if item.suffix.lower() in ['.txt', '.xml']:
                            dest = archive_file.parent / item.relative_to(extract_dir)
                            dest.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(item, dest)
                        elif item.suffix.lower() not in archive_extensions:
                            # Log other files being ignored
                            app.logger.debug(f"Ignoring file: {item.name} ({item.suffix})")

                # Remove the extracted archive and temp directory
                archive_file.unlink()
                shutil.rmtree(extract_dir, ignore_errors=True)
            else:
                # Clean up if extraction failed
                shutil.rmtree(extract_dir, ignore_errors=True)

        except Exception as e:
            app.logger.error(f"Error processing nested archive {archive_file}: {e}")

    return extracted_count

def process_txt_tree(root_dir: Path, output_dir: Path):
    """
    Processes all TXT and XML files in the directory tree and categorizes them.
    Searches for keywords in both filename and file content.
    Returns both statistics and detailed file classification info.
    """
    pko_dir = output_dir / 'PKO'
    nonko_dir = output_dir / 'NON-KO'
    mysteries_dir = output_dir / 'MYSTERIES'
    pko_dir.mkdir(parents=True, exist_ok=True)
    nonko_dir.mkdir(parents=True, exist_ok=True)
    mysteries_dir.mkdir(parents=True, exist_ok=True)

    processed_count = 0
    pko_count = 0
    nonko_count = 0
    mystery_count = 0
    unknown_count = 0

    # Lists to store content for compiled files
    pko_contents = []
    nonko_contents = []
    mystery_contents = []

    # List to store file classification details for manifest
    file_classifications = []

    # Find all files first to count other files
    all_files = list(root_dir.rglob('*'))
    all_files_count = len([f for f in all_files if f.is_file()])

    # Find all TXT and XML files recursively
    txt_files = list(root_dir.rglob('*.txt'))
    xml_files = list(root_dir.rglob('*.xml'))
    processable_files = txt_files + xml_files
    other_files_count = all_files_count - len(processable_files)

    app.logger.info(f"Found {all_files_count} total files: {len(processable_files)} TXT/XML files to process ({len(txt_files)} TXT, {len(xml_files)} XML), {other_files_count} other files to ignore")

    # Log some examples of other files being ignored
    if other_files_count > 0:
        other_files = [f for f in all_files if f.is_file() and f.suffix.lower() not in ['.txt', '.xml']][:5]  # First 5 examples
        for f in other_files:
            app.logger.debug(f"Ignoring file: {f.name} ({f.suffix})")

    for file in processable_files:
        try:
            processed_count += 1

            # Get filename for pattern matching
            filename = file.name
            app.logger.debug(f"Processing file: {filename}")

            # Get relative path from root_dir for manifest
            relative_path = file.relative_to(root_dir)

            # Get file size
            file_size = file.stat().st_size

            # Check if file is valid (not empty or binary-only)
            is_valid = True
            content = None
            used_encoding = "unknown"

            try:
                # Detect encoding using chardet
                with open(file, 'rb') as f:
                    raw_data = f.read()

                    # Check if file is empty
                    if not raw_data:
                        is_valid = False
                        content = ""
                        used_encoding = "empty"
                        app.logger.warning(f"Empty file: {file}")
                    else:
                        detection = chardet.detect(raw_data)
                        detected_encoding = detection.get('encoding', 'utf-8') if detection else 'utf-8'
                        confidence = detection.get('confidence', 0) if detection else 0

                        # If confidence is low, try common encodings
                        if confidence < 0.7:
                            encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
                            if detected_encoding and detected_encoding not in encodings_to_try:
                                encodings_to_try.insert(0, detected_encoding)
                        else:
                            encodings_to_try = [detected_encoding, 'utf-8', 'latin1']

                        # Try to decode with detected or fallback encodings
                        for encoding in encodings_to_try:
                            try:
                                if encoding:
                                    content = raw_data.decode(encoding, errors='replace')
                                    used_encoding = encoding
                                    # Check if too many replacement characters (likely binary file)
                                    replacement_char_count = content.count('')
                                    if replacement_char_count > len(content) * 0.3:  # More than 30% replacement chars
                                        is_valid = False
                                        app.logger.warning(f"File appears to be binary or corrupted: {file}")
                                    break
                            except (UnicodeDecodeError, LookupError):
                                continue

                        if content is None:
                            # Last resort: use utf-8 with replace errors
                            content = raw_data.decode('utf-8', errors='replace')
                            used_encoding = "utf-8-fallback"
                            app.logger.warning(f"Using fallback encoding for: {file}")
                            # Check if valid text
                            replacement_char_count = content.count('')
                            if replacement_char_count > len(content) * 0.3:
                                is_valid = False

            except Exception as e:
                app.logger.error(f"Error reading file {file}: {e}")
                is_valid = False
                content = ""
                used_encoding = "error"

            # Enhanced detection with network-specific rules
            # First try GG-specific Mystery Bounty pattern
            gg_mystery = GG_MYSTERY_PATTERN.search(content[:1000]) if content else False
            
            # Then check for general mystery words
            mystery_in_filename = WORD_MYSTERY.search(filename)
            mystery_in_content = WORD_MYSTERY.search(content) if content else False

            if gg_mystery or mystery_in_filename or mystery_in_content:
                # Generate unique filename to avoid conflicts
                base_name = file.stem
                counter = 1
                new_name = f"{base_name}{file.suffix}"
                target_path = mysteries_dir / new_name

                while target_path.exists():
                    new_name = f"{base_name}_{counter}{file.suffix}"
                    target_path = mysteries_dir / new_name
                    counter += 1

                shutil.copy2(file, target_path)
                mystery_count += 1

                # Add content to mystery compilation
                if content:
                    mystery_contents.append(f"=== {file.name} ===\n{content}\n\n")

                # Determine reason for classification
                if gg_mystery:
                    reason = "'Mystery Bounty' detected (GGPoker format)"
                    app.logger.debug(f"Moved mystery file (GG Mystery): {file.name}")
                elif mystery_in_filename:
                    match = mystery_in_filename.group(1) if mystery_in_filename else "mystery"
                    reason = f"'{match}' in filename"
                    app.logger.debug(f"Moved mystery file (filename): {file.name}")
                else:
                    match = mystery_in_content.group(1) if mystery_in_content else "mystery"
                    reason = f"'{match}' in content"
                    app.logger.debug(f"Moved mystery file (content): {file.name}")

                # Add to file classifications
                file_classifications.append({
                    "input": str(relative_path),
                    "output_class": "mystery",
                    "detector": {
                        "reason": reason,
                        "score": 1.0
                    },
                    "encoding": used_encoding,
                    "bytes": file_size
                })

                continue

            # Check for PKO with enhanced network-specific rules
            # First check for PokerStars X+Y+fee format
            pokerstars_pko = POKERSTARS_PKO_PATTERN.search(content[:1000]) if content else False
            
            # Check for GG Bounty Hunters
            gg_bounty_hunters = GG_BOUNTY_HUNTERS_PATTERN.search(content[:1000]) if content else False
            
            # Then check general PKO patterns
            pko_in_filename = WORD_PKO.search(filename)
            pko_in_content = WORD_PKO.search(content) if content else False

            if pokerstars_pko or gg_bounty_hunters or pko_in_filename or pko_in_content:
                # Generate unique filename to avoid conflicts
                dest_name = file.name
                counter = 1
                while (pko_dir / dest_name).exists():
                    name_part = file.stem
                    ext_part = file.suffix
                    dest_name = f"{name_part}_{counter}{ext_part}"
                    counter += 1

                shutil.copy2(file, pko_dir / dest_name)
                pko_count += 1

                # Add content to PKO compilation
                if content:
                    pko_contents.append(f"=== {file.name} ===\n{content}\n\n")

                # Determine reason for classification
                if pokerstars_pko:
                    reason = "PokerStars PKO format (X+Y+fee) detected"
                    app.logger.debug(f"Copied to PKO (PokerStars format): {file.name}")
                elif gg_bounty_hunters:
                    reason = "'Bounty Hunters' detected (GGPoker)"
                    app.logger.debug(f"Copied to PKO (GG Bounty Hunters): {file.name}")
                elif pko_in_filename:
                    match = pko_in_filename.group(1) if pko_in_filename else "pko"
                    reason = f"'{match}' in filename"
                    app.logger.debug(f"Copied to PKO (filename): {file.name}")
                else:
                    match = pko_in_content.group(1) if pko_in_content else "pko"
                    reason = f"'{match}' in content"
                    app.logger.debug(f"Copied to PKO (content): {file.name}")

                # Add to file classifications
                file_classifications.append({
                    "input": str(relative_path),
                    "output_class": "PKO",
                    "detector": {
                        "reason": reason,
                        "score": 1.0
                    },
                    "encoding": used_encoding,
                    "bytes": file_size
                })
            else:
                # Check if file is valid or unknown
                if not is_valid:
                    # Unknown/invalid files go to NON-KO to maintain backwards compatibility
                    dest_name = file.name
                    counter = 1
                    while (nonko_dir / dest_name).exists():
                        name_part = file.stem
                        ext_part = file.suffix
                        dest_name = f"{name_part}_{counter}{ext_part}"
                        counter += 1

                    shutil.copy2(file, nonko_dir / dest_name)
                    unknown_count += 1  # Count as unknown internally

                    app.logger.debug(f"Copied unknown/invalid file to NON-KO: {file.name}")

                    # Add to file classifications as unknown
                    file_classifications.append({
                        "input": str(relative_path),
                        "output_class": "unknown",  # Marked as unknown in manifest
                        "detector": {
                            "reason": "invalid or binary content",
                            "score": 0.5
                        },
                        "encoding": used_encoding,
                        "bytes": file_size
                    })
                else:
                    # Regular valid file goes to NON-KO
                    dest_name = file.name
                    counter = 1
                    while (nonko_dir / dest_name).exists():
                        name_part = file.stem
                        ext_part = file.suffix
                        dest_name = f"{name_part}_{counter}{ext_part}"
                        counter += 1

                    shutil.copy2(file, nonko_dir / dest_name)
                    nonko_count += 1

                    # Add content to NON-KO compilation
                    if content:
                        nonko_contents.append(f"=== {file.name} ===\n{content}\n\n")

                    app.logger.debug(f"Copied to NON-KO: {file.name}")

                    # Add to file classifications
                    file_classifications.append({
                        "input": str(relative_path),
                        "output_class": "non-KO",
                        "detector": {
                            "reason": "no special keywords found",
                            "score": 1.0
                        },
                        "encoding": used_encoding,
                        "bytes": file_size
                    })

        except Exception as e:
            app.logger.error(f'Error processing {file}: {e}')
            continue

    # Create compiled files for each category (now includes both TXT and XML content)
    app.logger.info("Creating compiled files...")

    if pko_contents:
        pko_compiled_path = output_dir / 'PKO.txt'
        with open(pko_compiled_path, 'w', encoding='utf-8') as f:
            f.write(''.join(pko_contents))
        app.logger.debug(f"Created PKO.txt with {len(pko_contents)} files")

    if nonko_contents:
        nonko_compiled_path = output_dir / 'NON-KO.txt'
        with open(nonko_compiled_path, 'w', encoding='utf-8') as f:
            f.write(''.join(nonko_contents))
        app.logger.debug(f"Created NON-KO.txt with {len(nonko_contents)} files")

    if mystery_contents:
        mystery_compiled_path = output_dir / 'MYSTERIES.txt'
        with open(mystery_compiled_path, 'w', encoding='utf-8') as f:
            f.write(''.join(mystery_contents))
        app.logger.debug(f"Created MYSTERIES.txt with {len(mystery_contents)} files")

    app.logger.info(f"Processing complete: {processed_count} files processed, "
                   f"{pko_count} PKO, {nonko_count} NON-KO, {mystery_count} mysteries moved to MYSTERIES folder")

    return {
        'processed': processed_count,
        'pko': pko_count,
        'nonko': nonko_count,
        'mystery': mystery_count,
        'unknown': unknown_count,
        'file_classifications': file_classifications
    }

# Register hands API blueprint
app.register_blueprint(hands_api_bp)

# Register dashboard API blueprint
app.register_blueprint(bp_dashboard)

# Register dashboard frontend blueprint
app.register_blueprint(dashboard_bp)

# Register simplified workflow blueprint
app.register_blueprint(simplified_bp)

# Register authentication blueprint
app.register_blueprint(auth_bp)

# Register admin blueprint
app.register_blueprint(admin_bp)

# Register simple upload API blueprint (replaces distributed upload)
app.register_blueprint(simple_upload_bp)

# Register history API blueprint
from app.api.history import history_bp
app.register_blueprint(history_bp)

# Register cleanup admin API blueprint
from app.api.cleanup_admin import cleanup_admin_bp
app.register_blueprint(cleanup_admin_bp)

# Blueprint registration handled elsewhere

@app.route('/')
def index():
    # Smart redirect based on authentication status
    from flask_login import current_user
    
    if current_user.is_authenticated:
        # User is logged in, redirect to upload page
        return redirect('/upload')
    else:
        # User is not logged in, redirect to login
        return redirect('/auth/login')

@app.route('/history')
@login_required
def history_page():
    """Display processing history for current user"""
    return render_template('history.html')

@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint for monitoring and load balancers
    
    Returns:
        - 200 OK: Service is healthy
        - 503 Service Unavailable: Service has issues
    """
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'poker-stats-manager',
        'environment': 'DEPLOYMENT' if os.environ.get('DEPLOYMENT') else 'LOCAL',
        'checks': {}
    }
    
    # Check database connectivity
    try:
        from app.services.db_pool import DatabasePool
        conn = DatabasePool.get_connection()
        DatabasePool.return_connection(conn)
        health_status['checks']['database'] = 'healthy'
    except Exception as e:
        health_status['checks']['database'] = f'unhealthy: {str(e)}'
        health_status['status'] = 'unhealthy'
    
    # Check Object Storage availability (production only)
    try:
        from app.services.storage import get_storage
        storage = get_storage()
        health_status['checks']['storage'] = 'cloud' if storage.use_cloud else 'local'
        health_status['checks']['storage_mode'] = 'production' if storage.use_cloud else 'development'
    except Exception as e:
        health_status['checks']['storage'] = f'error: {str(e)}'
        health_status['status'] = 'unhealthy'
    
    # Check background worker status
    try:
        from app.services.job_queue_service import JobQueueService
        job_queue = JobQueueService()
        pending_count = job_queue.count_pending_jobs()
        
        health_status['checks']['pending_jobs'] = pending_count
        
        # Warning if too many pending jobs (potential backlog)
        if pending_count > 10:
            health_status['checks']['worker_status'] = 'warning: high backlog'
        else:
            health_status['checks']['worker_status'] = 'healthy'
    except Exception as e:
        health_status['checks']['worker_status'] = f'error: {str(e)}'
    
    # Return appropriate HTTP status
    status_code = 200 if health_status['status'] == 'healthy' else 503
    return jsonify(health_status), status_code

@app.route('/upload-legacy', methods=['POST'])
def upload_legacy():
    """Handle simple file upload with fallback to chunked processing."""
    # Force JSON response headers
    response_headers = {
        'Content-Type': 'application/json',
        'X-Content-Type-Options': 'nosniff'
    }

    app.logger.info("=== UPLOAD REQUEST RECEIVED ===")
    app.logger.info(f"Request method: {request.method}")
    app.logger.info(f"Content type: {request.content_type}")
    app.logger.info(f"Content length: {request.content_length}")
    app.logger.info(f"Environment: {'DEPLOYMENT' if os.environ.get('DEPLOYMENT') else 'LOCAL'}")

    # Check if request is too large (deployment limit)
    if request.content_length and request.content_length > 50 * 1024 * 1024:  # 50MB
        app.logger.warning(f"Large file detected: {request.content_length / (1024*1024):.1f}MB - should use chunked upload")
        resp = make_response(jsonify({
            'success': False,
            'error': f'Ficheiro muito grande ({request.content_length / (1024*1024):.1f}MB). Este endpoint aceita apenas ficheiros até 50MB. Para ficheiros maiores, o sistema deveria usar upload em partes automaticamente.',
            'size': request.content_length,
            'max_size': 50 * 1024 * 1024
        }), 413)
        resp.headers.update(response_headers)
        return resp

    if 'file' not in request.files:
        app.logger.error("No file in request")
        resp = make_response(jsonify({'success': False, 'error': 'Nenhum ficheiro enviado'}), 400)
        resp.headers.update(response_headers)
        return resp

    file = request.files['file']
    if file.filename == '':
        app.logger.error("Empty filename")
        resp = make_response(jsonify({'success': False, 'error': 'Nome de ficheiro vazio'}), 400)
        resp.headers.update(response_headers)
        return resp

    if not file:
        app.logger.error("No file selected")
        resp = make_response(jsonify({'success': False, 'error': 'Nenhum ficheiro selecionado'}), 400)
        resp.headers.update(response_headers)
        return resp

    # Generate unique session ID for isolation
    session_id = uuid.uuid4().hex
    app.logger.info(f"Processing upload with session ID: {session_id}")
    app.logger.info(f"File info - name: {file.filename}, content_type: {file.content_type}")

    # Create temporary directories
    temp_root = Path(tempfile.gettempdir())
    app.logger.info(f"Temp directory: {temp_root}")
    app.logger.info(f"Temp directory exists: {temp_root.exists()}")
    app.logger.info(f"Temp directory writable: {os.access(temp_root, os.W_OK)}")

    tmp_base = temp_root / f"txt_filter_{session_id}"
    raw_dir = tmp_base / 'raw'
    work_dir = tmp_base / 'work'
    out_dir = tmp_base / 'out'

    try:
        # Create directories
        raw_dir.mkdir(parents=True, exist_ok=True)
        work_dir.mkdir(exist_ok=True)
        out_dir.mkdir(exist_ok=True)

        # Save uploaded file with streaming for large files
        filename = file.filename or "uploaded_file"
        file_path = raw_dir / filename

        # Stream file to disk to handle large files efficiently
        app.logger.info(f"Streaming large file to: {file_path}")
        total_size = 0

        try:
            with open(file_path, 'wb') as f:
                while True:
                    chunk = file.stream.read(8192)  # Read in 8KB chunks
                    if not chunk:
                        break
                    f.write(chunk)
                    total_size += len(chunk)
        except Exception as write_error:
            app.logger.error(f"Error writing file to disk: {write_error}")
            raise Exception(f"Erro ao gravar ficheiro: {str(write_error)}")

        app.logger.info(f"File saved successfully. Size: {total_size} bytes ({total_size / (1024*1024):.1f} MB)")

        # Step 1: Extract/unpack the file
        app.logger.info("Step 1: Extracting archive...")
        try:
            unpack_any(file_path, work_dir)
        except Exception as extract_error:
            app.logger.error(f"Error during extraction: {extract_error}")
            import traceback
            app.logger.error(f"Extraction traceback: {traceback.format_exc()}")
            raise Exception(f"Erro ao extrair arquivo: {str(extract_error)}")

        # Track timing for manifest
        start_time = datetime.now()

        # Step 2: Process and filter TXT/XML files
        app.logger.info("Step 2: Processing TXT and XML files...")
        try:
            stats = process_txt_tree(work_dir, out_dir)
        except Exception as process_error:
            app.logger.error(f"Error during TXT/XML processing: {process_error}")
            raise Exception(f"Erro ao processar ficheiros TXT/XML: {str(process_error)}")

        end_time = datetime.now()

        # Create classification manifest
        manifest = {
            "run_id": session_id,
            "started_at": start_time.isoformat(),
            "finished_at": end_time.isoformat(),
            "totals": {
                "PKO": stats.get('pko', 0),
                "mystery": stats.get('mystery', 0),
                "non-KO": stats.get('nonko', 0),
                "unknown": stats.get('unknown', 0)
            },
            "files": stats.get('file_classifications', [])
        }

        # Save manifest to output directory
        manifest_path = out_dir / 'classification_manifest.json'
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        app.logger.info(f"Created classification manifest with {len(manifest['files'])} files")

        # Parser integration - check for environment variable or request parameter
        enable_parser = os.environ.get('ENABLE_PARSER', 'false').lower() == 'true'

        # Also check if parser was requested via form data (for backoffice use)
        if request.form:
            enable_parser = enable_parser or request.form.get('enable_parser', 'false').lower() == 'true'

        if enable_parser:
            try:
                app.logger.info("Parser enabled - processing classified files...")
                from app.parse.runner import parse_folder

                # Create parsed directory
                parsed_dir = out_dir / 'parsed'
                parsed_dir.mkdir(exist_ok=True)

                # Output file for JSONL
                out_jsonl = parsed_dir / 'hands.jsonl'

                # Default hero aliases path
                aliases_path = Path('./app/config/hero_aliases.json')

                # Run parser on classified folders
                parse_stats = parse_folder(
                    in_root=str(out_dir),
                    out_jsonl=str(out_jsonl),
                    hero_aliases_path=str(aliases_path) if aliases_path.exists() else ""
                )

                # Save parse statistics
                stats_path = parsed_dir / 'parse_stats.json'
                with open(stats_path, 'w', encoding='utf-8') as f:
                    json.dump(parse_stats, f, indent=2)

                app.logger.info(f"Parser completed: {parse_stats.get('hands', 0)} hands extracted to {out_jsonl}")
                app.logger.info(f"Parse statistics saved to {stats_path}")

                # Add parser info to manifest
                manifest['parser'] = {
                    'enabled': True,
                    'hands_extracted': parse_stats.get('hands', 0),
                    'files_processed': parse_stats.get('files', 0),
                    'output_file': 'parsed/hands.jsonl',
                    'error_log': parse_stats.get('error_log', None)
                }

                # Update manifest with parser info
                with open(manifest_path, 'w', encoding='utf-8') as f:
                    json.dump(manifest, f, indent=2, ensure_ascii=False)

            except Exception as e:
                app.logger.error(f"Parser failed but classification succeeded: {e}")
                import traceback
                app.logger.error(traceback.format_exc())
                # Don't fail the main request if parser fails
                manifest['parser'] = {
                    'enabled': True,
                    'error': str(e)
                }
                with open(manifest_path, 'w', encoding='utf-8') as f:
                    json.dump(manifest, f, indent=2, ensure_ascii=False)

        # Check for DRY_RUN flag (internal flag, default False)
        # Check query params first, then form data, then env
        DRY_RUN = (request.args.get('dry_run', 'false').lower() == 'true') or \
                  (request.form.get('dry_run', 'false').lower() == 'true') or \
                  (os.environ.get('DRY_RUN', 'false').lower() == 'true')

        # Extract original filename without extension
        original_name = Path(filename).stem
        
        if DRY_RUN:
            app.logger.info("DRY_RUN mode: Skipping ZIP creation, returning manifest")
            # Store the session data for potential pipeline processing
            session['last_dry_run'] = {
                'session_id': session_id,
                'out_dir': str(out_dir),
                'manifest': manifest,
                'original_name': original_name
            }
            return jsonify({
                'success': True,
                'dry_run': True,
                'session_id': session_id,
                'manifest': manifest,
                'stats': stats
            })

        # Step 3: Create result ZIP with original name + "_separada"
        app.logger.info("Step 3: Creating result ZIP...")

        # Extract original filename without extension
        original_name = Path(filename).stem
        zip_name = tmp_base / f'{original_name}_separada.zip'
        shutil.make_archive(str(zip_name.with_suffix('')), 'zip', str(out_dir))

        app.logger.info(f"Processing complete. Statistics: {stats}")

        # Save result to database for download persistence
        zip_filename = f'{original_name}_separada.zip'
        save_result_to_db(session_id, str(zip_name), zip_filename)

        # Use streaming response for deployment compatibility
        app.logger.info(f"Preparing download response for: {zip_name}")

        # Try using the centralized download response function
        if zip_name.exists():
            file_size = zip_name.stat().st_size
            app.logger.info(f"File exists, size: {file_size} bytes")

            # For simple uploads, use direct streaming response
            return create_download_response(zip_name, zip_filename)
        else:
            app.logger.error(f"Result file not found: {zip_name}")
            resp = make_response(jsonify({
                'success': False,
                'error': 'Ficheiro de resultado não encontrado'
            }), 500)
            resp.headers['Content-Type'] = 'application/json'
            return resp

    except Exception as e:
        app.logger.error(f"Error processing upload: {e}")
        import traceback
        app.logger.error(f"Full traceback: {traceback.format_exc()}")
        # Clean up on error
        if tmp_base.exists():
            shutil.rmtree(tmp_base, ignore_errors=True)

        # Return JSON error response for better deployment compatibility
        resp = make_response(jsonify({
            'success': False,
            'error': f'Erro no processamento: {str(e)}'
        }), 500)
        resp.headers.update(response_headers)
        return resp

    finally:
        # Schedule cleanup (in a real production app, you'd use a background task)
        # For now, we'll leave cleanup to the OS temp directory management
        pass

def duplicate_bb_fold_columns(headers, data):
    """
    Duplicate 'BB Fold to SB Steal' column for 9max, 6max, and PKO files.
    Returns dictionary with duplicated headers and values.
    """
    duplications = {
        'headers': [],
        'values': []
    }

    # Define which columns to duplicate for each file type
    source_columns = [
        '9MAX_BB Fold to SB Steal',
        '6MAX_BB Fold to SB Steal',
        'PKO_BB Fold to SB Steal'
    ]

    # Define new column names (will be placed after "BB Fold to BTN Steal")
    duplicate_names = [
        '9MAX_BB Fold to SB Steal (Dup)',
        '6MAX_BB Fold to SB Steal (Dup)',
        'PKO_BB Fold to SB Steal (Dup)'
    ]

    for i, source_col in enumerate(source_columns):
        try:
            # Find the index of the source column
            source_idx = headers.index(source_col) if source_col in headers else -1

            if source_idx >= 0:
                # Get the value from the source column
                source_value = data[source_idx]

                # Add to duplications
                duplications['headers'].append(duplicate_names[i])
                duplications['values'].append(source_value)

                app.logger.debug(f"Duplicated {source_col} as {duplicate_names[i]}: {source_value}")
            else:
                app.logger.warning(f"Source column not found: {source_col}")

        except (ValueError, IndexError) as e:
            app.logger.warning(f"Could not duplicate {source_col}: {e}")
            continue

    return duplications

def organize_csv_columns(headers, data):
    """
    Organize columns in the correct order, inserting duplicated BB Fold columns
    and calculated VPIP columns in their proper positions.
    """
    organized_headers = []
    organized_data = []

    # Process each file type separately to maintain order
    file_types = ['9max', '6max', 'pko', 'postflop']

    for file_type in file_types:
        file_prefix = file_type.upper()

        # Get all columns for this file type
        file_columns = [(i, h) for i, h in enumerate(headers) if h.startswith(file_prefix + '_')]

        for idx, header in file_columns:
            organized_headers.append(header)
            organized_data.append(data[idx])

            # After "BB Fold to BTN Steal", insert duplicated "BB Fold to SB Steal"
            if header.endswith('BB Fold to BTN Steal') and file_type != 'postflop':
                # Find the corresponding "BB Fold to SB Steal" column
                sb_steal_header = f"{file_prefix}_BB Fold to SB Steal"
                if sb_steal_header in headers:
                    sb_steal_idx = headers.index(sb_steal_header)
                    dup_header = f"{file_prefix}_BB Fold to SB Steal (Dup)"
                    organized_headers.append(dup_header)
                    organized_data.append(data[sb_steal_idx])
                    app.logger.debug(f"Inserted {dup_header} after {header}")

            # After certain columns, insert calculated VPIP columns
            vpip_mappings = {
                'EP Cold Call': 'EP VPIP',
                'MP Cold Call': 'MP VPIP', 
                'CO Cold Call': 'CO VPIP',
                'BTN Cold Call': 'BTN VPIP'
            }

            for col_suffix, vpip_suffix in vpip_mappings.items():
                if header.endswith(col_suffix) and file_type != 'postflop':
                    # Calculate VPIP value
                    threbet_header = header.replace('Cold Call', '3Bet')
                    coldcall_header = header

                    if threbet_header in headers:
                        threbet_idx = headers.index(threbet_header)
                        coldcall_idx = headers.index(coldcall_header)

                        try:
                            threbet_val = float(data[threbet_idx].replace(',', '.')) if data[threbet_idx] and data[threbet_idx] != 'NA' else 0.0
                            coldcall_val = float(data[coldcall_idx].replace(',', '.')) if data[coldcall_idx] and data[coldcall_idx] != 'NA' else 0.0

                            vpip_val = threbet_val + coldcall_val
                            vpip_formatted = f"{vpip_val:.2f}".replace('.', ',')

                            vpip_header = f"{file_prefix}_{vpip_suffix}"
                            organized_headers.append(vpip_header)
                            organized_data.append(vpip_formatted)

                            app.logger.debug(f"Calculated {vpip_header}: {threbet_val} + {coldcall_val} = {vpip_val}")
                        except (ValueError, IndexError) as e:
                            app.logger.warning(f"Could not calculate VPIP for {file_prefix}: {e}")

    return {
        'headers': organized_headers,
        'data': organized_data
    }

def merge_csv_files(files_dict):
    """
    Merges CSV files by taking the second row (line 2) from each file.
    Expected format: files_dict = {'9max': file, '6max': file, 'pko': file, 'postflop': file}
    Returns a dictionary with headers and data for web display.
    """
    order = ['9max', '6max', 'pko', 'postflop']
    combined_headers = []
    combined_data = []

    try:
        for file_type in order:
            if file_type not in files_dict:
                app.logger.error(f"Missing file type: {file_type}")
                continue

            file = files_dict[file_type]
            app.logger.debug(f"Processing CSV file: {file.filename}")

            # Read file content
            content = file.read().decode('utf-8')

            # Parse CSV
            csv_reader = csv.reader(io.StringIO(content))
            rows = list(csv_reader)

            if len(rows) < 2:
                app.logger.error(f"File {file.filename} doesn't have enough rows (need at least 2)")
                continue

            # Get headers (row 1) and data (row 2)
            headers = rows[0] if len(rows) > 0 else []
            data_row = rows[1] if len(rows) > 1 else []

            # Add prefix to headers to identify source
            prefixed_headers = [f"{file_type.upper()}_{header}" for header in headers]

            combined_headers.extend(prefixed_headers)
            combined_data.extend(data_row)

            app.logger.debug(f"Added {len(data_row)} columns from {file_type}")

        # Organize columns in correct order with VPIP calculations and duplications
        organized_headers = []
        organized_data = []

        # Process each file type separately to maintain order
        file_types = ['9max', '6max', 'pko', 'postflop']

        for file_type in file_types:
            file_prefix = file_type.upper()

            # Get all columns for this file type in original order
            file_columns = [(i, h) for i, h in enumerate(combined_headers) if h.startswith(file_prefix + '_')]

            # For PKO, reorganize to put Total Hands first, then UO VPIP stats
            if file_type == 'pko':
                # Separate columns by priority
                total_hands_column = []
                uo_vpip_columns = []
                other_columns = []

                for idx, header in file_columns:
                    if 'Total Hands' in header:
                        total_hands_column.append((idx, header))
                    elif any(uo_stat in header for uo_stat in ['Early UO VPIP', 'Middle UO VPIP', 'Cutoff UO VPIP', 'Button UO VPIP']):
                        uo_vpip_columns.append((idx, header))
                    else:
                        other_columns.append((idx, header))

                # Reorder: Total Hands first, then UO VPIP, then others
                file_columns = total_hands_column + uo_vpip_columns + other_columns
                app.logger.debug(f"Reordered PKO columns: Total Hands first, then UO VPIP stats")

            for idx, header in file_columns:
                # Skip certain columns based on file type
                should_skip = False

                if file_type in ['9max', '6max']:
                    # For 9max and 6max: exclude only specific UO VPIP stats (not SB UO VPIP)
                    if any(uo_stat in header for uo_stat in ['Early UO VPIP', 'Middle UO VPIP', 'Cutoff UO VPIP', 'Button UO VPIP']):
                        should_skip = True
                        app.logger.debug(f"Skipping {header} for {file_type} report")

                elif file_type == 'pko':
                    # For PKO: exclude specific RFI/Steal stats but keep all others
                    if any(rfi_stat in header for rfi_stat in ['Early RFI', 'Middle RFI', 'CO Steal', 'BTN Steal']) and not any(fold_stat in header for fold_stat in ['Fold to', 'Resteal']):
                        should_skip = True
                        app.logger.debug(f"Skipping {header} for PKO report")

                if should_skip:
                    continue

                # Process the value: multiply by 100 (except Total Hands, River Agg, W$WSF Rating) and format
                value = combined_data[idx]
                if ('Total Hands' not in header and 
                    'POSTFLOP_River Agg' not in header and 
                    'POSTFLOP_W$WSF Rating' not in header):
                    # Convert to percentage format (multiply by 100, round to 1 decimal)
                    try:
                        numeric_value = float(value.replace(',', '.')) if value and value != 'NA' else 0.0
                        percentage_value = numeric_value * 100
                        formatted_value = f"{percentage_value:.1f}".replace('.', ',')
                    except (ValueError, AttributeError):
                        formatted_value = value  # Keep original if conversion fails
                else:
                    formatted_value = value  # Keep Total Hands, River Agg, and W$WSF Rating as-is

                organized_headers.append(header)
                organized_data.append(formatted_value)

                # After "BB Fold to BTN Steal", insert duplicated "BB Fold to SB Steal"
                if header.endswith('BB Fold to BTN Steal') and file_type != 'postflop':
                    # Find the corresponding "BB Fold to SB Steal" column
                    sb_steal_header = f"{file_prefix}_BB Fold to SB Steal"
                    if sb_steal_header in combined_headers:
                        sb_steal_idx = combined_headers.index(sb_steal_header)
                        dup_header = f"{file_prefix}_BB Fold to SB Steal (Dup)"

                        # Format the duplicated value (multiply by 100, round to 1 decimal)
                        dup_value = combined_data[sb_steal_idx]
                        try:
                            numeric_value = float(dup_value.replace(',', '.')) if dup_value and dup_value != 'NA' else 0.0
                            percentage_value = numeric_value * 100
                            formatted_dup_value = f"{percentage_value:.1f}".replace('.', ',')
                        except (ValueError, AttributeError):
                            formatted_dup_value = dup_value  # Keep original if conversion fails

                        organized_headers.append(dup_header)
                        organized_data.append(formatted_dup_value)
                        app.logger.debug(f"Inserted {dup_header} after {header}")

                # After certain columns, insert calculated VPIP columns
                vpip_mappings = {
                    'EP Cold Call': 'EP VPIP',
                    'MP Cold Call': 'MP VPIP', 
                    'CO Cold Call': 'CO VPIP',
                    'BTN Cold Call': 'BTN VPIP'
                }

                for col_suffix, vpip_suffix in vpip_mappings.items():
                    if header.endswith(col_suffix) and file_type != 'postflop':
                        # Calculate VPIP value
                        threbet_header = header.replace('Cold Call', '3Bet')
                        coldcall_header = header

                        if threbet_header in combined_headers:
                            threbet_idx = combined_headers.index(threbet_header)
                            coldcall_idx = combined_headers.index(coldcall_header)

                            try:
                                threbet_val = float(combined_data[threbet_idx].replace(',', '.')) if combined_data[threbet_idx] and combined_data[threbet_idx] != 'NA' else 0.0
                                coldcall_val = float(combined_data[coldcall_idx].replace(',', '.')) if combined_data[coldcall_idx] and combined_data[coldcall_idx] != 'NA' else 0.0

                                vpip_val = threbet_val + coldcall_val
                                # Convert to percentage format (multiply by 100, round to 1 decimal)
                                vpip_percentage = vpip_val * 100
                                vpip_formatted = f"{vpip_percentage:.1f}".replace('.', ',')

                                vpip_header = f"{file_prefix}_{vpip_suffix}"
                                organized_headers.append(vpip_header)
                                organized_data.append(vpip_formatted)

                                app.logger.debug(f"Calculated {vpip_header}: {threbet_val} + {coldcall_val} = {vpip_val} ({vpip_percentage:.1f}%)")
                            except (ValueError, IndexError) as e:
                                app.logger.warning(f"Could not calculate VPIP for {file_prefix}: {e}")

        app.logger.info(f"Successfully merged CSV files. Result has {len(organized_headers)} columns (including calculated)")

        return {
            'headers': organized_headers,
            'data': organized_data
        }

    except Exception as e:
        app.logger.error(f"Error merging CSV files: {e}")
        raise ValueError(f'Erro ao combinar ficheiros CSV: {str(e)}')

def calculate_vpip_columns(headers, data):
    """
    Calculate VPIP columns from 3bet and cold call values.
    Returns dictionary with new headers and calculated values.
    """
    calculations = {
        'headers': [],
        'values': []
    }

    # Define position mappings for each file type
    position_mappings = [
        ('9MAX_EP 3Bet', '9MAX_EP Cold Call', '9MAX_EP VPIP'),
        ('9MAX_MP 3Bet', '9MAX_MP Cold Call', '9MAX_MP VPIP'),
        ('9MAX_CO 3Bet', '9MAX_CO Cold Call', '9MAX_CO VPIP'),
        ('9MAX_BTN 3Bet', '9MAX_BTN Cold Call', '9MAX_BTN VPIP'),
        ('6MAX_EP 3Bet', '6MAX_EP Cold Call', '6MAX_EP VPIP'),
        ('6MAX_MP 3Bet', '6MAX_MP Cold Call', '6MAX_MP VPIP'),
        ('6MAX_CO 3Bet', '6MAX_CO Cold Call', '6MAX_CO VPIP'),
        ('6MAX_BTN 3Bet', '6MAX_BTN Cold Call', '6MAX_BTN VPIP'),
        ('PKO_EP 3Bet', 'PKO_EP Cold Call', 'PKO_EP VPIP'),
        ('PKO_MP 3Bet', 'PKO_MP Cold Call', 'PKO_MP VPIP'),
        ('PKO_CO 3Bet', 'PKO_CO Cold Call', 'PKO_CO VPIP'),
        ('PKO_BTN 3Bet', 'PKO_BTN Cold Call', 'PKO_BTN VPIP'),
    ]

    for threbet_col, coldcall_col, vpip_col in position_mappings:
        try:
            # Find indices of the required columns
            threbet_idx = headers.index(threbet_col) if threbet_col in headers else -1
            coldcall_idx = headers.index(coldcall_col) if coldcall_col in headers else -1

            if threbet_idx >= 0 and coldcall_idx >= 0:
                # Get values and convert to float
                threbet_val = float(data[threbet_idx].replace(',', '.')) if data[threbet_idx] else 0.0
                coldcall_val = float(data[coldcall_idx].replace(',', '.')) if data[coldcall_idx] else 0.0

                # Calculate VPIP (sum of 3bet and cold call)
                vpip_val = threbet_val + coldcall_val

                # Round to 2 decimal places and format
                vpip_formatted = f"{vpip_val:.2f}".replace('.', ',')

                calculations['headers'].append(vpip_col)
                calculations['values'].append(vpip_formatted)

                app.logger.debug(f"Calculated {vpip_col}: {threbet_val} + {coldcall_val} = {vpip_val}")

        except (ValueError, IndexError) as e:
            app.logger.warning(f"Could not calculate {vpip_col}: {e}")
            continue

    return calculations

@app.route('/process-room-csv', methods=['POST'])
def process_room_csv():
    """Handle room CSV processing and return formatted JSON data for web display."""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum ficheiro enviado'})

        file = request.files['file']
        if not file or file.filename == '':
            return jsonify({'success': False, 'error': 'Ficheiro inválido'})

        app.logger.info(f"Processing room CSV file: {file.filename}")

        # Read file content
        content = file.read().decode('utf-8')

        # Parse CSV
        csv_reader = csv.reader(io.StringIO(content))
        rows = list(csv_reader)

        if len(rows) < 2:
            return jsonify({'success': False, 'error': 'Ficheiro CSV deve ter pelo menos 2 linhas (cabeçalho + dados)'})

        headers = rows[0]
        formatted_data = []

        # Find player column index
        player_col_idx = None
        for idx, header in enumerate(headers):
            if 'player' in header.lower() and 'site' in header.lower():
                player_col_idx = idx
                break

        # Process each data row
        for row_idx in range(1, len(rows)):
            row = rows[row_idx]

            # Skip rows where player column is empty (usually average/total rows)
            if player_col_idx is not None and (len(row) <= player_col_idx or not row[player_col_idx] or not row[player_col_idx].strip()):
                continue

            formatted_row = []

            for col_idx, value in enumerate(row):
                if col_idx < len(headers):
                    header = headers[col_idx]

                    # Skip formatting for Count and Total Hands columns
                    if 'Count' in header or 'Total Hands' in header:
                        formatted_row.append(value)
                    else:
                        # Format other values: multiply by 100, round to 1 decimal
                        try:
                            if value and value.strip() and value != 'NA':
                                numeric_value = float(value.replace(',', '.'))
                                percentage_value = numeric_value * 100
                                formatted_value = f"{percentage_value:.1f}".replace('.', ',')
                                formatted_row.append(formatted_value)
                            else:
                                formatted_row.append(value)
                        except (ValueError, AttributeError):
                            formatted_row.append(value)  # Keep original if conversion fails
                else:
                    formatted_row.append('')

            formatted_data.extend(formatted_row)

        app.logger.info(f"Successfully processed room CSV. Headers: {len(headers)}, Rows: {len(rows)-1}")

        return jsonify({
            'success': True,
            'headers': headers,
            'data': formatted_data
        })

    except Exception as e:
        app.logger.error(f"Error processing room CSV: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/parse', methods=['POST'])
def api_parse():
    """
    API endpoint for parsing classified files.

    Accepts:
    - classified_dir: Path to directory with PKO/, non-KO/, mystery/ folders
    - classified_zip_url: URL to download classified ZIP (not implemented)
    - aliases: Hero aliases configuration (optional)

    Returns:
    - Path to generated JSONL file
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No JSON data provided'}), 400

        # Get input directory
        classified_dir = data.get('classified_dir')
        classified_zip_url = data.get('classified_zip_url')

        if not classified_dir and not classified_zip_url:
            return jsonify({'success': False, 'error': 'Either classified_dir or classified_zip_url required'}), 400

        if classified_zip_url:
            return jsonify({'success': False, 'error': 'classified_zip_url not yet implemented'}), 501

        # Validate directory exists
        in_dir = Path(classified_dir)
        if not in_dir.exists():
            return jsonify({'success': False, 'error': f'Directory not found: {classified_dir}'}), 404

        # Create output directory
        run_id = uuid.uuid4().hex
        parsed_dir = Path('./parsed') / run_id
        parsed_dir.mkdir(parents=True, exist_ok=True)

        # Output file
        out_jsonl = parsed_dir / 'hands.jsonl'

        # Get aliases
        aliases = data.get('aliases', {})
        aliases_path = None

        # If aliases provided, save to temp file
        if aliases:
            aliases_path = parsed_dir / 'hero_aliases.json'
            with open(aliases_path, 'w') as f:
                json.dump(aliases, f)
        else:
            # Use default config
            aliases_path = Path('./app/config/hero_aliases.json')

        # Parse folder
        from app.parse.runner import parse_folder
        stats = parse_folder(
            in_root=str(in_dir),
            out_jsonl=str(out_jsonl),
            hero_aliases_path=str(aliases_path)
        )

        # Return success with stats
        return jsonify({
            'success': True,
            'run_id': run_id,
            'output_file': str(out_jsonl),
            'stats': stats
        })

    except Exception as e:
        app.logger.error(f"Error in /api/parse: {e}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/merge-csv', methods=['POST'])
def merge_csv():
    """Handle CSV file merging and return JSON data for web display."""
    try:
        # Check if all required files are present
        required_types = ['9max', '6max', 'pko', 'postflop']
        files_dict = {}

        for file_type in required_types:
            if file_type not in request.files:
                app.logger.error(f"Missing file type: {file_type}")
                abort(400, f'Ficheiro {file_type} em falta')

            file = request.files[file_type]
            if file.filename == '':
                app.logger.error(f"Empty filename for {file_type}")
                abort(400, f'Nome de ficheiro vazio para {file_type}')

            files_dict[file_type] = file

        app.logger.info("All CSV files received, starting merge process")

        # Merge the CSV files
        merged_data = merge_csv_files(files_dict)

        app.logger.info("CSV merge completed successfully")

        # Return JSON response for web display
        from flask import jsonify
        return jsonify({
            'success': True,
            'headers': merged_data['headers'],
            'data': merged_data['data']
        })

    except Exception as e:
        app.logger.error(f"Error in CSV merge endpoint: {e}")
        from flask import jsonify
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Note: CHUNKED_UPLOADS is defined at the top of the file

@app.route('/upload-chunk', methods=['POST'])
def upload_chunk():
    """Handle individual chunks for large file uploads."""
    try:
        upload_id = request.form.get('uploadId')
        chunk_index_str = request.form.get('chunkIndex')
        total_chunks_str = request.form.get('totalChunks')
        file_name = request.form.get('fileName')
        chunk = request.files.get('chunk')

        if not all([upload_id, chunk_index_str, total_chunks_str, file_name, chunk]):
            return jsonify({'success': False, 'error': 'Missing required parameters'}), 400

        try:
            chunk_index = int(chunk_index_str) if chunk_index_str else 0
            total_chunks = int(total_chunks_str) if total_chunks_str else 0
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': 'Invalid chunk parameters'}), 400

        # Initialize upload tracking
        if upload_id not in CHUNKED_UPLOADS:
            upload_dir = Path(tempfile.mkdtemp(prefix=f"chunked_{upload_id}_"))
            CHUNKED_UPLOADS[upload_id] = {
                'upload_dir': upload_dir,
                'file_name': file_name,
                'total_chunks': total_chunks,
                'received_chunks': {},
                'created_at': time.time()
            }
            # Save state after creating new upload
            save_upload_state()

        upload_info = CHUNKED_UPLOADS[upload_id]

        # Save chunk to disk with better error handling
        chunk_path = upload_info['upload_dir'] / f"chunk_{chunk_index:06d}"
        if chunk and hasattr(chunk, 'save'):
            try:
                chunk.save(str(chunk_path))
                upload_info['received_chunks'][chunk_index] = chunk_path

                # Verify chunk was saved correctly
                if not chunk_path.exists():
                    app.logger.error(f"Chunk file {chunk_path} was not created")
                    return jsonify({'success': False, 'error': 'Failed to save chunk file'}), 500

                chunk_size = chunk_path.stat().st_size
                app.logger.debug(f"Saved chunk {chunk_index + 1}/{total_chunks} for upload {upload_id} (size: {chunk_size} bytes)")

            except Exception as save_error:
                app.logger.error(f"Error saving chunk {chunk_index}: {save_error}")
                return jsonify({'success': False, 'error': f'Failed to save chunk: {str(save_error)}'}), 500
        else:
            app.logger.error(f"No chunk data received for upload {upload_id}, chunk {chunk_index}")
            return jsonify({'success': False, 'error': 'No chunk data received'}), 400

        return jsonify({
            'success': True,
            'chunkIndex': chunk_index,
            'totalChunks': total_chunks,
            'receivedChunks': len(upload_info['received_chunks'])
        })

    except Exception as e:
        app.logger.error(f"Error handling chunk: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/finalize-upload', methods=['POST'])
def finalize_upload():
    """Combine chunks and process the file."""
    try:
        data = request.get_json()
        upload_id = data.get('uploadId')
        file_name = data.get('fileName')
        total_size = data.get('totalSize')

        if upload_id not in CHUNKED_UPLOADS:
            app.logger.error(f"Upload {upload_id} not found in tracking")
            app.logger.debug(f"Available uploads: {list(CHUNKED_UPLOADS.keys())}")

            # Try to load state in case it was lost
            load_upload_state()

            if upload_id not in CHUNKED_UPLOADS:
                return jsonify({'success': False, 'error': 'Upload session expired. Please try uploading again.'}), 404

        upload_info = CHUNKED_UPLOADS[upload_id]

        # Check all chunks received
        if len(upload_info['received_chunks']) != upload_info['total_chunks']:
            return jsonify({
                'success': False, 
                'error': f"Missing chunks: received {len(upload_info['received_chunks'])}/{upload_info['total_chunks']}"
            }), 400

        # Combine chunks into final file
        final_file_path = upload_info['upload_dir'] / file_name
        app.logger.info(f"Combining {upload_info['total_chunks']} chunks into {final_file_path}")

        with open(final_file_path, 'wb') as final_file:
            for i in range(upload_info['total_chunks']):
                chunk_path = upload_info['received_chunks'][i]
                with open(chunk_path, 'rb') as chunk_file:
                    final_file.write(chunk_file.read())
                # Clean up chunk file
                chunk_path.unlink()

        app.logger.info(f"File reassembled: {final_file_path} ({final_file_path.stat().st_size} bytes)")

        # Now process the file using existing logic
        return process_uploaded_file(final_file_path, upload_id)

    except Exception as e:
        app.logger.error(f"Error finalizing upload: {e}")
        import traceback
        app.logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)}), 500

def process_uploaded_file(file_path, upload_id):
    """Process the uploaded file and return result."""
    try:
        if upload_id not in CHUNKED_UPLOADS:
            app.logger.error(f"Upload {upload_id} not found in tracking")
            return jsonify({'success': False, 'error': 'Upload not found'}), 404

        upload_info = CHUNKED_UPLOADS[upload_id]
        work_dir = upload_info['upload_dir'] / 'work'
        out_dir = upload_info['upload_dir'] / 'out'

        # Create directories with error handling
        try:
            work_dir.mkdir(exist_ok=True)
            out_dir.mkdir(exist_ok=True)
        except Exception as dir_error:
            app.logger.error(f"Failed to create directories: {dir_error}")
            return jsonify({'success': False, 'error': 'Failed to create work directories'}), 500

        # Validate file exists and is readable
        if not file_path.exists():
            app.logger.error(f"File not found: {file_path}")
            return jsonify({'success': False, 'error': 'File not found'}), 404

        file_size = file_path.stat().st_size
        app.logger.info(f"Processing file: {file_path} ({file_size} bytes)")

        # Extract the archive
        app.logger.info(f"Extracting archive: {file_path} ({file_size / (1024*1024):.2f} MB)")
        try:
            # For very large files, add timeout protection
            import signal

            def timeout_handler(signum, frame):
                raise TimeoutError("Archive extraction exceeded time limit")

            # Set timeout for extraction (10 minutes for large files)
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(600)  # 10 minutes

            try:
                success = unpack_any(file_path, work_dir)
                signal.alarm(0)  # Cancel alarm

                if not success:
                    app.logger.error("Archive extraction failed")
                    return jsonify({'success': False, 'error': 'Failed to extract archive - unsupported format or corrupted file'}), 400
            except TimeoutError:
                signal.alarm(0)  # Cancel alarm
                app.logger.error("Archive extraction timed out")
                return jsonify({'success': False, 'error': 'Archive extraction timed out - file may be too large or complex'}), 408

        except Exception as e:
            app.logger.error(f"Extraction error: {e}")
            import traceback
            app.logger.error(f"Extraction traceback: {traceback.format_exc()}")
            return jsonify({'success': False, 'error': f'Erro na extração: {str(e)}'}), 400

        # Check if any files were extracted
        extracted_files = list(work_dir.rglob('*'))
        if not extracted_files:
            app.logger.error("No files found after extraction")
            return jsonify({'success': False, 'error': 'No files found in archive'}), 400

        # Process TXT and XML files
        app.logger.info(f"Processing TXT and XML files from {len(extracted_files)} extracted files...")
        try:
            process_txt_tree(work_dir, out_dir)
        except Exception as process_error:
            app.logger.error(f"Error processing TXT/XML files: {process_error}")
            return jsonify({'success': False, 'error': f'Error processing files: {str(process_error)}'}), 500

        # Create result ZIP
        try:
            original_filename = Path(upload_info['file_name']).stem
            zip_filename = f"{original_filename}_separada.zip"
            zip_path = upload_info['upload_dir'] / zip_filename

            app.logger.info(f"Creating result ZIP: {zip_path}")
            files_added = 0

            # Use compression level 6 for better balance between speed and size
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
                for folder_path in [out_dir / "PKO", out_dir / "NON-KO", out_dir / "MYSTERIES"]:
                    if folder_path.exists():
                        folder_files = list(folder_path.rglob('*'))
                        app.logger.info(f"Adding {len(folder_files)} files from {folder_path.name}")

                        for file_path in folder_files:
                            if file_path.is_file():
                                try:
                                    arcname = str(file_path.relative_to(out_dir))
                                    zipf.write(file_path, arcname)
                                    files_added += 1

                                    # Log progress for large operations
                                    if files_added % 100 == 0:
                                        app.logger.info(f"Progress: {files_added} files added to ZIP")

                                except Exception as zip_error:
                                    app.logger.warning(f"Failed to add {file_path} to ZIP: {zip_error}")

            if files_added == 0:
                app.logger.error("No files were added to the result ZIP")
                return jsonify({'success': False, 'error': 'No files were processed'}), 400

            app.logger.info(f"Created ZIP with {files_added} files")

            # Store result path for download
            upload_info['result_path'] = zip_path
            upload_info['zip_filename'] = zip_filename

            # Save to SQLite database for persistence
            save_result_to_db(upload_id, str(zip_path), zip_filename)

            # Save state after processing completes
            save_upload_state()

            # Also backup to session for reliability
            session[f'result_{upload_id}'] = {
                'result_path': str(zip_path),
                'zip_filename': zip_filename
            }

            app.logger.info(f"Processing completed for upload {upload_id}")
            return jsonify({'success': True, 'uploadId': upload_id})

        except Exception as zip_error:
            app.logger.error(f"Error creating result ZIP: {zip_error}")
            return jsonify({'success': False, 'error': f'Error creating result: {str(zip_error)}'}), 500

    except Exception as e:
        app.logger.error(f"Error processing file: {e}")
        import traceback
        app.logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)})



@app.route('/download-result/<upload_id>')
def download_result(upload_id):
    """Download the processed result."""
    try:
        app.logger.info(f"Download request for upload ID: {upload_id}")

        # Try to reload state in case it was lost
        if upload_id not in CHUNKED_UPLOADS:
            app.logger.warning(f"Upload ID {upload_id} not in memory, trying to reload state...")
            load_upload_state()

        # Try database first if not found in memory
        if upload_id not in CHUNKED_UPLOADS:
            app.logger.info(f"Trying database for upload ID: {upload_id}")
            result_path_str, zip_filename = get_result_from_db(upload_id)
            if result_path_str and zip_filename:
                result_path = Path(result_path_str)
                if result_path.exists():
                    app.logger.info(f"Found result in database: {result_path}")
                    file_size = result_path.stat().st_size
                    app.logger.info(f"Preparing streaming download: {result_path} ({file_size} bytes)")

                    # Use centralized download function
                    app.logger.info(f"Using database result for download: {zip_filename}")
                    return create_download_response(result_path, zip_filename)
                else:
                    app.logger.error(f"Database result file not found: {result_path}")
                    # Clean up invalid database entry
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute('DELETE FROM results WHERE upload_id = ?', (upload_id,))
                        conn.commit()
                        conn.close()
                        app.logger.info(f"Cleaned up invalid database entry for {upload_id}")
                    except Exception as cleanup_error:
                        app.logger.error(f"Error cleaning up database: {cleanup_error}")

        # Try file-based tracking if not found in memory or database
        if upload_id not in CHUNKED_UPLOADS:
            result_tracking_file = Path(f'/tmp/result_{upload_id}.txt')
            if result_tracking_file.exists():
                try:
                    with open(result_tracking_file, 'r') as f:
                        lines = f.read().strip().split('\n')
                        if len(lines) >= 2:
                            result_path = Path(lines[0])
                            zip_filename = lines[1]

                            if result_path.exists():
                                app.logger.info(f"Found result via file tracking: {result_path}")
                                file_size = result_path.stat().st_size
                                app.logger.info(f"Sending file: {result_path} ({file_size} bytes)")

                                return create_download_response(result_path, zip_filename)
                            else:
                                app.logger.error(f"Tracked result file not found: {result_path}")
                except Exception as track_error:
                    app.logger.error(f"Error reading tracking file: {track_error}")

        # Try session backup if still not found
        if upload_id not in CHUNKED_UPLOADS:
            session_key = f'result_{upload_id}'
            if session_key in session:
                app.logger.info(f"Found upload {upload_id} in session backup")
                session_data = session[session_key]
                result_path = Path(session_data['result_path'])
                zip_filename = session_data['zip_filename']

                # Validate file still exists
                if result_path.exists():
                    app.logger.info(f"Session backup file exists: {result_path}")
                    file_size = result_path.stat().st_size

                    return create_download_response(result_path, zip_filename)
                else:
                    app.logger.error(f"Session backup file not found: {result_path}")

        if upload_id not in CHUNKED_UPLOADS:
            app.logger.error(f"Upload ID {upload_id} not found in CHUNKED_UPLOADS")
            app.logger.debug(f"Available upload IDs: {list(CHUNKED_UPLOADS.keys())}")

            # Debug: List all possible result files
            result_files = list(Path('/tmp').glob(f'result_{upload_id}.*'))
            app.logger.debug(f"Result files for {upload_id}: {result_files}")

            # Debug: List all tmp directories that might contain this upload
            tmp_dirs = list(Path('/tmp').glob(f'*{upload_id}*'))
            app.logger.debug(f"Tmp directories for {upload_id}: {tmp_dirs}")

            return jsonify({
                'success': False, 
                'error': 'Upload result expired or not found. Please upload your file again.',
                'message': 'The upload may have been processed before recent improvements. Try uploading again.'
            }), 404

        upload_info = CHUNKED_UPLOADS[upload_id]
        if 'result_path' not in upload_info:
            app.logger.error(f"Result path not found for upload ID: {upload_id}")
            abort(404, 'Result not ready')

        # Convert to Path object if needed
        if 'result_path' in upload_info:
            result_path = Path(upload_info['result_path']) if isinstance(upload_info['result_path'], str) else upload_info['result_path']
            zip_filename = upload_info.get('zip_filename', 'result.zip')
        else:
            app.logger.error(f"No result_path in upload_info for {upload_id}")
            return jsonify({'success': False, 'error': 'Result not ready'}), 404

        # Validate that the file exists and is accessible
        if not result_path.exists():
            app.logger.error(f"Result file not found: {result_path}")
            return jsonify({'success': False, 'error': 'Result file not found'}), 404

        file_size = result_path.stat().st_size
        app.logger.info(f"Sending file: {result_path} ({file_size} bytes)")

        # Use centralized download function for all files
        app.logger.info(f"Processing download from CHUNKED_UPLOADS: {zip_filename}")
        return create_download_response(result_path, zip_filename)

    except Exception as e:
        app.logger.error(f"Error in download_result: {e}")
        import traceback
        app.logger.error(f"Download traceback: {traceback.format_exc()}")
        return jsonify({'success': False, 'error': f'Download failed: {str(e)}'}), 500

@app.errorhandler(413)
def too_large(e):
    return jsonify({
        'success': False,
        'error': 'Ficheiro demasiado grande. Tente usar a funcionalidade de upload chunked.'
    }), 413

@app.errorhandler(400)
def bad_request(e):
    app.logger.error(f"Bad request: {e}")
    return jsonify({
        'success': False,
        'error': f'Erro na requisição: {e.description if hasattr(e, "description") else str(e)}'
    }), 400

@app.errorhandler(500)
def internal_error(e):
    app.logger.error(f"Internal server error: {e}")
    import traceback
    app.logger.error(f"Traceback: {traceback.format_exc()}")
    return jsonify({
        'success': False, 
        'error': 'Erro interno do servidor. Por favor, tente novamente.'
    }), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Catch all handler for deployment"""
    app.logger.error(f"Unhandled exception: {type(e).__name__}: {e}")
    import traceback
    app.logger.error(f"Full traceback: {traceback.format_exc()}")

    # Return JSON response
    return jsonify({
        'success': False,
        'error': 'Erro no processamento do ficheiro',
        'type': type(e).__name__
    }), 500

def init_app():
    """Initialize the application - called by both direct run and gunicorn."""
    try:
        # Initialize database on startup
        init_db()
        # Load any existing upload state
        load_upload_state()
        # Run database migrations
        check_and_run_migrations()
        # Ensure primary admin always exists (both dev and prod)
        ensure_primary_admin()
        # Initialize approved emails in production
        initialize_production_emails()
        
        # Initialize database connection pool (Task 7)
        DatabasePool.initialize()
        
        # No background worker needed - processing is now synchronous
        app.logger.info("Application initialized successfully (synchronous processing mode)")
    except Exception as e:
        app.logger.error(f"Error initializing application: {e}")
        raise

# Import deployment configuration
try:
    from deployment_config import configure_deployment
    configure_deployment(app)
except ImportError:
    pass  # deployment_config is optional

# Initialize when imported (for gunicorn)
init_app()

# ============================================================================
# PARTITION API ENDPOINTS
# ============================================================================

# POST /api/partition -> constrói partições
@app.route('/api/partition', methods=['POST'])
def api_partition():
    """Build partitions from enriched hands JSONL file."""
    data = request.get_json(force=True)
    in_jsonl = data.get("in_jsonl")
    out_dir = data.get("out_dir", "partitions")
    if not in_jsonl or not os.path.exists(in_jsonl):
        return {"error": "in_jsonl not found"}, 400
    result = build_partitions(in_jsonl, out_dir)

    # Auto-validate if requested
    if data.get("validate", False):
        from app.partition.validator import validate_with_summary
        validation = validate_with_summary(result["counts_path"], in_jsonl)
        result["validation"] = validation

    return {"ok": True, **result}


# GET /api/partition/counts?path=... -> devolve o JSON de counts
@app.route('/api/partition/counts', methods=['GET'])
def api_partition_counts():
    """Return the partition counts JSON file."""
    path = request.args.get('path')
    if not path or not os.path.exists(path):
        return {"error": "counts_path not found"}, 404
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


# GET /api/partition/debug?counts_path=... -> info útil p/ debugging
@app.route('/api/partition/debug', methods=['GET'])
def api_partition_debug():
    """Return debug information about partitions."""
    path = request.args.get('counts_path')
    if not path or not os.path.exists(path):
        return {"error": "counts_path not found"}, 404
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # estatísticas rápidas
    months = data.get("counts", {}) or {}
    totals = data.get("totals", {}) or {}
    top_months = sorted(
        months.items(),
        key=lambda kv: sum(g["hands"] for g in kv[1].values()),
        reverse=True
    )[:5]

    debug_info = {
        "total_months": len(months),
        "groups_active": list(totals.keys()),
        "top_months": [
            {"month": m, "hands": sum(g["hands"] for g in d.values())} for m, d in top_months
        ]
    }
    return {"data": data, "debug": debug_info}


# POST /api/partition/validate -> valida integridade das partições
@app.route('/api/partition/validate', methods=['POST'])
def api_partition_validate():
    """Validate partition integrity."""
    from app.partition.validator import validate_with_summary

    data = request.get_json(force=True)
    counts_path = data.get("counts_path")
    hands_jsonl = data.get("hands_jsonl")

    if not counts_path or not os.path.exists(counts_path):
        return {"error": "counts_path not found"}, 400
    if not hands_jsonl or not os.path.exists(hands_jsonl):
        return {"error": "hands_jsonl not found"}, 400

    result = validate_with_summary(counts_path, hands_jsonl)
    return result


@app.route('/api/debug/derived-postflop', methods=['GET'])
def debug_derived_postflop():
    """Conta quantas mãos têm flags pós-flop ativas (amostra)."""
    import json, os
    path = request.args.get('path', 'parsed/hands_enriched.jsonl')
    if not os.path.exists(path):
        return {"error":"hands_enriched.jsonl not found", "path": path}, 404

    keys = [
        "cbet_flop_opp_ip","cbet_flop_att_ip",
        "cbet_flop_opp_oop","cbet_flop_att_oop",
        "vs_cbet_flop_fold_ip","vs_cbet_flop_raise_ip",
        "vs_cbet_flop_fold_oop","vs_cbet_flop_raise_oop",
        "donk_flop","flop_bet_vs_missed_cbet_srp",
        "saw_flop","saw_showdown"
    ]
    counts = {k:0 for k in keys}
    total = 0
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            total += 1
            h = json.loads(line)
            post = (h.get("derived") or {}).get("postflop") or {}
            for k in keys:
                if post.get(k):
                    counts[k] += 1
    return {"total_hands": total, "flags_true": counts}


# ============================================================================
# STATS API ENDPOINTS
# ============================================================================

# POST /api/stats/build
@app.route('/api/stats/build', methods=['POST'])
def api_stats_build():
    """Build stats from parsed hands"""
    try:
        data = request.get_json() or {}
        input_file = data.get('input_file', 'parsed/hands_enriched.jsonl')
        dsl_file = data.get('dsl_file', 'app/stats/dsl/stats.yml')
        output_dir = data.get('output_dir', 'stats')

        if not os.path.exists(input_file):
            return jsonify({"error": f"Input file not found: {input_file}"}), 400

        if not os.path.exists(dsl_file):
            return jsonify({"error": f"DSL file not found: {dsl_file}"}), 400

        from app.stats.runner import run_stats
        result = run_stats(input_file, dsl_file, output_dir)

        return jsonify({
            "success": True,
            "result": result
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# GET /api/stats/summary?path=stats/stat_counts.json
@app.route('/api/stats/summary', methods=['GET'])
def api_stats_summary():
    """Return the stat counts JSON file."""
    path = request.args.get('path', 'stats/stat_counts.json')
    if not os.path.exists(path): 
        return jsonify({"error": "stat_counts.json not found"}), 404
    with open(path, 'r', encoding='utf-8') as f: 
        return jsonify(json.load(f))


# GET /api/stats/hands?month=YYYY-MM&group=...&stat=...&type=opps|attempts
@app.route('/api/stats/hands', methods=['GET'])
def api_stats_hands():
    """Return hand IDs for a specific stat/group/month."""
    token = request.args.get('token')
    
    if token:
        # Dashboard v2 mode - return sample hand IDs
        stat = request.args.get('stat')
        group = request.args.get('group')
        family = request.args.get('family')
        
        # For now, return sample hand IDs
        # In production, would read from token's index files
        sample_hands = [f"#{i:09d}" for i in range(100000000, 100000050, 7)]
        
        return jsonify({"ids": sample_hands})
    else:
        # Original mode
        base = request.args.get('index_dir', 'stats/index')
        month = request.args.get('month')
        group = request.args.get('group')
        stat  = request.args.get('stat')
        kind  = request.args.get('type', 'opps')
        if not all([month, group, stat]): 
            return jsonify({"error": "Missing params"}), 400
        if kind not in ("opps", "attempts"): 
            return jsonify({"error": "type must be opps|attempts"}), 400
        path = os.path.join(base, f"{month}__{group}__{stat}__{kind}.ids")
        if not os.path.exists(path): 
            return jsonify({"error": "index not found", "path": path}), 404
        with open(path, 'r', encoding='utf-8') as f: 
            ids = [ln.strip() for ln in f if ln.strip()]
        return jsonify({
            "month": month, 
            "group": group, 
            "stat": stat, 
            "type": kind, 
            "count": len(ids), 
            "hand_ids": ids
        })


# ============ PHASE 9.A: TIME SERIES AND BREAKDOWN ENDPOINTS ============

# GET /api/stats/timeseries?stat=...&group=...&months=12
@app.route('/api/stats/timeseries', methods=['GET'])
def api_stats_timeseries():
    """
    Get time series data for a specific stat over multiple months
    
    Query params:
        stat: Stat name (e.g., "POST_CBET_FLOP_IP")
        group: Group name (e.g., "postflop_all")
        months: Number of months to retrieve (default 12)
        time_decay: Apply time decay weights (true/false, default false)
    """
    try:
        from app.stats.timeseries import get_timeseries
        
        stat = request.args.get('stat')
        group = request.args.get('group')
        months = int(request.args.get('months', 12))
        apply_time_decay = request.args.get('time_decay', 'false').lower() == 'true'
        
        if not stat or not group:
            return jsonify({"error": "Missing required parameters: stat and group"}), 400
        
        result = get_timeseries(
            stat=stat,
            group=group,
            months=months,
            apply_time_decay=apply_time_decay
        )
        
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        app.logger.error(f"Timeseries API error: {e}")
        return jsonify({"error": str(e)}), 500


# GET /api/stats/breakdown?group=...&family=POSTFLOP_CBET
@app.route('/api/stats/breakdown', methods=['GET'])
def api_stats_breakdown():
    """
    Get breakdown of stats by position and street for a family
    
    Query params:
        group: Group name (e.g., "postflop_all")
        family: Family of stats (e.g., "POSTFLOP_CBET", "POSTFLOP_VS_CBET")
        month: Specific month to analyze (latest if not provided)
    """
    try:
        from app.stats.timeseries import get_breakdown
        
        group = request.args.get('group')
        family = request.args.get('family')
        month = request.args.get('month')
        
        if not group or not family:
            return jsonify({"error": "Missing required parameters: group and family"}), 400
        
        result = get_breakdown(
            group=group,
            family=family,
            month=month
        )
        
        return jsonify(result)
        
    except Exception as e:
        app.logger.error(f"Breakdown API error: {e}")
        return jsonify({"error": str(e)}), 500


# GET /api/stats/trend?stat=...&group=...&months=6
@app.route('/api/stats/trend', methods=['GET'])
def api_stats_trend():
    """
    Analyze trend for a specific stat over time
    
    Query params:
        stat: Stat name (e.g., "POST_CBET_FLOP_IP")
        group: Group name (e.g., "postflop_all")
        months: Number of months to analyze (default 6)
        token: Upload token (optional, for dashboard v2)
        family: Family name (optional, for dashboard v2)
    """
    try:
        token = request.args.get('token')
        
        if token:
            # Dashboard v2 mode - return simple trend data for chart
            stat = request.args.get('stat')
            group = request.args.get('group')
            family = request.args.get('family')
            
            # For now, return mock data for the chart
            # In production, would read from token's stat_counts.json
            import random
            labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
            values = [random.uniform(10, 30) for _ in range(6)]
            
            return jsonify({"labels": labels, "values": values})
        else:
            # Original mode
            from app.stats.timeseries import get_trend_analysis
            
            stat = request.args.get('stat')
            group = request.args.get('group')
            months = int(request.args.get('months', 6))
            
            if not stat or not group:
                return jsonify({"error": "Missing required parameters: stat and group"}), 400
            
            result = get_trend_analysis(
                stat=stat,
                group=group,
                months=months
            )
            
            return jsonify(result)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        app.logger.error(f"Trend API error: {e}")
        return jsonify({"error": str(e)}), 500


# ============ DERIVE ENDPOINT ============

@app.route('/api/derive/build', methods=['POST'])
def api_derive_build():
    try:
        data = request.get_json(force=True) if request.is_json else {}
        in_jsonl  = data.get("in_jsonl", "parsed/hands.jsonl")
        out_jsonl = data.get("out_jsonl", "parsed/hands_enriched.jsonl")
        force = bool(data.get("force", False))  # Aceitar parâmetro force

        from app.derive.runner import enrich_hands
        stats = enrich_hands(in_jsonl, out_jsonl, force=force)

        return jsonify({"success": True, "in_jsonl": in_jsonl, "out_jsonl": out_jsonl, "stats": stats})
    except Exception as e:
        app.logger.error(f"derive build failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ============ PHASE 6: SCORING ENDPOINTS ============

# POST /api/score/build
@app.route('/api/score/build', methods=['POST'])
def api_score_build():
    """Build scorecard with time-decay scoring"""
    from app.score.runner import build_scorecard
    data = request.get_json(force=True) if request.is_json else {}
    stat_counts = data.get("stat_counts", "stats/stat_counts.json")
    cfg_path = data.get("cfg_path", "app/score/config.yml")
    out_dir = data.get("out_dir", "scores")
    force = bool(data.get("force", False))

    try:
        res = build_scorecard(stat_counts, cfg_path, out_dir, force=force)
        return jsonify({"success": True, **res})
    except Exception as e:
        app.logger.error(f"Score build failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# GET /api/score/summary?path=scores/scorecard.json
@app.route('/api/score/summary', methods=['GET'])
def api_score_summary():
    """Devolve scorecard 'achatado' independentemente do formato em disco."""
    try:
        path = request.args.get('path', 'scores/scorecard.json')
        if not os.path.exists(path):
            return jsonify({"error": "scorecard.json not found", "path": path}), 404

        import json
        with open(path, 'r', encoding='utf-8') as f:
            raw = json.load(f)

        # Desembrulhar formatos comuns: {"data":{...}} | {"scorecard":{...}} | objeto direto
        sc = raw.get("data") if isinstance(raw, dict) and "data" in raw else \
             raw.get("scorecard") if isinstance(raw, dict) and "scorecard" in raw else \
             raw

        # Alguns ficheiros podem aninhar outra camada "summary"
        sc = sc.get("summary", sc) if isinstance(sc, dict) else sc

        # Garantir chaves principais: overall, group_level, stat_level (se existirem noutro nome)
        out = {}
        # mapeamentos prováveis
        out["overall"]      = sc.get("overall") if isinstance(sc, dict) else None
        out["group_level"]  = sc.get("group_level") if isinstance(sc, dict) else sc.get("groups") if isinstance(sc, dict) else {}
        out["stat_level"]   = sc.get("stat_level") if isinstance(sc, dict) else sc.get("stats") if isinstance(sc, dict) else {}

        # Devolver também o bruto para debugging (sem quebrar o cliente atual)
        out["_raw"] = sc
        return jsonify(out)

    except Exception as e:
        app.logger.error(f"Score summary failed: {e}")
        return jsonify({"error": str(e)}), 500

# GET /api/score/config
@app.route('/api/score/config', methods=['GET'])
def api_score_config_get():
    """Get scoring configuration"""
    from app.score.loader import load_config
    try:
        cfg = load_config()
        return jsonify(cfg)
    except Exception as e:
        app.logger.error(f"Failed to load config: {e}")
        return jsonify({"error": str(e)}), 500

# POST /api/score/config
@app.route('/api/score/config', methods=['POST'])
def api_score_config_post():
    """Update scoring configuration"""
    from app.score.loader import save_config
    data = request.get_json()
    try:
        save_config(data)
        return jsonify({"success": True})
    except Exception as e:
        app.logger.error(f"Failed to save config: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# GET /api/score/flat - Returns flat list with percentages and scores
@app.get("/api/score/flat")
def api_score_flat():
    """
    Retorna uma lista plana: [ { group, subgroup, stat, percentage, attempts, opportunities, score }, ... ]
    para renderização simples no dashboard.
    """
    import glob
    import json
    import os
    
    try:
        # Tenta descobrir último scorecard gerado
        candidates = sorted(glob.glob("/tmp/jobs/*/out/scores/scorecard.json"))
        if not candidates:
            # Fallback para outras localizações
            candidates = sorted(glob.glob("/tmp/*/scores/scorecard.json"))
        if not candidates:
            candidates = sorted(glob.glob("/tmp/uploads/*/out/scores/scorecard.json"))
        if not candidates:
            # Para teste - procurar em qualquer lugar
            candidates = sorted(glob.glob("/tmp/test_scorecard/scorecard.json"))
        
        if not candidates:
            return jsonify(ok=True, items=[])
        
        sc_path = candidates[-1]
        app.logger.info(f"Loading scorecard from: {sc_path}")
        
        with open(sc_path, "r", encoding="utf-8") as f:
            sc = json.load(f)
        
        items = []
        
        # Navegar pela estrutura real do scorecard
        # stat_level contém todos os stats com sua info completa
        stat_level = sc.get("stat_level", {})
        
        for stat_id, stat_data in stat_level.items():
            # stat_data tem: stat, group, subgroup, percentage, attempts, opportunities, score, etc.
            item = {
                "group": stat_data.get("group", ""),
                "subgroup": stat_data.get("subgroup", ""),
                "stat": stat_data.get("stat", stat_id),
                "percentage": stat_data.get("percentage", 0),
                "attempts": stat_data.get("attempts", 0),
                "opportunities": stat_data.get("opportunities", 0),
                "score": stat_data.get("score", 0),
                "note": stat_data.get("note", "")
            }
            items.append(item)
        
        # Se não há stat_level, tentar navegar via group_level
        if not items and "group_level" in sc:
            for group, gdata in sc.get("group_level", {}).items():
                for sub, sdata in gdata.get("subgroups", {}).items():
                    for stat, row in sdata.get("stats", {}).items():
                        items.append({
                            "group": group,
                            "subgroup": sub,
                            "stat": stat,
                            "percentage": row.get("percentage", 0),
                            "attempts": row.get("attempts", 0),
                            "opportunities": row.get("opportunities", 0),
                            "score": row.get("score", 0),
                            "note": row.get("note", "")
                        })
        
        return jsonify(ok=True, items=items)
        
    except Exception as e:
        app.logger.error(f"Score flat API error: {e}")
        return jsonify(ok=False, error=str(e), items=[])


# ============ INGEST API ============

@app.route('/api/ingest', methods=['POST'])
def api_ingest():
    """New ingest service for ZIP files with improved classification"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if not file or file.filename == '':
            return jsonify({'success': False, 'error': 'Empty file'}), 400
        
        # Check if it's a ZIP file
        if not file.filename or not file.filename.lower().endswith('.zip'):
            return jsonify({'success': False, 'error': 'Only ZIP files are supported'}), 400
        
        # Read file bytes
        file_bytes = file.read()
        
        # Use the new ingest service
        from app.upload.ingest import ingest_zip
        
        app.logger.info(f"Starting ingest for file: {file.filename}")
        manifest = ingest_zip(file_bytes)
        
        # Add additional processing flags
        enable_parser = request.form.get('enable_parser', 'false').lower() == 'true' if request.form else False
        dry_run = request.form.get('dry_run', 'false').lower() == 'true' if request.form else False
        
        # Store session data for pipeline
        if dry_run:
            session['last_dry_run'] = {
                'token': manifest['token'],
                'manifest': manifest,
                'original_name': Path(file.filename or 'uploaded').stem
            }
        
        app.logger.info(f"Ingest completed: {manifest['counts']}")
        
        return jsonify({
            'success': True,
            'manifest': manifest,
            'dry_run': dry_run,
            'enable_parser': enable_parser
        })
        
    except Exception as e:
        app.logger.error(f"Ingest failed: {e}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500

# ============ NEW UPLOAD AND PIPELINE ENDPOINTS ============

def _jobs_dir():
    base = os.environ.get("MTT_JOBS_DIR", os.path.join(tempfile.gettempdir(), "mtt_jobs"))
    os.makedirs(base, exist_ok=True)
    return base

def _new_job_id():
    return uuid.uuid4().hex[:16]  # 16 chars, simples p/ UI

@app.post("/upload_v2")
def upload_v2():
    f = request.files.get("file")
    if not f or not f.filename or not f.filename.lower().endswith(".zip"):
        return jsonify(ok=False, error="invalid_file", detail="Submete um .zip"), 400
    job_id = _new_job_id()
    job_dir = os.path.join(_jobs_dir(), job_id)
    os.makedirs(job_dir, exist_ok=True)
    zip_path = os.path.join(job_dir, "upload.zip")
    f.save(zip_path)
    try:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(os.path.join(job_dir, "inbox"))
    except Exception as e:
        return jsonify(ok=False, error="bad_zip", detail=str(e)), 400
    return jsonify(ok=True, job_id=job_id)

@app.post("/api/upload/zip")
def api_upload_zip():
    from app.upload.ingest import ingest_zip
    f = request.files.get("file")
    if not f:
        return jsonify({"error":"missing file"}), 400
    manifest = ingest_zip(f.read())
    return jsonify({"ok": True, "manifest": manifest})

# ============ PIPELINE RUN ENDPOINT ============

@app.route('/api/pipeline/run', methods=['POST'])
def api_pipeline_run_v2():
    """
    Run the full pipeline on an uploaded ZIP file
    Returns a token for accessing results
    """
    from app.pipeline.runner import run_full_pipeline
    import tempfile
    
    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({"ok": False, "error": "No file uploaded"}), 400
        
        file = request.files['file']
        
        # Check if file is a ZIP
        if not file.filename or not file.filename.lower().endswith('.zip'):
            return jsonify({"ok": False, "error": "File must be a ZIP archive"}), 400
        
        # Save uploaded file to temp location
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
            file.save(tmp_file.name)
            temp_zip_path = tmp_file.name
        
        try:
            # Run pipeline
            success, token, error_info = run_full_pipeline(temp_zip_path)
            
            if success:
                return jsonify({"ok": True, "token": token})
            else:
                return jsonify({
                    "ok": False, 
                    "token": token,
                    "step": error_info.get("step", "unknown") if error_info else "unknown",
                    "error": error_info.get("error", "Unknown error") if error_info else "Unknown error"
                }), 500
                
        finally:
            # Clean up temp file
            if os.path.exists(temp_zip_path):
                os.unlink(temp_zip_path)
                
    except Exception as e:
        app.logger.exception("Pipeline run error")
        return jsonify({"ok": False, "error": str(e)}), 500

# ============ FLAT DATA CONTRACT ENDPOINT ============

from app.api.flat import build_flat

@app.route('/api/flat/<token>')
def api_flat_contract(token):
    """Get flat data contract combining stats and scoring"""
    try:
        result = build_flat(token)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e), "token": token}), 500

# ============ DISTRIBUTED DOWNLOAD ENDPOINTS ============

@app.route('/api/download/result/<token>')
def api_download_result(token):
    """
    Download processed result ZIP (production-ready, works with autoscale)
    
    This endpoint:
    - Queries processing_jobs database for job info
    - Retrieves result ZIP from Object Storage (cloud) or local filesystem (dev)
    - Works across multiple autoscaled instances
    """
    try:
        # Validate token
        if not re.match(r'^[a-f0-9]{12}$', token):
            return jsonify({"error": "Invalid token"}), 400
        
        # Get job info from database
        from app.services.job_queue_service import JobQueueService
        job_queue = JobQueueService()
        job = job_queue.get_job(token)
        
        if not job:
            return jsonify({"error": "Job not found"}), 404
        
        if job['status'] != 'completed':
            return jsonify({"error": f"Job not ready (status: {job['status']})"}), 400
        
        # Get ZIP filename from result_data
        zip_filename = None
        if job['result_data'] and isinstance(job['result_data'], dict):
            zip_filename = job['result_data'].get('zip_filename')
        
        if not zip_filename:
            # Fallback: construct filename from token
            zip_filename = f"{token}_separada.zip"
        
        # Try to download from Object Storage
        from app.services.storage import get_storage
        storage = get_storage()
        storage_path = f"/results/{token}/{zip_filename}"
        
        file_data = storage.download_file(storage_path)
        
        if file_data:
            # File found in storage
            app.logger.info(f"Serving ZIP from storage: {zip_filename} ({len(file_data)} bytes)")
            return send_file(
                io.BytesIO(file_data),
                as_attachment=True,
                download_name=zip_filename,
                mimetype='application/zip'
            )
        
        # Fallback to local filesystem (for backwards compatibility)
        local_path = os.path.join("work", token, zip_filename)
        if os.path.exists(local_path):
            app.logger.info(f"Serving ZIP from local: {zip_filename}")
            return send_file(
                local_path,
                as_attachment=True,
                download_name=zip_filename,
                mimetype='application/zip'
            )
        
        return jsonify({"error": "Result file not found"}), 404
        
    except Exception as e:
        app.logger.error(f"Error downloading result: {e}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/api/job/download')
def api_job_download():
    """Download a file from a job directory"""
    job = request.args.get('job', '').strip()
    file_path = request.args.get('file', '').strip()
    
    # Validate job ID
    if not job or not re.match(r'^[a-zA-Z0-9_\-]{8,64}$', job):
        return jsonify({'error': 'invalid job id'}), 400
    
    # Validate file path (prevent directory traversal)
    if not file_path or '..' in file_path or file_path.startswith('/'):
        return jsonify({'error': 'invalid file path'}), 400
    
    # Build full path
    base = os.environ.get("MTT_JOBS_DIR", os.path.join(tempfile.gettempdir(), "mtt_jobs"))
    full_path = os.path.join(base, job, file_path)
    
    # Check if file exists and is within job directory
    if not os.path.exists(full_path) or not os.path.isfile(full_path):
        return jsonify({'error': 'file not found'}), 404
    
    if not os.path.abspath(full_path).startswith(os.path.abspath(os.path.join(base, job))):
        return jsonify({'error': 'access denied'}), 403
    
    return send_file(full_path, as_attachment=True)

# ============ ADMIN ENDPOINTS ============

@app.route('/api/admin/cleanup', methods=['POST'])
def admin_cleanup():
    """
    Manual cleanup trigger (admin only)
    
    Cleans up:
    - Old processing jobs (>7 days)
    - Old upload sessions and chunks (>7 days)
    - Expired sessions
    - Temporary files
    - Local work directories
    
    Returns:
        Cleanup statistics
    """
    try:
        # TODO: Add admin authentication check
        # For now, anyone can trigger (in production, add auth middleware)
        
        from app.services.cleanup_service import CleanupService
        
        # Get optional parameters
        data = request.get_json() if request.is_json else {}
        job_days = data.get('job_days', 7)
        session_days = data.get('session_days', 7)
        temp_days = data.get('temp_days', 7)
        work_days = data.get('work_days', 30)
        
        # Run cleanup
        stats = CleanupService.run_cleanup(
            job_days=job_days,
            session_days=session_days,
            temp_days=temp_days,
            work_days=work_days
        )
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        app.logger.error(f"Cleanup failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/admin/stats')
def admin_stats():
    """
    System statistics endpoint (admin only)
    
    Returns:
        Current system statistics from SQLite job queue
    """
    try:
        from app.services.job_queue_service import JobQueueService
        import sqlite3
        
        job_queue = JobQueueService()
        stats = {}
        
        # Job statistics by status
        conn = sqlite3.connect('/tmp/job_queue.db')
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT status, COUNT(*)
            FROM jobs
            GROUP BY status
        """)
        stats['jobs_by_status'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Recent activity (last 24h)
        cursor.execute("""
            SELECT COUNT(*)
            FROM jobs
            WHERE created_at > datetime('now', '-24 hours')
        """)
        stats['jobs_last_24h'] = cursor.fetchone()[0]
        
        # Total jobs
        cursor.execute("SELECT COUNT(*) FROM jobs")
        stats['total_jobs'] = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'stats': stats,
            'system': 'sqlite_queue'
        })
        
    except Exception as e:
        app.logger.error(f"Stats query failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============ PIPELINE REBUILD ENDPOINT ============

@app.route('/api/pipeline/rebuild', methods=['POST'])
def api_pipeline_rebuild():
    """
    Recorre do hands_enriched.jsonl → partitions → stats → score
    """
    try:
        try:
            data = request.get_json(force=True) if request.is_json else {}
        except:
            data = {}
        in_enriched = data.get("in_enriched", "parsed/hands_enriched.jsonl")

        # partitions
        from app.partition.runner import build_partitions
        part = build_partitions(in_enriched, "partitions")

        # stats
        from app.stats.engine import run_stats
        stats_res = run_stats(in_enriched, "app/stats/dsl/stats.yml", "stats")

        # score
        from app.score.runner import build_scorecard
        sc = build_scorecard("stats/stat_counts.json", "app/score/config.yml", "scores", force=True)

        return jsonify({"success": True, "partitions": part, "stats": stats_res, "score": sc})
    except Exception as e:
        app.logger.error(f"Pipeline rebuild failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# UI Playground
@app.route('/scoring')
def scoring_page():
    """Scoring playground UI"""
    return render_template('scoring.html')

@app.route('/debug-hands')
def debug_hands_page():
    """Debug interface for hands API"""
    return render_template('debug_hands.html')

@app.route('/validate')
def validate():
    return render_template('validate.html')

@app.route('/pipeline')
def pipeline_page():
    return render_template('pipeline.html')

@app.route('/dashboard')
def dashboard_page():
    """Phase 9.B - Dashboard UI with charts and filters"""
    return render_template('dashboard.html')

@app.route('/dashboard_v2')
def dashboard_v2_page():
    """Phase 10.C - Dashboard v2 with hierarchical UI"""
    # Pass token to template for "Voltar ao pipeline" buttons
    token = request.args.get("token", "")
    return render_template('dashboard_tabs.html', token=token)

@app.route('/ingest')
def ingest_page():
    """Ingest service test page"""
    return render_template('ingest.html')

@app.route('/test_mtt')
def test_mtt_page():
    """Test page for MTT import API"""
    return render_template('test_mtt.html')

@app.route('/import_mtt')
def import_mtt_page():
    """Página minimalista para importar MTT"""
    return render_template('import_mtt.html')

@app.route('/import')
def import_page():
    """Redireciona para o ponto de entrada único"""
    from flask import redirect
    return redirect('/import_mtt')

# ============ ROBUST PIPELINE ENDPOINT ============

JOBS_ROOT = "/tmp/jobs"
MAX_ZIP_BYTES = 200 * 1024 * 1024  # 200 MB para conteúdo expandido
ALLOWED_UPLOADS = {".zip"}

os.makedirs(JOBS_ROOT, exist_ok=True)

def _save_upload_zip(file_storage):
    job_id = uuid.uuid4().hex[:10]
    job_dir = os.path.join(JOBS_ROOT, job_id)
    os.makedirs(job_dir, exist_ok=True)
    fn = secure_filename(file_storage.filename or f"{job_id}.zip")
    zip_path = os.path.join(job_dir, fn)
    # grava em blocos para não rebentar memória
    with open(zip_path, "wb") as f:
        while True:
            chunk = file_storage.stream.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
    return job_id, job_dir, zip_path

def _extract_zip(zip_path, out_dir):
    with zipfile.ZipFile(zip_path) as zf:
        total = sum(i.file_size for i in zf.infolist())
        if total > MAX_ZIP_BYTES:
            raise ValueError(f"ZIP demasiado grande ({total} bytes). Limite {MAX_ZIP_BYTES}.")
        zf.extractall(out_dir)
    txts = []
    for root, _, files in os.walk(out_dir):
        for fn in files:
            if fn.lower().endswith(".txt"):
                txts.append(os.path.join(root, fn))
    if not txts:
        raise ValueError("O ZIP não contém .txt.")
    return txts

# ===== Classificador de torneios (PKO / Mystery / non-KO) =====
PKO_PATTERNS_ROBUST = [
    r"\bBounty\s*Hunters?\b",      # GG (ex.: "Bounty Hunters $32")
    r"\b(Bounty\s*Builder|Progressive\s*KO|PKO)\b",  # PokerStars PKO
    r"[€$£]\d+(?:\.\d+)?\s*\+\s*[€$£]\d+(?:\.\d+)?\s*\+",  # preço X+Y+fee (PS PKO)
]
MYSTERY_PATTERNS_ROBUST = [r"\bMystery\s*Bounty\b"]

def _detect_mode_from_text(text):
    # retorna "mystery" | "pko" | "nonko"
    t = text
    for p in MYSTERY_PATTERNS_ROBUST:
        if re.search(p, t, re.IGNORECASE):
            return "mystery"
    for p in PKO_PATTERNS_ROBUST:
        if re.search(p, t, re.IGNORECASE):
            return "pko"
    # Heurística para non-KO: preço apenas X+fee
    simple_buyin = re.search(r"[€$£]\d+(?:\.\d+)?\s*\+\s*[€$£]\d+(?:\.\d+)?\s*[A-Z]+", t)
    if simple_buyin and not re.search(PKO_PATTERNS_ROBUST[2], t):
        return "nonko"
    return "nonko"

def _classify_txts(txt_files):
    buckets = {"pko": [], "mystery": [], "nonko": []}
    for p in txt_files:
        # lê só o início para ser rápido
        try:
            with open(p, "rb") as f:
                head = f.read(8192).decode("utf-8", errors="ignore")
            mode = _detect_mode_from_text(head)
            buckets[mode].append(p)
        except Exception as e:
            app.logger.warning(f"Could not classify {p}: {e}")
            buckets["nonko"].append(p)  # Default to nonko if can't read
    return buckets

def _run_robust_pipeline(job_dir, buckets):
    """
    Integra com o orquestrador simplificado.
    """
    try:
        # 1) Cria estrutura de diretórios esperada
        raw_dir = Path(job_dir) / "raw"
        raw_dir.mkdir(exist_ok=True)
        
        # Organiza arquivos por tipo
        for mode, files in buckets.items():
            if files:
                mode_dir = raw_dir / mode.upper().replace("NONKO", "NON-KO")
                mode_dir.mkdir(exist_ok=True)
                for src in files:
                    dst = mode_dir / Path(src).name
                    shutil.copy2(src, dst)
        
        # 2) Cria manifest para o pipeline
        manifest = {
            "job_dir": job_dir,
            "inputs": {
                "pko": len(buckets.get("pko", [])),
                "mystery": len(buckets.get("mystery", [])),
                "nonko": len(buckets.get("nonko", [])),
                "total": sum(len(files) for files in buckets.values())
            }
        }
        manifest_path = Path(job_dir) / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        
        # 3) Executa pipeline com orquestrador simplificado
        from app.pipeline_orchestrator import build_all_safe
        out_dir = Path(job_dir) / "out"
        result = build_all_safe(str(manifest_path), str(out_dir))
        
        # 4) Adiciona token para compatibilidade com UI
        token = Path(job_dir).name
        if not isinstance(result, dict):
            result = {"error": "Invalid result from pipeline"}
        
        # Add token and job_id to the result dict
        result["token"] = token
        result["job_id"] = token
        
        # 5) Cria link simbólico para compatibilidade
        token_root = Path("/tmp/uploads") / token
        if token_root.exists() or token_root.is_symlink():
            if token_root.is_symlink():
                token_root.unlink()
            else:
                shutil.rmtree(token_root)
        token_root.symlink_to(job_dir)
        
        # 6) Adiciona classificação ao resultado
        result["classification"] = manifest["inputs"]
        
        return result
    except Exception as e:
        app.logger.error(f"Pipeline failed: {e}")
        app.logger.error(traceback.format_exc())
        raise

@app.post("/api/pipeline")
def api_pipeline():
    """Endpoint robusto para processar ZIP com hand histories"""
    try:
        if "file" not in request.files:
            return jsonify(ok=False, error="Faltou o ficheiro .zip no campo 'file'."), 400
        up = request.files["file"]
        ext = os.path.splitext(up.filename or "")[1].lower()
        if ext not in ALLOWED_UPLOADS:
            return jsonify(ok=False, error="Só é aceite .zip."), 400

        job_id, job_dir, zip_path = _save_upload_zip(up)
        in_dir = os.path.join(job_dir, "in")
        os.makedirs(in_dir, exist_ok=True)
        txt_files = _extract_zip(zip_path, in_dir)
        buckets = _classify_txts(txt_files)
        
        app.logger.info(f"Job {job_id}: found {len(txt_files)} txt files")
        app.logger.info(f"Classification: PKO={len(buckets['pko'])}, Mystery={len(buckets['mystery'])}, NON-KO={len(buckets['nonko'])}")

        result = _run_robust_pipeline(job_dir, buckets)
        return jsonify(ok=True, job_id=job_id, result=result)
    except Exception as e:
        # Nunca deixa 500 sem rasto:
        job_id = locals().get("job_id", "nojob")
        err_txt = traceback.format_exc()
        err_dir = os.path.join(JOBS_ROOT, str(job_id))
        os.makedirs(err_dir, exist_ok=True)
        with open(os.path.join(err_dir, "error.txt"), "w", encoding="utf-8") as f:
            f.write(err_txt)
        app.logger.error(f"Pipeline error for job {job_id}: {e}")
        app.logger.error(err_txt)
        return jsonify(ok=False, error=str(e), job_id=job_id), 500

# Serve static files from token directory
@app.get("/files/<token>/<path:rest>")
def serve_token_files(token, rest):
    from flask import send_file
    from pathlib import Path
    
    # Check both old and new session directories
    for base_dir in [Path("/tmp/uploads"), Path("/tmp/mtt_sessions")]:
        base = base_dir / token
        if base.exists():
            target = base / rest
            if str(target.resolve()).startswith(str(base.resolve())) and target.exists():
                return send_file(str(target))
    
    return "Not found", 404

# ============ STATS AND SCORE API ENDPOINTS ============

@app.route('/api/stats/flat')
def api_stats_flat():
    token = request.args.get("token", "current")
    try:
        data = build_flat(token)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        app.logger.exception("stats/flat error")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/api/pipeline/logs')
def api_pipeline_logs():
    """Get pipeline logs"""
    token = request.args.get('token')
    if not token:
        return jsonify({"ok": False, "error": "Token required"}), 400
    
    log_file = Path(f"work/{token}/_logs/steps.jsonl")
    if not log_file.exists():
        return jsonify({"ok": False, "error": "No logs found"}), 404
    
    try:
        logs = []
        with open(log_file, 'r') as f:
            for line in f:
                logs.append(json.loads(line))
        
        return jsonify({"ok": True, "logs": logs})
    except Exception as e:
        app.logger.error(f"Error reading logs: {str(e)}")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/api/stats/export.csv')
def api_stats_export_csv():
    """Export flat data as CSV"""
    from app.api.flat import build_flat
    import csv
    from io import StringIO
    
    token = request.args.get('token')
    if not token:
        return jsonify({"ok": False, "error": "Token required"}), 400
    
    try:
        # Get flat data
        flat_data = build_flat(token)
        if "error" in flat_data:
            return jsonify({"ok": False, "error": flat_data["error"]}), 404
        
        # Create CSV in memory
        output = StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            'Group', 'Group_Weight', 'Subgroup', 'Subgroup_Weight',
            'Stat', 'Percentage', 'Ideal_Min', 'Ideal_Max',
            'Attempts', 'Opportunities', 'Score', 'Stat_Weight',
            'Note', 'Month', 'Overall_Score', 'Total_Hands'
        ])
        
        # Extract summary data
        month_latest = flat_data.get('month_latest', '')
        overall_score = flat_data.get('overall_score', '')
        total_hands = flat_data.get('sample', {}).get('hands', 0)
        
        # Write data rows
        for group in flat_data.get('groups', []):
            group_label = group.get('label', '')
            group_weight = group.get('weight', 0)
            
            for subgroup in group.get('subgroups', []):
                subgroup_label = subgroup.get('label', '')
                subgroup_weight = subgroup.get('weight', 0)
                
                for row in subgroup.get('rows', []):
                    writer.writerow([
                        group_label,
                        group_weight,
                        subgroup_label,
                        subgroup_weight,
                        row.get('label', ''),
                        row.get('pct', ''),
                        row.get('ideal_min', ''),
                        row.get('ideal_max', ''),
                        row.get('att', 0),
                        row.get('opps', 0),
                        row.get('score', ''),
                        row.get('weight_stat', 0),
                        row.get('note', ''),
                        month_latest,
                        overall_score,
                        total_hands
                    ])
        
        # Get CSV content
        csv_content = output.getvalue()
        output.close()
        
        # Return as CSV file
        response = Response(csv_content, mimetype='text/csv')
        response.headers['Content-Disposition'] = f'attachment; filename=mtt_stats_{token}.csv'
        return response
        
    except Exception as e:
        app.logger.error(f"Error exporting CSV: {str(e)}")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/api/score/detailed')
def api_score_detailed():
    """
    Get detailed score summary with weights and notes per subgroup for dashboard v2.
    """
    session = request.args.get('session', '')
    
    # Get the scorecard file for the session
    score_path = None
    
    if session:
        mtt_path = Path(f'/tmp/mtt_sessions/{session}/out/scores/scorecard.json')
        if mtt_path.exists():
            score_path = mtt_path
        else:
            old_path = Path(f'/tmp/uploads/{session}/out/scores/scorecard.json')
            if old_path.exists():
                score_path = old_path
    
    if not score_path:
        return jsonify({"error": "Scorecard not found for session"}), 404
    
    try:
        with open(score_path, 'r') as f:
            scorecard = json.load(f)
        
        # Extract relevant score data
        summary = {
            'overall': scorecard.get('overall', 0),
            'groups': {}
        }
        
        groups = scorecard.get('groups', {})
        for group_name, group_data in groups.items():
            summary['groups'][group_name] = {
                'score': group_data.get('score', 0),
                'grade': group_data.get('grade', 'N/A'),
                'weight': group_data.get('weight', 0),
                'subgroups': {}
            }
            
            subgroups = group_data.get('subgroups', {})
            for subgroup_name, subgroup_data in subgroups.items():
                stats = subgroup_data.get('stats', {})
                
                summary['groups'][group_name]['subgroups'][subgroup_name] = {
                    'score': subgroup_data.get('score', 0),
                    'weight': subgroup_data.get('weight', 0),
                    'stats': {}
                }
                
                for stat_name, stat_data in stats.items():
                    summary['groups'][group_name]['subgroups'][subgroup_name]['stats'][stat_name] = {
                        'score': stat_data.get('score', 0),
                        'grade': stat_data.get('grade', 'N/A'),
                        'ideal': stat_data.get('ideal_range', 'N/A'),
                        'note': stat_data.get('note', '')
                    }
        
        return jsonify(summary)
        
    except Exception as e:
        app.logger.error(f"Error loading scorecard: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/dashboard/<token>', methods=['GET'])
def api_dashboard_token(token):
    """Get dashboard data for a specific token with proper response structure"""
    try:
        data = build_dashboard_payload(token)
        return jsonify({
            "ok": True,
            "data": data
        })
    except Exception as e:
        app.logger.error(f"Error fetching dashboard data: {e}")
        return jsonify({
            "ok": False,
            "error": str(e)
        })

@app.route('/api/dashboard/payload', methods=['GET'])
def api_dashboard_payload():
    """Get consolidated dashboard payload from runs or current directory"""
    token = request.args.get('token')
    out = build_dashboard_payload(token)
    return jsonify(out)

@app.route('/app')
def app_unificada():
    """Unified MTT analysis interface"""
    return render_template('app.html')

# ============ ONE-SHOT RUN ENDPOINT ============

@app.route('/api/run', methods=['POST'])
def api_run():
    """One-shot endpoint: ZIP → classify → pipeline → dashboard.json"""
    import uuid
    import shutil
    import zipfile
    import traceback
    from app.mtt_import.detectors import detect_network, detect_tourney_type, smart_read_text
    
    try:
        # Validate file
        f = request.files.get("file")
        if not f or not f.filename or not f.filename.lower().endswith(".zip"):
            return jsonify({"ok": False, "error": "Send a .zip file"}), 400
        
        # Generate token
        token = uuid.uuid4().hex[:16]
        
        # Create runs/<token> structure
        run_dir = Path(f"runs/{token}")
        raw_dir = run_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Save and extract ZIP
        zip_path = raw_dir / "source.zip"
        f.save(str(zip_path))
        
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(str(raw_dir))
        
        # Find TXT files
        txt_files = []
        for root, dirs, files in os.walk(str(raw_dir)):
            for file in files:
                if file and file.lower().endswith('.txt'):
                    txt_files.append(os.path.join(root, file))
        
        # Classify files (PKO / Mystery / NON-KO)
        def classify_file(path):
            """Classify a single TXT file"""
            text = smart_read_text(Path(path))[:5000]  # Read first 5000 chars for classification
            network = detect_network(text)
            tourney_type = detect_tourney_type(text)
            return tourney_type.lower() if tourney_type else "nonko"
        
        classified = {"pko": [], "mystery": [], "nonko": []}
        for txt_path in txt_files:
            category = classify_file(txt_path)
            if category not in classified:
                category = "nonko"
            classified[category].append(txt_path)
        
        # Classification summary
        classification_summary = {
            "files_total": len(txt_files),
            "files_pko": len(classified['pko']),
            "files_mystery": len(classified['mystery']),
            "files_nonko": len(classified['nonko']),
            "timestamp": datetime.now().isoformat()
        }
        
        app.logger.info(f"Run {token}: PKO={classification_summary['files_pko']}, Mystery={classification_summary['files_mystery']}, NON-KO={classification_summary['files_nonko']}")
        
        # Save classification manifest to runs/<token>/manifest.json
        run_manifest_path = run_dir / "manifest.json"
        with open(run_manifest_path, "w") as mf:
            json.dump({
                **classification_summary,
                "token": token,
                "source_zip": f.filename if hasattr(f, 'filename') else 'upload.zip',
                "categories": {cat: [os.path.basename(fp) for fp in files] for cat, files in classified.items()}
            }, mf, indent=2)
        
        # Setup job directory for pipeline
        job_dir = Path(f"/tmp/jobs/{token}")
        in_dir = job_dir / "in"
        out_dir = job_dir / "out"
        in_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy classified files to job structure
        for category, files in classified.items():
            if files:
                cat_dir = in_dir / category
                cat_dir.mkdir(exist_ok=True)
                for file_path in files:
                    shutil.copy2(file_path, str(cat_dir))
        
        # Create manifest
        manifest = {
            "inputs": {cat.upper(): [os.path.basename(f) for f in files] 
                      for cat, files in classified.items() if files},
            "outputs": {cat.upper(): [f"{cat}/{os.path.basename(f)}" for f in files] 
                       for cat, files in classified.items() if files},
            "timestamp": datetime.now().isoformat(),
            "job_dir": str(in_dir)
        }
        
        manifest_path = job_dir / "classification_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        
        # Execute pipeline (derive → partitions → stats → score)
        from app.pipeline_orchestrator import build_all_safe
        pipeline_result = build_all_safe(str(manifest_path), str(out_dir))
        
        # Generate dashboard.json
        dashboard_payload = build_dashboard_payload(token)
        dashboard_path = run_dir / "dashboard.json"
        with open(dashboard_path, "w") as f:
            json.dump(dashboard_payload, f, indent=2)
        
        app.logger.info(f"Run {token} complete: dashboard.json saved")
        
        return jsonify({
            "token": token,
            "ok": True,
            "files": len(txt_files),
            "categories": {k: len(v) for k, v in classified.items()}
        })
        
    except Exception as e:
        app.logger.error(f"Run failed: {str(e)}")
        app.logger.error(traceback.format_exc())
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500

# ============ NEW MTT IMPORT ENDPOINT ============

@app.post("/api/import/upload_mtt")
def upload_mtt():
    """Upload ZIP → pipeline → token → dashboard"""
    import uuid
    import shutil
    import zipfile
    import traceback
    
    UPLOAD_ROOT = "/tmp/mtt_uploads"
    
    def _safe_mkdir(p): 
        os.makedirs(p, exist_ok=True)
    
    try:
        f = request.files.get("file")
        if not f or not f.filename or not f.filename.lower().endswith(".zip"):
            return jsonify({"ok": False, "error": "Envie um .zip com .txt"}), 400

        token = uuid.uuid4().hex[:16]
        base = os.path.join(UPLOAD_ROOT, token)
        _safe_mkdir(base)
        zip_path = os.path.join(base, "source.zip")
        f.save(zip_path)

        # unzip
        extract = os.path.join(base, "unzipped")
        _safe_mkdir(extract)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(extract)

        # Usar nosso pipeline existente
        txt_files = []
        for root, dirs, files in os.walk(extract):
            for file in files:
                if file and file.lower().endswith('.txt'):
                    txt_files.append(os.path.join(root, file))
        
        # Classificar arquivos usando a função existente
        buckets = _classify_txts(txt_files)
        
        app.logger.info(f"MTT Upload {token}: PKO={len(buckets['pko'])}, Mystery={len(buckets['mystery'])}, NON-KO={len(buckets['nonko'])}")
        
        # Criar estrutura compatível com o pipeline
        job_dir = os.path.join("/tmp/jobs", token)
        _safe_mkdir(job_dir)
        in_dir = os.path.join(job_dir, "in")
        _safe_mkdir(in_dir)
        
        # Copiar arquivos classificados
        for cat, files in buckets.items():
            cat_dir = os.path.join(in_dir, cat)
            _safe_mkdir(cat_dir)
            for txt_path in files:
                shutil.copy2(txt_path, cat_dir)
        
        # Executar pipeline completo
        result = _run_robust_pipeline(job_dir, buckets)
        
        # Criar link simbólico para compatibilidade
        token_link = Path(UPLOAD_ROOT) / token / "out"
        token_link.parent.mkdir(parents=True, exist_ok=True)
        if token_link.exists() or token_link.is_symlink():
            if token_link.is_symlink():
                token_link.unlink()
            else:
                shutil.rmtree(token_link)
        token_link.symlink_to(Path(job_dir) / "out")
        
        # Guardar manifest
        manifest_path = os.path.join(base, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as out:
            json.dump(result, out, ensure_ascii=False, indent=2)

        return jsonify({"ok": True, "token": token, "result": result})
        
    except Exception as e:
        traceback.print_exc()
        app.logger.error(f"MTT Upload error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

# ============ NEW MTT IMPORT ENDPOINT ============

@app.post("/api/mtt/import")
def api_mtt_import():
    """
    Unified MTT import endpoint with safe unzip, network detection,
    tournament classification, and full pipeline execution.
    """
    try:
        # Step 1: Receive and validate file
        if 'file' not in request.files:
            return jsonify({"ok": False, "where": "upload", "error": "No file provided"}), 400
        
        file = request.files['file']
        if not file.filename:
            return jsonify({"ok": False, "where": "upload", "error": "No filename"}), 400
        
        if not file.filename.lower().endswith('.zip'):
            return jsonify({"ok": False, "where": "upload", "error": "Only ZIP files are accepted"}), 400
        
        # Step 2: Create session directory
        session_id = uuid.uuid4().hex
        session_dir = Path('/tmp/mtt_sessions') / session_id
        inbox_dir = session_dir / 'inbox'
        inbox_dir.mkdir(parents=True, exist_ok=True)
        
        # Save uploaded ZIP
        zip_path = session_dir / 'upload.zip'
        file.save(str(zip_path))
        
        app.logger.info(f"MTT Import session {session_id}: ZIP saved ({zip_path.stat().st_size} bytes)")
        
        # Step 3: Safe unzip
        from app.mtt_import.zip_utils import safe_unzip
        try:
            extracted_files = safe_unzip(zip_path, inbox_dir)
            app.logger.info(f"Extracted {len(extracted_files)} files")
        except ValueError as e:
            return jsonify({"ok": False, "where": "unzip", "error": str(e)}), 400
        
        # Step 4: Process text files with detection
        from app.mtt_import.detectors import detect_network, detect_tourney_type, smart_read_text
        
        classified = {
            "NON_KO": [],
            "PKO": [],
            "MYSTERY": []
        }
        
        by_network = {}
        
        # Create classification folders
        for bucket in classified.keys():
            (session_dir / bucket).mkdir(exist_ok=True)
        
        # Process each text file
        txt_files = [f for f in extracted_files if f.suffix.lower() == '.txt']
        app.logger.info(f"Processing {len(txt_files)} text files")
        
        for txt_file in txt_files:
            try:
                # Read with encoding detection
                content = smart_read_text(txt_file)
                
                # Detect network
                network = detect_network(content)
                by_network[network] = by_network.get(network, 0) + 1
                
                # Detect tournament type
                tourney_type = detect_tourney_type(content)
                
                # Move to classified folder
                target_dir = session_dir / tourney_type
                target_file = target_dir / txt_file.name
                
                # Write normalized content (UTF-8, LF)
                target_file.write_text(content.replace('\r\n', '\n'), encoding='utf-8')
                txt_file.unlink()  # Remove from inbox
                
                classified[tourney_type].append(str(target_file))
                
                app.logger.debug(f"File {txt_file.name}: {network} / {tourney_type}")
                
            except Exception as e:
                app.logger.error(f"Error processing {txt_file.name}: {e}")
                continue
        
        # Step 5: Create manifest
        manifest = {
            "session": session_id,
            "files": {k: len(v) for k, v in classified.items()},
            "by_network": by_network,
            "total_files": sum(len(v) for v in classified.values()),
            "classified": classified,
            "timestamp": datetime.now().isoformat()
        }
        
        manifest_path = session_dir / 'manifest.json'
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
        
        app.logger.info(f"Manifest created: {manifest['files']}")
        
        # Step 6: Run pipeline (parse → derive → partitions → stats → score)
        try:
            out_dir = session_dir / 'out'
            out_dir.mkdir(exist_ok=True)
            parsed_dir = out_dir / 'parsed'
            parsed_dir.mkdir(exist_ok=True)
            
            # 6.1: Parse hands
            app.logger.info("Running parser...")
            from app.parse.runner import parse_folder
            
            parse_stats = parse_folder(
                in_root=str(session_dir),
                out_jsonl=str(parsed_dir / 'hands.jsonl'),
                hero_aliases_path='app/config/hero_aliases.json'
            )
            app.logger.info(f"Parsed {parse_stats['hands']} hands")
            
            # 6.2: Derive enriched data
            app.logger.info("Running derive...")
            from app.derive.runner import enrich_hands
            
            derive_stats = enrich_hands(
                str(parsed_dir / 'hands.jsonl'),
                str(parsed_dir / 'hands_enriched.jsonl'),
                force=True
            )
            app.logger.info(f"Enriched {derive_stats['hands']} hands")
            
            # 6.3: Build partitions
            app.logger.info("Running partitions...")
            partition_stats = build_partitions(
                str(parsed_dir / 'hands_enriched.jsonl'),
                str(out_dir / 'partitions')
            )
            
            # 6.4: Build statistics
            app.logger.info("Running stats...")
            stats_result = run_stats(
                str(parsed_dir / 'hands_enriched.jsonl'),
                'app/stats/dsl/stats.yml',
                str(out_dir / 'stats')
            )
            
            # 6.5: Calculate scores
            app.logger.info("Running scoring...")
            from app.score.runner import build_scorecard
            
            score_result = build_scorecard(
                str(out_dir / 'stats' / 'stat_counts.json'),
                'app/score/config.yml',
                str(out_dir / 'scores'),
                force=True
            )
            
            # Step 7: Read scorecard for summary
            scorecard_path = out_dir / 'scores' / 'scorecard.json'
            if scorecard_path.exists():
                scorecard = json.loads(scorecard_path.read_text())
                overall_score = scorecard.get('overall', 0)
                groups = scorecard.get('groups', {})
                
                # Get sample stats
                sample_stats = {}
                for group_name, group_data in list(groups.items())[:3]:  # First 3 groups
                    sample_stats[group_name] = {
                        'score': group_data.get('score', 0),
                        'grade': group_data.get('grade', 'N/A'),
                        'weight': group_data.get('weight', 0)
                    }
            else:
                overall_score = 0
                sample_stats = {}
            
            # Step 8: Prepare response
            response = {
                "ok": True,
                "session": session_id,
                "summary": {
                    "overall": round(overall_score, 2),
                    "hands_parsed": parse_stats['hands'],
                    "files_processed": manifest['total_files'],
                    "sample_groups": sample_stats
                },
                "links": {
                    "dashboard": f"/dashboard_v2?token={session_id}",
                    "score_json": f"/files/{session_id}/out/scores/scorecard.json",
                    "stats_json": f"/files/{session_id}/out/stats/stat_counts.json",
                    "enriched_jsonl": f"/files/{session_id}/out/parsed/hands_enriched.jsonl"
                },
                "manifest": manifest
            }
            
            app.logger.info(f"MTT Import completed successfully for session {session_id}")
            return jsonify(response)
            
        except Exception as e:
            app.logger.error(f"Pipeline error: {e}")
            import traceback
            app.logger.error(traceback.format_exc())
            return jsonify({
                "ok": False, 
                "where": "pipeline", 
                "error": str(e),
                "session": session_id,
                "manifest": manifest
            }), 500
            
    except Exception as e:
        app.logger.error(f"MTT Import error: {e}")
        import traceback
        app.logger.error(traceback.format_exc())
        return jsonify({
            "ok": False,
            "where": "general",
            "error": str(e)
        }), 500


@app.route('/api/download/hands_by_stat/<token>/<stat_filename>')
def download_hands_by_stat(token, stat_filename):
    """Download hands for a specific stat (old format - root directory)."""
    try:
        # Validate token
        if not re.match(r'^[a-f0-9]{12}$', token):
            return jsonify({"error": "Invalid token"}), 400
        
        # Security check for filename
        stat_filename = secure_filename(stat_filename)
        if not stat_filename.endswith('.txt'):
            return jsonify({"error": "Invalid file type"}), 400
        
        # Try Object Storage first (production), fallback to local (dev)
        from app.services.storage import get_storage
        storage = get_storage()
        storage_path = f"/results/{token}/hands_by_stat/{stat_filename}"
        
        # Try downloading from storage
        file_data = storage.download_file(storage_path)
        
        if file_data:
            # File found in cloud storage
            return send_file(
                io.BytesIO(file_data),
                as_attachment=True,
                download_name=stat_filename,
                mimetype='text/plain'
            )
        
        # Fallback to local filesystem (for backwards compatibility)
        file_path = os.path.join("work", token, "hands_by_stat", stat_filename)
        
        if os.path.exists(file_path):
            return send_file(
                file_path,
                as_attachment=True,
                download_name=stat_filename,
                mimetype='text/plain'
            )
        
        return jsonify({"error": f"File not found: {stat_filename}"}), 404
        
    except Exception as e:
        app.logger.error(f"Error downloading hands by stat: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/download/hands_by_stat/<token>/<format_name>/<stat_filename>')
def download_hands_by_stat_with_format(token, format_name, stat_filename):
    """Download hands for a specific stat with format subdirectory."""
    try:
        # Validate token
        if not re.match(r'^[a-f0-9]{12}$', token):
            return jsonify({"error": "Invalid token"}), 400
        
        # Validate format name
        if format_name not in ["nonko_9max", "nonko_6max", "pko"]:
            return jsonify({"error": "Invalid format"}), 400
        
        # Security check for filename
        stat_filename = secure_filename(stat_filename)
        if not stat_filename.endswith('.txt'):
            return jsonify({"error": "Invalid file type"}), 400
        
        # Get optional month parameter for monthly downloads
        month = request.args.get('month')
        
        # Try Object Storage first (production), fallback to local (dev)
        from app.services.storage import get_storage
        storage = get_storage()
        
        # Build storage paths (month-specific first, then aggregate fallback)
        storage_paths = []
        if month:
            # Validate month format (YYYY-MM)
            if not re.match(r'^\d{4}-\d{2}$', month):
                return jsonify({"error": "Invalid month format"}), 400
            storage_paths.append(f"/results/{token}/months/{month}/hands_by_stat/{format_name}/{stat_filename}")
        storage_paths.append(f"/results/{token}/hands_by_stat/{format_name}/{stat_filename}")

        file_data = None
        for storage_path in storage_paths:
            try:
                file_data = storage.download_file(storage_path)
            except Exception as download_error:
                app.logger.debug(f"Download failed for {storage_path}: {download_error}")
                file_data = None
            if file_data:
                break

        if file_data:
            # File found in cloud storage
            return send_file(
                io.BytesIO(file_data),
                as_attachment=True,
                download_name=f"{format_name}_{stat_filename}",
                mimetype='text/plain'
            )
        
        # Fallback to local filesystem (for backwards compatibility)
        local_paths = []
        if month:
            local_paths.append(os.path.join("work", token, "months", month, "hands_by_stat", format_name, stat_filename))
            local_paths.append(os.path.join("work", token, "months", month, "hands_by_stat", format_name, "hands_by_stat", stat_filename))
        local_paths.append(os.path.join("work", token, "hands_by_stat", format_name, stat_filename))
        local_paths.append(os.path.join("work", token, "hands_by_stat", format_name, "hands_by_stat", stat_filename))

        file_path = next((path for path in local_paths if os.path.exists(path)), None)

        if file_path and os.path.exists(file_path):
            return send_file(
                file_path,
                as_attachment=True,
                download_name=f"{format_name}_{stat_filename}",
                mimetype='text/plain'
            )
        
        return jsonify({"error": f"File not found: {format_name}/{stat_filename}"}), 404
        
    except Exception as e:
        app.logger.error(f"Error downloading hands by stat with format: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/hands_by_stat/<token>')
def list_hands_by_stat(token):
    """List available stat-specific hand files organized by format."""
    app.logger.info(f"🔍 list_hands_by_stat called for token: {token}")
    try:
        # Validate token
        if not re.match(r'^[a-f0-9]{12}$', token):
            app.logger.info(f"Invalid token format: {token}")
            return jsonify({"error": "Invalid token"}), 400
        
        # Get optional month parameter for monthly file listings
        month = request.args.get('month')
        if month:
            # Validate month format (YYYY-MM)
            if not re.match(r'^\d{4}-\d{2}$', month):
                return jsonify({"error": "Invalid month format"}), 400
            app.logger.info(f"📅 Listing hands_by_stat for month: {month}")
        
        from app.services.storage import get_storage
        from app.services.result_storage import get_result_storage
        from app.stats.hand_collector import HandCollector

        storage = get_storage()
        result_storage = get_result_storage()

        month_not_found = False
        using_month_data = False

        pipeline_data = None
        if month:
            try:
                pipeline_data = result_storage.get_pipeline_result(token, month=month)
                if pipeline_data:
                    using_month_data = True
            except FileNotFoundError:
                month_not_found = True
            except Exception as load_error:
                app.logger.warning(f"Failed to load monthly pipeline data: {load_error}")

        if not pipeline_data:
            pipeline_data = result_storage.get_pipeline_result(token)

        if not pipeline_data or 'combined' not in pipeline_data:
            return jsonify({
                "token": token,
                "formats": {},
                "requested_month": month,
                "selected_month": month if using_month_data else None,
                "month_not_found": month_not_found,
                "cloud_storage": storage.use_cloud,
            })

        base_storage_prefix = f"/results/{token}/hands_by_stat"
        base_local_dir = os.path.join("work", token, "hands_by_stat")
        if using_month_data and month:
            base_storage_prefix = f"/results/{token}/months/{month}/hands_by_stat"
            base_local_dir = os.path.join("work", token, "months", month, "hands_by_stat")

        def load_metadata(group_key: str) -> dict:
            metadata = {}
            storage_path = f"{base_storage_prefix}/{group_key}/metadata.json"
            try:
                data = storage.download_file(storage_path)
                if data:
                    metadata = json.loads(data.decode('utf-8'))
            except Exception as metadata_error:
                app.logger.debug(f"Metadata download failed for %s: %s", storage_path, metadata_error)

            if not metadata:
                local_path = os.path.join(base_local_dir, group_key, "metadata.json")
                if os.path.exists(local_path):
                    try:
                        with open(local_path, 'r', encoding='utf-8') as meta_file:
                            metadata = json.load(meta_file)
                    except Exception as local_error:
                        app.logger.debug(f"Metadata read failed for %s: %s", local_path, local_error)

            return metadata or {}

        formats_data = {}
        stat_filename_map = HandCollector.stat_filenames

        for group_key in ["nonko_9max", "nonko_6max", "pko"]:
            group_stats = pipeline_data.get('combined', {}).get(group_key)
            if not isinstance(group_stats, dict):
                continue

            metadata = load_metadata(group_key)
            hands_per_stat = metadata.get('hands_per_stat', {})
            descriptions = metadata.get('stat_descriptions', {})
            hand_ids_map = metadata.get('hand_ids', {})

            stats_entries = []
            for stat_name, stat_info in (group_stats.get('stats') or {}).items():
                filename = stat_filename_map.get(stat_name)
                if not filename:
                    continue

                opportunities = stat_info.get('opportunities', 0)
                attempts = stat_info.get('attempts', 0)
                hand_count = hands_per_stat.get(stat_name, 0)

                if hand_count <= 0 and opportunities <= 0 and attempts <= 0:
                    continue

                download_url = f"/api/download/hands_by_stat/{token}/{group_key}/{filename}"
                if using_month_data and month:
                    download_url += f"?month={month}"

                stats_entries.append({
                    "filename": filename,
                    "stat_name": stat_name,
                    "description": descriptions.get(stat_name, ''),
                    "hand_count": hand_count if hand_count > 0 else attempts,
                    "opportunities": opportunities,
                    "attempts": attempts,
                    "download_url": download_url,
                    "hands": hand_ids_map.get(stat_name, []),
                })

            if stats_entries:
                stats_entries.sort(key=lambda x: x['stat_name'])
                formats_data[group_key] = {
                    "display_name": group_key.replace("nonko_", "").replace("_", "-").upper(),
                    "stats": stats_entries,
                    "total_stats": len(stats_entries)
                }

        response_payload = {
            "token": token,
            "formats": formats_data,
            "cloud_storage": storage.use_cloud,
            "requested_month": month,
            "selected_month": month if using_month_data else None,
            "month_scope": "monthly" if using_month_data else "aggregate",
            "month_not_found": month_not_found,
        }

        return jsonify(response_payload)
        
    except Exception as e:
        app.logger.error(f"Error listing hands by stat: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)