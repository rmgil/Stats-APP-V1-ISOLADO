import sqlite3
import threading
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
import secrets


class JobQueueService:
    def __init__(self, db_path: str = '/tmp/job_queue.db'):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()
    
    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS job_queue (
                token TEXT PRIMARY KEY,
                user_email TEXT NOT NULL,
                filename TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER DEFAULT 0,
                message TEXT,
                payload_path TEXT NOT NULL,
                result_data TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON job_queue(status, created_at)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_user_status ON job_queue(user_email, status)')
        conn.commit()
        conn.close()
    
    def create_job(self, user_email: str, filename: str, payload_path: str, token: Optional[str] = None) -> str:
        if token is None:
            token = secrets.token_hex(6)
        now = datetime.utcnow().isoformat()
        
        conn = self._get_connection()
        conn.execute('''
            INSERT INTO job_queue (token, user_email, filename, status, progress, message, payload_path, created_at, updated_at)
            VALUES (?, ?, ?, 'pending', 0, 'Aguardando processamento...', ?, ?, ?)
        ''', (token, user_email, filename, payload_path, now, now))
        conn.commit()
        
        return token
    
    def claim_next_job(self) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        now = datetime.utcnow().isoformat()
        
        cursor = conn.execute('''
            UPDATE job_queue
            SET status = 'processing',
                updated_at = ?,
                message = 'A processar...'
            WHERE token = (
                SELECT token FROM job_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
            )
            RETURNING *
        ''', (now,))
        
        row = cursor.fetchone()
        conn.commit()
        
        if row:
            return dict(row)
        return None
    
    def update_progress(self, token: str, progress: int, message: str):
        conn = self._get_connection()
        now = datetime.utcnow().isoformat()
        
        conn.execute('''
            UPDATE job_queue
            SET progress = ?,
                message = ?,
                updated_at = ?
            WHERE token = ?
        ''', (progress, message, now, token))
        conn.commit()
    
    def mark_completed(self, token: str, result_data: str):
        conn = self._get_connection()
        now = datetime.utcnow().isoformat()
        
        conn.execute('''
            UPDATE job_queue
            SET status = 'completed',
                progress = 100,
                message = 'Processamento concluído',
                result_data = ?,
                updated_at = ?,
                completed_at = ?
            WHERE token = ?
        ''', (result_data, now, now, token))
        conn.commit()
    
    def mark_failed(self, token: str, error_message: str):
        conn = self._get_connection()
        now = datetime.utcnow().isoformat()
        
        conn.execute('''
            UPDATE job_queue
            SET status = 'failed',
                message = ?,
                updated_at = ?,
                completed_at = ?
            WHERE token = ?
        ''', (error_message, now, now, token))
        conn.commit()
    
    def get_job(self, token: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        cursor = conn.execute('SELECT * FROM job_queue WHERE token = ?', (token,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None
    
    def count_active_jobs_by_user(self, user_email: str) -> int:
        conn = self._get_connection()
        cursor = conn.execute('''
            SELECT COUNT(*) as count
            FROM job_queue
            WHERE user_email = ?
            AND status IN ('pending', 'processing')
        ''', (user_email,))
        
        result = cursor.fetchone()
        return result['count'] if result else 0
    
    def count_pending_jobs(self) -> int:
        conn = self._get_connection()
        cursor = conn.execute("SELECT COUNT(*) as count FROM job_queue WHERE status = 'pending'")
        result = cursor.fetchone()
        return result['count'] if result else 0
    
    def get_queue_position(self, token: str) -> Optional[int]:
        conn = self._get_connection()
        cursor = conn.execute('''
            SELECT COUNT(*) as position
            FROM job_queue
            WHERE status = 'pending'
            AND created_at < (SELECT created_at FROM job_queue WHERE token = ?)
        ''', (token,))
        
        result = cursor.fetchone()
        return result['position'] + 1 if result else None
    
    def cleanup_old_jobs(self, days: int = 7):
        conn = self._get_connection()
        cutoff = datetime.utcnow().isoformat()
        
        conn.execute('''
            DELETE FROM job_queue
            WHERE completed_at IS NOT NULL
            AND datetime(completed_at) < datetime(?, '-' || ? || ' days')
        ''', (cutoff, days))
        conn.commit()
