"""Job service backed by PostgreSQL."""

import logging
import secrets
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.services.db_pool import DatabasePool

logger = logging.getLogger(__name__)


class JobService:
    """Manage background jobs stored in PostgreSQL."""

    def create_job(
        self,
        *,
        user_id: str,
        upload_id: str,
        input_path: str,
    ) -> Optional[str]:
        job_id = secrets.token_hex(6)
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO jobs (id, user_id, upload_id, status, progress, created_at, input_path)
                    VALUES (%s, %s, %s, 'pending', 0, NOW(), %s)
                    RETURNING id
                    """,
                    (job_id, user_id, upload_id, input_path),
                )
                conn.commit()
                return cur.fetchone()[0]
        except Exception as exc:
            logger.error("Failed to create job: %s", exc, exc_info=True)
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def claim_pending_jobs(self, limit: int) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH next_jobs AS (
                        SELECT id FROM jobs
                        WHERE status = 'pending'
                        ORDER BY created_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    UPDATE jobs
                    SET status = 'processing', started_at = NOW(), progress = 0
                    WHERE id IN (SELECT id FROM next_jobs)
                    RETURNING id, user_id, upload_id, status, progress, created_at, started_at, input_path, result_path
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
                conn.commit()
                return [self._row_to_dict(cur.description, row) for row in rows]
        except Exception as exc:
            logger.error("Failed to claim jobs: %s", exc, exc_info=True)
            if conn:
                conn.rollback()
            return []
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def update_progress(self, job_id: str, progress: int, message: Optional[str] = None) -> None:
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET progress = %s, status = CASE WHEN status='pending' THEN 'processing' ELSE status END
                    WHERE id = %s
                    """,
                    (progress, job_id),
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Could not update progress for %s: %s", job_id, exc)
            if conn:
                conn.rollback()
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def mark_done(self, job_id: str, *, result_path: Optional[str]) -> None:
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'done', finished_at = NOW(), progress = 100, result_path = %s
                    WHERE id = %s
                    """,
                    (result_path, job_id),
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Could not mark job %s done: %s", job_id, exc)
            if conn:
                conn.rollback()
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def mark_error(self, job_id: str, *, error_message: str) -> None:
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'error', finished_at = NOW(), error_message = %s
                    WHERE id = %s
                    """,
                    (error_message, job_id),
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Could not mark job %s errored: %s", job_id, exc)
            if conn:
                conn.rollback()
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, user_id, upload_id, status, progress, created_at, started_at, finished_at,
                           error_message, input_path, result_path
                    FROM jobs
                    WHERE id = %s
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return self._row_to_dict(cur.description, row)
        except Exception as exc:
            logger.error("Failed to fetch job %s: %s", job_id, exc, exc_info=True)
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def get_queue_position(self, job_id: str) -> Optional[int]:
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT created_at FROM jobs WHERE id = %s",
                    (job_id,),
                )
                current = cur.fetchone()
                if not current:
                    return None

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM jobs
                    WHERE status = 'pending' AND created_at < %s
                    """,
                    (current[0],),
                )
                position = cur.fetchone()[0]
                return position + 1
        except Exception as exc:
            logger.warning("Could not compute queue position for %s: %s", job_id, exc)
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def count_pending_jobs(self) -> int:
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'pending'")
                return cur.fetchone()[0]
        except Exception as exc:
            logger.warning("Could not count pending jobs: %s", exc)
            return 0
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    @staticmethod
    def _row_to_dict(description, row) -> Dict[str, Any]:
        columns = [col[0] for col in description]
        data: Dict[str, Any] = dict(zip(columns, row))
        for ts_key in ["created_at", "started_at", "finished_at"]:
            if isinstance(data.get(ts_key), datetime):
                data[ts_key] = data[ts_key].isoformat()
        return data
