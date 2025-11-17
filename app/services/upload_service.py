"""Service for managing logical uploads in the primary database."""
import logging
from typing import Any, Dict, Optional

from app.services.db_pool import DatabasePool

logger = logging.getLogger(__name__)


class UploadService:
    """Provide CRUD helpers for the uploads table."""

    def get_active_upload_by_hash(self, user_id: str, file_hash: str) -> Optional[Dict[str, Any]]:
        """Fetch the most recent active upload matching a user's file hash."""
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, user_id, client_upload_token, file_name, file_hash,
                           created_at, updated_at, is_active, is_master
                    FROM uploads
                    WHERE user_id = %s AND file_hash = %s AND is_active = true
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (user_id, file_hash),
                )
                row = cur.fetchone()
                if not row:
                    return None

                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))
        except Exception as exc:
            logger.error("Failed to fetch existing upload: %s", exc, exc_info=True)
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def create_upload(
        self,
        *,
        user_id: str,
        client_upload_token: Optional[str],
        file_name: str,
        file_hash: str,
        is_master: bool = False,
    ) -> Optional[str]:
        """Create a new logical upload entry and return its ID."""
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO uploads (
                        user_id, client_upload_token, file_name, file_hash, is_active, is_master
                    ) VALUES (%s, %s, %s, %s, true, %s)
                    RETURNING id
                    """,
                    (user_id, client_upload_token, file_name, file_hash, is_master),
                )
                new_id = cur.fetchone()[0]
                conn.commit()
                logger.info(
                    "Created logical upload %s for user %s (token=%s)",
                    new_id,
                    user_id,
                    client_upload_token,
                )
                return str(new_id)
        except Exception as exc:
            logger.error("Failed to create logical upload: %s", exc, exc_info=True)
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def refresh_upload(self, upload_id: str, *, client_upload_token: Optional[str] = None, file_name: Optional[str] = None) -> bool:
        """Update the timestamp (and optionally metadata) for an existing upload."""
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE uploads
                    SET updated_at = NOW(),
                        client_upload_token = COALESCE(%s, client_upload_token),
                        file_name = COALESCE(%s, file_name)
                    WHERE id = %s
                    """,
                    (client_upload_token, file_name, upload_id),
                )
                conn.commit()
                return True
        except Exception as exc:
            logger.error("Failed to refresh upload %s: %s", upload_id, exc, exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                DatabasePool.return_connection(conn)
