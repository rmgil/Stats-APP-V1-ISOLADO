"""Service for managing logical uploads in the primary database."""
import logging
from typing import Any, Dict, Optional

from app.services.db_pool import DatabasePool

logger = logging.getLogger(__name__)


class UploadService:
    """Provide CRUD helpers for the uploads table."""

    DEFAULT_SELECT = (
        "id",
        "user_id",
        "token",
        "filename",
        "status",
        "uploaded_at",
        "processed_at",
        "hand_count",
        "archive_sha256",
        "error_message",
    )

    @staticmethod
    def _row_to_dict(description, row) -> Dict[str, Any]:
        columns = [desc[0] for desc in description]
        data = dict(zip(columns, row))
        return data

    @classmethod
    def get_master_or_latest_upload_for_user(cls, user_id: str) -> Optional[Dict[str, Any]]:
        """Return the latest upload for the user (master concept removed)."""

        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, user_id, token, filename, status,
                           uploaded_at, processed_at, hand_count,
                           archive_sha256, error_message
                    FROM uploads
                    WHERE user_id = %s
                    ORDER BY COALESCE(processed_at, uploaded_at) DESC
                    LIMIT 1
                    """,
                    (user_id,),
                )

                row = cur.fetchone()
                if row:
                    return cls._row_to_dict(cur.description, row)
                return None
        except Exception as exc:
            logger.error(
                "Failed to fetch latest upload for user %s: %s",
                user_id,
                exc,
                exc_info=True,
            )
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def get_active_upload_by_hash(self, user_id: str, file_hash: str) -> Optional[Dict[str, Any]]:
        """Fetch the most recent upload matching a user's archive hash."""
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, user_id, token, filename, status,
                           uploaded_at, processed_at, hand_count,
                           archive_sha256, error_message
                    FROM uploads
                    WHERE user_id = %s AND archive_sha256 = %s
                    ORDER BY uploaded_at DESC
                    LIMIT 1
                    """,
                    (user_id, file_hash),
                )
                row = cur.fetchone()
                if not row:
                    return None

                return self._row_to_dict(cur.description, row)
        except Exception as exc:
            logger.error("Failed to fetch existing upload: %s", exc, exc_info=True)
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def get_master_upload(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Return the latest processed upload for a user (legacy helper)."""
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, user_id, token, filename, status,
                           uploaded_at, processed_at, hand_count,
                           archive_sha256, error_message
                    FROM uploads
                    WHERE user_id = %s
                    ORDER BY COALESCE(processed_at, uploaded_at) DESC
                    LIMIT 1
                    """,
                    (user_id,),
                )
                row = cur.fetchone()
                if row:
                    return self._row_to_dict(cur.description, row)
                return None
        except Exception as exc:
            logger.error("Failed to fetch latest upload: %s", exc, exc_info=True)
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def get_upload_by_token(self, user_id: str, token: str) -> Optional[Dict[str, Any]]:
        """Fetch a single upload owned by the user that matches the token."""

        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, user_id, token, filename, status,
                           uploaded_at, processed_at, hand_count,
                           archive_sha256, error_message
                    FROM uploads
                    WHERE user_id = %s AND token = %s
                    ORDER BY uploaded_at DESC
                    LIMIT 1
                    """,
                    (user_id, token),
                )
                row = cur.fetchone()
                if row:
                    return self._row_to_dict(cur.description, row)
                return None
        except Exception as exc:
            logger.error(
                "Failed to fetch upload %s for user %s: %s",
                token,
                user_id,
                exc,
                exc_info=True,
            )
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def create_upload(
        self,
        *,
        user_id: str,
        token: str,
        filename: str,
        archive_sha256: Optional[str],
        status: str = "uploaded",
        hand_count: Optional[int] = 0,
    ) -> Optional[str]:
        """Create a new upload entry and return its ID."""
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO uploads (
                        id,
                        uploaded_at,
                        processed_at,
                        hand_count,
                        status,
                        error_message,
                        archive_sha256,
                        user_id,
                        filename,
                        token
                    )
                    VALUES (
                        gen_random_uuid(),
                        NOW(),
                        NULL,
                        %s,
                        %s,
                        NULL,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    RETURNING id
                    """,
                    (hand_count or 0, status, archive_sha256, user_id, filename, token),
                )
                new_id = cur.fetchone()[0]
                conn.commit()
                logger.info(
                    "Created upload %s for user %s (token=%s)",
                    new_id,
                    user_id,
                    token,
                )
                return str(new_id)
        except Exception as exc:
            logger.error("Failed to create upload: %s", exc, exc_info=True)
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def update_upload_status(
        self,
        upload_id: str,
        *,
        status: str,
        processed: bool = False,
        hand_count: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Update status and optional metadata for an upload entry."""

        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE uploads
                    SET status = %s,
                        processed_at = CASE
                            WHEN %s THEN COALESCE(processed_at, NOW())
                            ELSE processed_at
                        END,
                        hand_count = COALESCE(%s, hand_count),
                        error_message = %s
                    WHERE id = %s
                    """,
                    (status, processed, hand_count, error_message, upload_id),
                )
                conn.commit()
                return cur.rowcount > 0
        except Exception as exc:
            logger.error("Failed to update upload %s: %s", upload_id, exc, exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def set_master_upload(self, user_id: str, upload_id: str) -> bool:
        """Legacy helper used by background worker to mark upload as processed."""
        updated = self.update_upload_status(upload_id, status="processed", processed=True)
        if updated:
            logger.info("Marked upload %s as processed for user %s", upload_id, user_id)
        return updated

    def list_active_uploads(self, user_id: str) -> list[Dict[str, Any]]:
        """Return all uploads for a user ordered by most recent."""

        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, user_id, token, filename, status,
                           uploaded_at, processed_at, hand_count,
                           archive_sha256, error_message
                    FROM uploads
                    WHERE user_id = %s
                    ORDER BY uploaded_at DESC
                    """,
                    (user_id,),
                )

                rows = cur.fetchall() or []
                return [self._row_to_dict(cur.description, row) for row in rows]
        except Exception as exc:
            logger.error("Failed to list uploads: %s", exc, exc_info=True)
            return []
        finally:
            if conn:
                DatabasePool.return_connection(conn)

    def list_all_uploads(self, user_id: str) -> list[Dict[str, Any]]:
        """Alias to list_active_uploads for backwards compatibility."""

        return self.list_active_uploads(user_id)

    def refresh_upload(
        self,
        upload_id: str,
        *,
        token: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> bool:
        """Update metadata for an existing upload."""
        conn = None
        try:
            conn = DatabasePool.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE uploads
                    SET token = COALESCE(%s, token),
                        filename = COALESCE(%s, filename)
                    WHERE id = %s
                    """,
                    (token, filename, upload_id),
                )
                conn.commit()
                return cur.rowcount > 0
        except Exception as exc:
            logger.error("Failed to refresh upload %s: %s", upload_id, exc, exc_info=True)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                DatabasePool.return_connection(conn)
