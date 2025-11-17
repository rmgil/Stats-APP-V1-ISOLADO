"""Ensure single master upload per user

Revision ID: 007_master_upload_per_user
Revises: 006_create_uploads_table
Create Date: 2025-02-10

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '007_master_upload_per_user'
down_revision: Union[str, Sequence[str], None] = '006_create_uploads_table'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add partial unique index so only one master upload exists per user."""
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_uploads_master_per_user
        ON uploads (user_id)
        WHERE is_master = true
        """
    )


def downgrade() -> None:
    """Remove master upload uniqueness constraint."""
    op.execute("DROP INDEX IF EXISTS uq_uploads_master_per_user")
