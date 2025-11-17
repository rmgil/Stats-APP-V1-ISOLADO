"""create jobs table for background processing

Revision ID: 008_create_jobs_table
Revises: 007_master_upload_per_user
Create Date: 2025-03-01
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "008_create_jobs_table"
down_revision: Union[str, Sequence[str], None] = "007_master_upload_per_user"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create jobs table used by the background worker queue."""
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    if "jobs" in inspector.get_table_names():
        return

    op.create_table(
        "jobs",
        sa.Column("id", sa.String(length=32), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("auth.users.id"), nullable=False),
        sa.Column("upload_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("uploads.id"), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("progress", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", postgresql.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("input_path", sa.Text(), nullable=False),
        sa.Column("result_path", sa.Text(), nullable=True),
    )

    op.create_index("idx_jobs_status_created", "jobs", ["status", "created_at"])
    op.create_index("idx_jobs_upload", "jobs", ["upload_id"])
    op.create_index("idx_jobs_user", "jobs", ["user_id"])


def downgrade() -> None:
    """Drop jobs table and indexes."""
    op.drop_index("idx_jobs_user", table_name="jobs")
    op.drop_index("idx_jobs_upload", table_name="jobs")
    op.drop_index("idx_jobs_status_created", table_name="jobs")
    op.drop_table("jobs")
