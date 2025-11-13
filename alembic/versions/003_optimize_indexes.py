"""Optimize indexes for autoscale performance

Revision ID: 003_optimize_indexes
Revises: 002_distributed_uploads
Create Date: 2025-10-21

Adds compound indexes for:
1. Job claiming queries (SELECT FOR UPDATE SKIP LOCKED)
2. Session status lookups
3. Worker coordination queries
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '003_optimize_indexes'
down_revision: Union[str, Sequence[str], None] = '002_distributed_uploads'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add optimized compound indexes for autoscale performance"""
    
    # Check if tables exist before creating indexes
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    existing_tables = inspector.get_table_names()
    
    if 'processing_jobs' in existing_tables:
        # Compound index for job claiming (SELECT ... WHERE status = 'pending' ORDER BY created_at FOR UPDATE)
        # This makes the critical job claiming query use index-only scan
        op.create_index(
            'idx_processing_jobs_status_created',
            'processing_jobs',
            ['status', 'created_at'],
            unique=False
        )
        
        # Index for finding jobs by worker (useful for monitoring and cleanup)
        op.create_index(
            'idx_processing_jobs_worker',
            'processing_jobs',
            ['worker_id', 'status'],
            unique=False
        )
    
    if 'upload_sessions' in existing_tables:
        # Compound index for session status lookups
        # Optimizes queries like: WHERE user_email = ? AND status = ?
        op.create_index(
            'idx_upload_sessions_email_status',
            'upload_sessions',
            ['user_email', 'status'],
            unique=False
        )


def downgrade() -> None:
    """Remove optimized indexes"""
    op.drop_index('idx_upload_sessions_email_status', 'upload_sessions')
    op.drop_index('idx_processing_jobs_worker', 'processing_jobs')
    op.drop_index('idx_processing_jobs_status_created', 'processing_jobs')
