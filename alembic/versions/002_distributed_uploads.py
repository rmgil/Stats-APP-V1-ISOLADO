"""Add distributed upload tables

Revision ID: 002_distributed_uploads
Revises: 001_baseline_schema
Create Date: 2025-10-20

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '002_distributed_uploads'
down_revision: Union[str, Sequence[str], None] = '001_baseline_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add tables for distributed upload system"""
    
    # Check if tables exist
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    existing_tables = inspector.get_table_names()
    
    # Table 1: upload_sessions - tracks upload sessions across instances
    if 'upload_sessions' not in existing_tables:
        op.create_table('upload_sessions',
            sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
            sa.Column('token', sa.VARCHAR(length=50), nullable=False),
            sa.Column('user_email', sa.VARCHAR(length=255), nullable=True),
            sa.Column('filename', sa.VARCHAR(length=500), nullable=False),
            sa.Column('total_chunks', sa.INTEGER(), nullable=False),
            sa.Column('received_chunks', sa.INTEGER(), server_default=sa.text('0'), nullable=False),
            sa.Column('file_size', sa.BIGINT(), nullable=True),
            sa.Column('status', sa.VARCHAR(length=50), server_default=sa.text("'uploading'"), nullable=False),
            sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
            sa.Column('updated_at', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
            sa.Column('expires_at', postgresql.TIMESTAMP(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('token')
        )
        op.create_index('idx_upload_sessions_token', 'upload_sessions', ['token'])
        op.create_index('idx_upload_sessions_status', 'upload_sessions', ['status'])
        op.create_index('idx_upload_sessions_expires', 'upload_sessions', ['expires_at'])
    
    # Table 2: upload_chunks - stores individual chunks in PostgreSQL
    if 'upload_chunks' not in existing_tables:
        op.create_table('upload_chunks',
            sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
            sa.Column('session_token', sa.VARCHAR(length=50), nullable=False),
            sa.Column('chunk_index', sa.INTEGER(), nullable=False),
            sa.Column('chunk_data', postgresql.BYTEA(), nullable=False),
            sa.Column('chunk_size', sa.INTEGER(), nullable=False),
            sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.ForeignKeyConstraint(['session_token'], ['upload_sessions.token'], ondelete='CASCADE'),
            sa.UniqueConstraint('session_token', 'chunk_index', name='uq_session_chunk')
        )
        op.create_index('idx_upload_chunks_session', 'upload_chunks', ['session_token'])
        op.create_index('idx_upload_chunks_index', 'upload_chunks', ['session_token', 'chunk_index'])
    
    # Table 3: processing_jobs - background processing queue
    if 'processing_jobs' not in existing_tables:
        op.create_table('processing_jobs',
            sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
            sa.Column('token', sa.VARCHAR(length=50), nullable=False),
            sa.Column('user_email', sa.VARCHAR(length=255), nullable=True),
            sa.Column('job_type', sa.VARCHAR(length=50), nullable=False),
            sa.Column('status', sa.VARCHAR(length=50), server_default=sa.text("'pending'"), nullable=False),
            sa.Column('progress', sa.INTEGER(), server_default=sa.text('0'), nullable=False),
            sa.Column('error_message', sa.TEXT(), nullable=True),
            sa.Column('result_data', postgresql.JSONB(), nullable=True),
            sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
            sa.Column('started_at', postgresql.TIMESTAMP(), nullable=True),
            sa.Column('completed_at', postgresql.TIMESTAMP(), nullable=True),
            sa.Column('worker_id', sa.VARCHAR(length=100), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('token')
        )
        op.create_index('idx_processing_jobs_token', 'processing_jobs', ['token'])
        op.create_index('idx_processing_jobs_status', 'processing_jobs', ['status'])
        op.create_index('idx_processing_jobs_created', 'processing_jobs', ['created_at'])


def downgrade() -> None:
    """Remove distributed upload tables"""
    op.drop_index('idx_processing_jobs_created', 'processing_jobs')
    op.drop_index('idx_processing_jobs_status', 'processing_jobs')
    op.drop_index('idx_processing_jobs_token', 'processing_jobs')
    op.drop_table('processing_jobs')
    
    op.drop_index('idx_upload_chunks_index', 'upload_chunks')
    op.drop_index('idx_upload_chunks_session', 'upload_chunks')
    op.drop_table('upload_chunks')
    
    op.drop_index('idx_upload_sessions_expires', 'upload_sessions')
    op.drop_index('idx_upload_sessions_status', 'upload_sessions')
    op.drop_index('idx_upload_sessions_token', 'upload_sessions')
    op.drop_table('upload_sessions')
