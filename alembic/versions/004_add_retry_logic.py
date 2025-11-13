"""Add retry logic fields to processing_jobs

Revision ID: 004_add_retry_logic
Revises: 003_optimize_indexes
Create Date: 2025-10-21

Adds retry management fields for automatic job retries with exponential backoff
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '004_add_retry_logic'
down_revision: Union[str, Sequence[str], None] = '003_optimize_indexes'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add retry management fields to processing_jobs"""
    
    # Check if table exists
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    
    if 'processing_jobs' in inspector.get_table_names():
        # Add retry_count field (how many times this job has been retried)
        op.add_column('processing_jobs', 
            sa.Column('retry_count', sa.INTEGER(), server_default=sa.text('0'), nullable=False)
        )
        
        # Add max_retries field (maximum allowed retries)
        op.add_column('processing_jobs', 
            sa.Column('max_retries', sa.INTEGER(), server_default=sa.text('3'), nullable=False)
        )
        
        # Add next_retry_at field (when to retry next - for exponential backoff)
        op.add_column('processing_jobs',
            sa.Column('next_retry_at', sa.TIMESTAMP(), nullable=True)
        )
        
        # Add original_error field (stores first error for debugging)
        op.add_column('processing_jobs',
            sa.Column('original_error', sa.TEXT(), nullable=True)
        )


def downgrade() -> None:
    """Remove retry management fields"""
    op.drop_column('processing_jobs', 'original_error')
    op.drop_column('processing_jobs', 'next_retry_at')
    op.drop_column('processing_jobs', 'max_retries')
    op.drop_column('processing_jobs', 'retry_count')
