"""Add month tracking columns for monthly data separation

Revision ID: 005_add_month_tracking
Revises: 004_add_retry_logic
Create Date: 2025-11-12

Adds monthly tracking fields:
- poker_stats_detail.month (VARCHAR(7)) - Month in YYYY-MM format, NULL for aggregate
- processing_history.months_summary (JSONB) - Monthly manifest with metadata
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '005_add_month_tracking'
down_revision: Union[str, Sequence[str], None] = '004_add_retry_logic'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add month tracking columns"""
    
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    
    # Add month column to poker_stats_detail
    if 'poker_stats_detail' in inspector.get_table_names():
        columns = [col['name'] for col in inspector.get_columns('poker_stats_detail')]
        
        if 'month' not in columns:
            op.add_column('poker_stats_detail',
                sa.Column('month', sa.String(length=7), nullable=True,
                         comment='Month in YYYY-MM format. NULL = aggregate data (all months)')
            )
            
            # Create composite index for (token, month) queries
            # Partial index: only index non-NULL months
            op.execute("""
                CREATE INDEX IF NOT EXISTS idx_poker_stats_token_month 
                ON poker_stats_detail(token, month) 
                WHERE month IS NOT NULL
            """)
            
            # Create standalone month index for month-focused queries
            op.execute("""
                CREATE INDEX IF NOT EXISTS idx_poker_stats_month 
                ON poker_stats_detail(month) 
                WHERE month IS NOT NULL
            """)
    
    # Add months_summary column to processing_history
    if 'processing_history' in inspector.get_table_names():
        columns = [col['name'] for col in inspector.get_columns('processing_history')]
        
        if 'months_summary' not in columns:
            op.add_column('processing_history',
                sa.Column('months_summary', postgresql.JSONB(), nullable=True,
                         comment='Monthly manifest: list of months processed with metadata')
            )


def downgrade() -> None:
    """Remove month tracking columns"""
    
    # Drop indexes first
    op.execute("DROP INDEX IF EXISTS idx_poker_stats_month")
    op.execute("DROP INDEX IF EXISTS idx_poker_stats_token_month")
    
    # Drop columns
    op.drop_column('poker_stats_detail', 'month')
    op.drop_column('processing_history', 'months_summary')
