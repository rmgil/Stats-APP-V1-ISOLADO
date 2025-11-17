"""Create uploads table for logical uploads

Revision ID: 006_create_uploads_table
Revises: 005_add_month_tracking
Create Date: 2025-02-07

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '006_create_uploads_table'
down_revision: Union[str, Sequence[str], None] = '005_add_month_tracking'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create uploads table with logical upload metadata."""
    connection = op.get_bind()
    inspector = sa.inspect(connection)

    if 'uploads' in inspector.get_table_names():
        return

    # Ensure pgcrypto is available for gen_random_uuid()
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')

    op.create_table(
        'uploads',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('auth.users.id'), nullable=False),
        sa.Column('client_upload_token', sa.String(length=100), nullable=True),
        sa.Column('file_name', sa.String(length=500), nullable=True),
        sa.Column('file_hash', sa.String(length=64), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('updated_at', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default=sa.text('true'), nullable=False),
        sa.Column('is_master', sa.Boolean(), server_default=sa.text('false'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'file_hash', 'is_active', name='uq_uploads_user_file_active'),
    )

    op.create_index('idx_uploads_user_hash_active', 'uploads', ['user_id', 'file_hash', 'is_active'])
    op.create_index('idx_uploads_client_token', 'uploads', ['client_upload_token'])
    op.create_index('idx_uploads_user', 'uploads', ['user_id'])


def downgrade() -> None:
    """Drop uploads table and indexes."""
    op.drop_index('idx_uploads_user', 'uploads')
    op.drop_index('idx_uploads_client_token', 'uploads')
    op.drop_index('idx_uploads_user_hash_active', 'uploads')
    op.drop_table('uploads')
