"""Baseline schema - current database state

Revision ID: 001_baseline_schema
Revises: 
Create Date: 2025-10-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_baseline_schema'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    This migration captures the current state of the database.
    Since the tables already exist, we don't create them.
    This serves as a baseline for future migrations.
    """
    # Check if tables exist, create only if they don't
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    existing_tables = inspector.get_table_names()
    
    # Create approved_emails table if not exists
    if 'approved_emails' not in existing_tables:
        op.create_table('approved_emails',
            sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
            sa.Column('email', sa.VARCHAR(length=255), nullable=False),
            sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
            sa.Column('created_by', sa.VARCHAR(length=255), nullable=True),
            sa.Column('active', sa.BOOLEAN(), server_default=sa.text('true'), nullable=True),
            sa.Column('notes', sa.TEXT(), nullable=True),
            sa.Column('is_admin', sa.BOOLEAN(), server_default=sa.text('false'), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('email')
        )
        op.create_index('idx_approved_emails_email', 'approved_emails', ['email'])
        op.create_index('idx_approved_emails_active', 'approved_emails', ['active'])
    
    # Create invite_codes table if not exists
    if 'invite_codes' not in existing_tables:
        op.create_table('invite_codes',
            sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
            sa.Column('code', sa.VARCHAR(length=100), nullable=False),
            sa.Column('used', sa.BOOLEAN(), server_default=sa.text('false'), nullable=True),
            sa.Column('used_by_email', sa.VARCHAR(length=255), nullable=True),
            sa.Column('used_at', postgresql.TIMESTAMP(), nullable=True),
            sa.Column('expires_at', postgresql.TIMESTAMP(), nullable=True),
            sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
            sa.Column('created_by', sa.VARCHAR(length=255), nullable=True),
            sa.Column('max_uses', sa.INTEGER(), server_default=sa.text('1'), nullable=True),
            sa.Column('times_used', sa.INTEGER(), server_default=sa.text('0'), nullable=True),
            sa.Column('notes', sa.TEXT(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('code')
        )
        op.create_index('idx_invite_codes_code', 'invite_codes', ['code'])
        op.create_index('idx_invite_codes_used', 'invite_codes', ['used'])
        op.create_index('idx_invite_codes_expires', 'invite_codes', ['expires_at'])
    
    # Create admin_audit_log table if not exists
    if 'admin_audit_log' not in existing_tables:
        op.create_table('admin_audit_log',
            sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
            sa.Column('admin_email', sa.VARCHAR(length=255), nullable=False),
            sa.Column('action_type', sa.VARCHAR(length=100), nullable=False),
            sa.Column('target_email', sa.VARCHAR(length=255), nullable=True),
            sa.Column('details', sa.TEXT(), nullable=True),
            sa.Column('ip_address', sa.VARCHAR(length=45), nullable=True),
            sa.Column('timestamp', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
        op.create_index('idx_admin_audit_admin', 'admin_audit_log', ['admin_email'])
        op.create_index('idx_admin_audit_timestamp', 'admin_audit_log', ['timestamp'])


def downgrade() -> None:
    """
    Since this is the baseline, downgrade would remove all tables.
    This should be done carefully in production.
    """
    op.drop_index('idx_admin_audit_timestamp', 'admin_audit_log')
    op.drop_index('idx_admin_audit_admin', 'admin_audit_log')
    op.drop_table('admin_audit_log')
    
    op.drop_index('idx_invite_codes_expires', 'invite_codes')
    op.drop_index('idx_invite_codes_used', 'invite_codes')
    op.drop_index('idx_invite_codes_code', 'invite_codes')
    op.drop_table('invite_codes')
    
    op.drop_index('idx_approved_emails_active', 'approved_emails')
    op.drop_index('idx_approved_emails_email', 'approved_emails')
    op.drop_table('approved_emails')