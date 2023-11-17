"""Migration script for schema 0.1.0.

Revision ID: schema_0.1.0
Revises: schema_root
Create Date: 2023-11-15 22:20:09.345919
"""

# revision identifiers, used by Alembic.
revision = "schema_0.1.0"
down_revision = "schema_root"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema to a new version."""
    raise NotImplementedError()


def downgrade() -> None:
    """Downgrade database schema to a previous version."""
    raise NotImplementedError()
