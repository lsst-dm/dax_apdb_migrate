"""Migration script for schema 0.1.0.

Revision ID: schema_0.1.0
Revises: schema_root
Create Date: 2025-04-02 10:23:20.577367
"""

# revision identifiers, used by Alembic.
revision = "schema_0.1.0"
down_revision = "schema_root"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade 'schema' tree to 0.1.0.

    Schema version 0.1.0 is the initial version of this tree, it can only be
    created by executing a corresponding ApdbCassandra method.
    """
    raise NotImplementedError()


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    raise NotImplementedError()
