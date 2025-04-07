"""Migration script for ApdbCassandra 0.1.0.

Revision ID: ApdbCassandra_0.1.0
Revises: ApdbCassandra_root
Create Date: 2025-04-02 10:17:14.436833
"""

# revision identifiers, used by Alembic.
revision = "ApdbCassandra_0.1.0"
down_revision = "ApdbCassandra_root"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade 'ApdbCassandra' tree to 0.1.0.

    Schema version 0.1.0 is the initial version of this tree, it can only be
    created by executing a corresponding ApdbCassandra method.
    """
    raise NotImplementedError()


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    raise NotImplementedError()
