"""Migration script for ApdbCassandraReplica 1.0.0.

Revision ID: ApdbCassandraReplica_1.0.0
Revises: ApdbCassandraReplica_root
Create Date: 2025-04-02 10:07:34.892335
"""

# revision identifiers, used by Alembic.
revision = "ApdbCassandraReplica_1.0.0"
down_revision = "ApdbCassandraReplica_root"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade 'ApdbCassandraReplica' tree to 1.0.0.

    Schema version 1.0.0 is the initial version of this tree, it can only be
    created by executing a corresponding ApdbCassandra method.
    """
    raise NotImplementedError()


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    raise NotImplementedError()
