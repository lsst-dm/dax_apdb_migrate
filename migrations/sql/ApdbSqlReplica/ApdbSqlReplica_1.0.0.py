"""Migration script for ApdbSqlReplica 1.0.0.

Revision ID: ApdbSqlReplica_1.0.0
Revises: ApdbSqlReplica_root
Create Date: 2024-07-29 11:49:49.532074

"""

# revision identifiers, used by Alembic.
revision = "ApdbSqlReplica_1.0.0"
down_revision = "ApdbSqlReplica_root"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade is not needed as this is the initial version."""
    raise NotImplementedError()


def downgrade() -> None:
    """Downgrade is not needed as this is the initial version."""
    raise NotImplementedError()
