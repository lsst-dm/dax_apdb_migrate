"""An initial pseudo-revision of the 'ApdbSqlReplica' tree.

Revision ID: ApdbSqlReplica_root
Revises:
Create Date: 2024-07-29 11:47:20.755789

"""

# revision identifiers, used by Alembic.
revision = "ApdbSqlReplica_root"
down_revision = None
branch_labels = ("ApdbSqlReplica",)
depends_on = None


def upgrade() -> None:
    """Upgrade is not needed for this revision."""
    raise NotImplementedError()


def downgrade() -> None:
    """Downgrade is not needed for this revision."""
    raise NotImplementedError()
