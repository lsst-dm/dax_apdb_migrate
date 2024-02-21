"""An initial pseudo-revision of the 'ApdbSql' tree.

Revision ID: ApdbSql_root
Revises:
Create Date: 2023-11-15 22:19:26.481473
"""

# revision identifiers, used by Alembic.
revision = "ApdbSql_root"
down_revision = None
branch_labels = ("ApdbSql",)
depends_on = None


def upgrade() -> None:
    """Upgrade database schema to a new version."""
    raise NotImplementedError()


def downgrade() -> None:
    """Downgrade database schema to a previous version."""
    raise NotImplementedError()
