"""An initial pseudo-revision of the 'schema' tree.

Revision ID: schema_root
Revises:
Create Date: 2023-11-15 22:19:21.552485
"""

# revision identifiers, used by Alembic.
revision = "schema_root"
down_revision = None
branch_labels = ("schema",)
depends_on = None


def upgrade() -> None:
    """Upgrade database schema to a new version."""
    raise NotImplementedError()


def downgrade() -> None:
    """Downgrade database schema to a previous version."""
    raise NotImplementedError()
