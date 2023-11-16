"""This is an initial pseudo-revision of the 'schema' tree.

Revision ID: schema_root
Revises:
Create Date: 2023-11-15 22:19:21.552485

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "schema_root"
down_revision = None
branch_labels = ("schema",)
depends_on = None


def upgrade() -> None:
    raise NotImplementedError()


def downgrade() -> None:
    raise NotImplementedError()
