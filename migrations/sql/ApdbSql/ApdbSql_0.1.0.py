"""Migration script for ApdbSql 0.1.0.

Revision ID: ApdbSql_0.1.0
Revises: ApdbSql_root
Create Date: 2023-11-15 22:20:15.292273

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ApdbSql_0.1.0"
down_revision = "ApdbSql_root"
branch_labels = None
depends_on = None


def upgrade() -> None:
    raise NotImplementedError()


def downgrade() -> None:
    raise NotImplementedError()
