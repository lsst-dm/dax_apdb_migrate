"""Migration script for schema 7.0.0.

Revision ID: schema_7.0.0
Revises: schema_6.0.0
Create Date: 2025-07-25 13:51:45.013540
"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_7.0.0"
down_revision = "schema_6.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 6.0.0 to 7.0.0 (ticket DM-51934).

    Summary of changes:
      - Add boolean ``glint_trail`` column to DiaSource table.
    """
    with Context(revision) as ctx:
        table_name = "DiaSource"
        table = ctx.get_table(table_name)
        with ctx.batch_alter_table(table_name, copy_from=table) as batch_op:
            # Add a column, NULL as default, do not fill.
            column_name = "glint_trail"
            _LOG.info("Adding %s column to %s table", column_name, table_name)
            batch_op.add_column(sqlalchemy.Column(column_name, sqlalchemy.types.Boolean, nullable=True))


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        table_name = "DiaSource"
        table = ctx.get_table(table_name)
        with ctx.batch_alter_table(table_name, copy_from=table) as batch_op:
            column_name = "glint_trail"
            _LOG.info("Dropping %s column from %s table", column_name, table_name)
            batch_op.drop_column(column_name)
