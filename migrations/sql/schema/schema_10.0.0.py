"""Migration script for schema 10.0.0.

Revision ID: schema_10.0.0
Revises: schema_9.1.1
Create Date: 2026-06-24 10:19:43.114066
"""

import logging

import sqlalchemy

from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_10.0.0"
down_revision = "schema_9.1.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

_NEW_COLUMNS = (
    sqlalchemy.Column("exposureTime", sqlalchemy.types.REAL, nullable=True),
    sqlalchemy.Column("trailAlgorithm", sqlalchemy.types.Integer, nullable=True),
    sqlalchemy.Column("trail_flag", sqlalchemy.types.Boolean, nullable=True),
    sqlalchemy.Column("reliabilityVersion", sqlalchemy.types.VARCHAR(7), nullable=True),
)


def upgrade() -> None:
    """Upgrade 'schema' tree from 9.1.1 to 10.0.0 (ticket DM-54971).

    Summary of changes:
      - New columns added to `DiaSource` table: `exposureTime`,
        `trailAlgorithm`, `trail_flag`, and `reliabilityVersion`.
    """
    with Context(revision) as ctx:
        table_name = "DiaSource"

        with ctx.batch_alter_table(table_name) as batch_op:
            for column in _NEW_COLUMNS:
                _LOG.info("Adding %s column to %s table", column.name, table_name)
                batch_op.add_column(column)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        table_name = "DiaSource"

        with ctx.batch_alter_table(table_name) as batch_op:
            for column in _NEW_COLUMNS:
                _LOG.info("Dropping %s column from %s table", column.name, table_name)
                batch_op.drop_column(column.name)
