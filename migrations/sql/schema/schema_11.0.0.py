"""Migration script for schema 11.0.0.

Revision ID: schema_11.0.0
Revises: schema_10.0.0
Create Date: 2026-11-02 18:23:29.747982
"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_11.0.0"
down_revision = "schema_10.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

_NEW_COLUMNS = (
    sqlalchemy.Column("pixelFlags_spike", sqlalchemy.types.Boolean, nullable=True),
    sqlalchemy.Column("templateMjdMean", sqlalchemy.types.Double, nullable=True),
    sqlalchemy.Column("templateMjdMin", sqlalchemy.types.Double, nullable=True),
    sqlalchemy.Column("templateMjdMax", sqlalchemy.types.Double, nullable=True),
    sqlalchemy.Column(
        "templateInstrument", sqlalchemy.types.VARCHAR(32), nullable=False, server_default="LSSTCam"
    ),
    sqlalchemy.Column("templateVersion", sqlalchemy.types.Integer, nullable=False, server_default="0"),
    sqlalchemy.Column("mixedInstruments", sqlalchemy.types.Boolean, nullable=False, server_default="false"),
)


def upgrade() -> None:
    """Upgrade 'schema' tree from 10.0.0 to 11.0.0 (ticket DM-56923).

    Summary of changes:
      - Seven new columns added to `DiaSource` table.
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
