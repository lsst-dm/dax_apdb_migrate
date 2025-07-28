"""Migration script for schema 7.0.0.

Revision ID: schema_7.0.0
Revises: schema_6.0.0
Create Date: 2025-07-25 13:59:21.723106
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_7.0.0"
down_revision = "schema_6.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 6.0.0 to 7.0.0 (ticket DM-51934).

    Summary of changes:
      - Add boolean ``glint_trail`` column to DiaSource table(s).
    """
    with Context(revision) as ctx:
        tables = ctx.schema.tables_for_schema("DiaSource")
        if not tables:
            raise RuntimeError("Cannot find DiaSource* tables in the database.")
        for table in tables:
            column_name = "glint_trail"
            _LOG.info("Adding %s column to %s table", column_name, table)
            query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD "{column_name}" BOOLEAN'
            ctx.update(query)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        tables = ctx.schema.tables_for_schema("DiaSource")
        if not tables:
            raise RuntimeError("Cannot find DiaSource* tables in the database.")
        for table in tables:
            column_name = "glint_trail"
            _LOG.info("Dropping %s column from %s table", column_name, table)
            query = f'ALTER TABLE "{ctx.keyspace}"."{table}" DROP "{column_name}"'
            ctx.update(query)
