"""Migration script for schema 3.0.0.

Revision ID: schema_3.0.0
Revises: schema_2.0.1
Create Date: 2025-04-02 10:25:59.949120
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_3.0.0"
down_revision = "schema_2.0.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 2.0.1 to 3.0.0 (ticket DM-48106).

    Summary of changes:
      - Add empty dipoleFitAttempted column.
    """
    _migrate(True, revision)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    _migrate(False, down_revision)


def _migrate(add: bool, final_revision: str) -> None:
    # Do schema migrations.
    with Context(final_revision) as ctx:
        tables = ["DiaSource"]
        if ctx.has_replicas():
            tables.append("DiaSourceChunks")

        column = "dipoleFitAttempted"
        for table in tables:
            if add:
                _LOG.info("Adding column %s to table %s", column, table)
                query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD "{column}" BOOLEAN'
            else:
                _LOG.info("Dropping column %s from table %s", column, table)
                query = f'ALTER TABLE "{ctx.keyspace}"."{table}" DROP "{column}"'
            ctx.update(query)
