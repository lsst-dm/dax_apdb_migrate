"""Migration script for schema 6.0.0.

Revision ID: schema_6.0.0
Revises: schema_5.0.0
Create Date: 2025-04-02 10:26:10.607570
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_6.0.0"
down_revision = "schema_5.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 5.0.0 to 6.0.0 (ticket DM-45621).

    Summary of changes:
      - Add empty pixelFlags_nodata and pixelFlags_nodataCenter columns.
    """
    _migrate(True, revision)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    _migrate(False, down_revision)


def _migrate(add: bool, final_revision: str) -> None:
    # Do schema migrations.
    with Context(final_revision) as ctx:
        new_columns = ["pixelFlags_nodata", "pixelFlags_nodataCenter"]
        tables = ["DiaSource"]
        if ctx.has_replicas():
            tables.append("DiaSourceChunks")

        for table in tables:
            for column in new_columns:
                if add:
                    _LOG.info("Adding column %s to table %s", column, table)
                    query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD "{column}" BOOLEAN'
                else:
                    _LOG.info("Dropping column %s from table %s", column, table)
                    query = f'ALTER TABLE "{ctx.keyspace}"."{table}" DROP "{column}"'
                ctx.update(query)
