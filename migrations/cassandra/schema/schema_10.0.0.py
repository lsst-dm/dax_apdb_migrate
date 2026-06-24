"""Migration script for schema 10.0.0.

Revision ID: schema_10.0.0
Revises: schema_9.1.1
Create Date: 2026-06-24 11:39:45.977765
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_10.0.0"
down_revision = "schema_9.1.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

_NEW_COLUMNS = (
    ("exposureTime", "FLOAT"),
    ("trailAlgorithm", "INT"),
    ("trail_flag", "BOOLEAN"),
    ("reliabilityVersion", "TEXT"),
)


def upgrade() -> None:
    """Upgrade 'schema' tree from 9.1.1 to 10.0.0 (ticket DM-54971).

    Summary of changes:
      - New columns added to `DiaSource` table: `exposureTime`,
        `trailAlgorithm`, `trail_flag`, and `reliabilityVersion`.
    """
    with Context(revision) as ctx:
        tables = ctx.schema.tables_for_schema("DiaSource")
        for table in tables:
            _LOG.info("Adding %d columns to table %s", len(_NEW_COLUMNS), table)
            additions = [f'"{column}" {column_type}' for column, column_type in _NEW_COLUMNS]
            query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD ({", ".join(additions)})'
            ctx.update(query)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        tables = ctx.schema.tables_for_schema("DiaSource")
        for table in tables:
            _LOG.info("Dropping %d columns from table %s", len(_NEW_COLUMNS), table)
            drops = [f'"{column}"' for column, _ in _NEW_COLUMNS]
            query = f'ALTER TABLE "{ctx.keyspace}"."{table}" DROP ({", ".join(drops)})'
            ctx.update(query)
