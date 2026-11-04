"""Migration script for schema 11.0.0.

Revision ID: schema_11.0.0
Revises: schema_10.0.0
Create Date: 2026-11-02 18:47:38.989065
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_11.0.0"
down_revision = "schema_10.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

_NEW_COLUMNS = (
    ("pixelFlags_spike", "BOOLEAN"),
    ("templateMjdMean", "DOUBLE"),
    ("templateMjdMin", "DOUBLE"),
    ("templateMjdMax", "DOUBLE"),
    ("templateInstrument", "TEXT"),
    ("templateVersion", "INT"),
    ("mixedInstruments", "BOOLEAN"),
)


def upgrade() -> None:
    """Upgrade 'schema' tree from 10.0.0 to 11.0.0 (ticket DM-56923).

    Summary of changes:
      - Seven new columns added to `DiaSource` table
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
