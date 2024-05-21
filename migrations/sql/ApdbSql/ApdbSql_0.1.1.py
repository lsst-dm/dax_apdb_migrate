"""Migration script for ApdbSql 0.1.1.

Revision ID: ApdbSql_0.1.1
Revises: ApdbSql_0.1.0
Create Date: 2024-05-20 14:34:21.898829
"""

import logging
from collections.abc import Iterator

import sqlalchemy

from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbSql_0.1.1"
down_revision = "ApdbSql_0.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

# Tables and columns to update.
_columns = {
    "DiaObject": ["validityStart", "validityEnd", "lastNonForcedSource"],
    "DiaForcedSource": ["time_processed", "time_withdrawn"],
    "DiaSource": ["ssObjectReassocTime", "time_processed", "time_withdrawn"],
    "MPCORB": ["arcStart", "arcEnd"],
    "ApdbReplicaChunk": ["last_update_time"],
}

# Tables that can have additional chunks tables.
_have_chunks = {"DiaObject", "DiaForcedSource", "DiaSource"}


def upgrade() -> None:
    """Upgrade 'ApdbSql' tree from 0.1.0 to 0.1.1 (ticket DM-44129).

    Summary of changes:

      - Timestamp column type changed to TIMESTAMP WITH TIMEZONE.
    """
    _migrate(True)


def downgrade() -> None:
    """Undo schema changes."""
    _migrate(False)


def _migrate(upgrade: bool) -> None:
    """Update types of timestamp columns.

    Parameters
    ----------
    upgrade : `bool`
        True if upgrading to TIMESTAMP WITH TIMEZONE type, false for downgrade.
    """
    ctx = Context()

    for table, columns in _find_tables(ctx):
        _LOG.info("Updating table %s columns: %s", table, columns)
        with ctx.batch_alter_table(table) as batch_op:
            for column in columns:
                if upgrade:
                    batch_op.alter_column(column, type_=sqlalchemy.types.TIMESTAMP(timezone=True))
                else:
                    batch_op.alter_column(column, type_=sqlalchemy.types.TIMESTAMP)

    # Update metadata version.
    if upgrade:
        tree, _, version = revision.partition("_")
    else:
        tree, _, version = down_revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)


def _find_tables(ctx: Context) -> Iterator[tuple[str, list[str]]]:
    """Generate list of tables that exist in the schema and the columns
    that need type change.

    Yields
    ------
    table : `str`.
        Table name.
    columns : `list` [`str`]
        Column names.
    """
    inspect = sqlalchemy.inspect(ctx.bind)
    for table, columns in _columns.items():
        if inspect.has_table(table, schema=ctx.schema):
            yield table, columns

    for table in _have_chunks:
        chunks_table = f"{table}Chunks"
        if inspect.has_table(chunks_table, schema=ctx.schema):
            yield chunks_table, _columns[table]
