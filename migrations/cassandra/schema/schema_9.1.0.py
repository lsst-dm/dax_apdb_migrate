"""Migration script for schema 9.1.0.

Revision ID: schema_9.1.0
Revises: schema_9.0.0
Create Date: 2025-10-14 20:54:13.669243
"""

import logging

import cassandra.query
from lsst.dax.apdb_migrate.cassandra.context import Context
from lsst.utils.iteration import chunk_iterable

# revision identifiers, used by Alembic.
revision = "schema_9.1.0"
down_revision = "schema_9.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 9.0.0 to 9.1.0 (ticket DM-52827).

    Summary of changes:

        - Add `validityStartMjdTai` column to `DiaObjectLast` table.
        - Populate new column with data from `DiaObject` table.

    Note that this implies scanning the whole `DiaObject` table and collecting
    a lot of information in memory. This may fail if amount of data is too
    large. It may be necessary to use different approach in that case, e.g.
    dumping data to CVS files with dsbulk and working on those files. Also,
    database may be configured without DiaObject table.
    """
    with Context(revision) as ctx:
        # Get the list of source tables.
        tables = ctx.schema.tables_for_schema("DiaObject", include_replica=False, include_obj_last=False)
        if tables != ["DiaObject"]:
            tables = [table for table in tables if table.startswith("DiaObject_")]
        if not tables:
            raise LookupError("Table DiaObject does not exist in this database.")

        # Get existing diaObjectIds in DiaObjectLast table.
        last_dia_object_ids = _get_last_dia_object_ids(ctx)
        _LOG.info("Found %d unique DiaObjects in DiaObjectLast table.", len(last_dia_object_ids))

        # Get diaObjectIds and their validityStart.
        validity_start_map = _get_validity_start(ctx, tables, last_dia_object_ids)
        _LOG.info("Found %d DiaObjects in DiaObject table.", len(validity_start_map))

        # Add the new column.
        _LOG.info("Adding new column")
        query = f'ALTER TABLE "{ctx.keyspace}"."DiaObjectLast" ADD "validityStartMjdTai" DOUBLE'
        ctx.update(query)

        # Fill coplumn with the data we collected.
        _populate(ctx, last_dia_object_ids, validity_start_map)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        _LOG.info("Dropping  column")
        query = f'ALTER TABLE "{ctx.keyspace}"."DiaObjectLast" DROP "validityStartMjdTai"'
        ctx.update(query)


def _get_validity_start(
    ctx: Context, tables: list[str], last_dia_object_ids: dict[int, int]
) -> dict[int, float]:
    """Scan DiaObject table(s) and return max. validityStart values for each
    diaObjectId.
    """
    validity_map: dict[int, float] = {}
    for table in sorted(tables):
        _LOG.info("Scanning %s table", table)

        query = f'SELECT "diaObjectId", "validityStartMjdTai" FROM "{ctx.keyspace}"."{table}" ALLOW FILTERING'
        # This can take some time, make sure we do not timeout.
        result = ctx.query(query, timeout=3600)

        for row in result:
            diaObjectId: int = row.diaObjectId
            if diaObjectId in last_dia_object_ids:
                validityStartMjdTai: float = row.validityStartMjdTai
                if (existing_validity := validity_map.get(diaObjectId)) is not None:
                    validity_map[diaObjectId] = max(existing_validity, validityStartMjdTai)
                else:
                    validity_map[diaObjectId] = validityStartMjdTai

    return validity_map


def _get_last_dia_object_ids(ctx: Context) -> dict[int, int]:
    """Return all existing diaObjectIds in DiaObjectLast table.

    Parameters
    ----------
    ctx
        Migration context.

    Returns
    -------
    ids : `dict` [`int`, `int`]
        Mapping of diaObjectId to its corresponding partition in DiaObjectLast
        table.
    """
    _LOG.info("Scanning DiaObjectLast table")

    query = f'SELECT "diaObjectId", apdb_part FROM "{ctx.keyspace}"."DiaObjectLast" ALLOW FILTERING'
    # This can take some time, make sure we do not timeout.
    result = ctx.query(query, timeout=3600)
    return {row[0]: row[1] for row in result}


def _populate(
    ctx: Context, last_dia_object_ids: dict[int, int], validity_start_map: dict[int, float]
) -> None:
    """Fill validityStart column in DiaObjectLast."""
    update = (
        f'UPDATE "{ctx.keyspace}"."DiaObjectLast" '
        'SET "validityStartMjdTai" = ? '
        'WHERE apdb_part = ? AND "diaObjectId" = ?'
    )
    # This code cannot be executed in dry-run mode because of prepare(),
    # so just print something and return.
    if ctx.dry_run:
        count = len(set(last_dia_object_ids) & set(validity_start_map))
        _LOG.info("Dry-run mode - will update %d records, query: %s", count, update)
        return

    # Prepare UPDATE query.
    update_stmt = ctx.session.prepare(update)

    # Make batches of 10k updates, it may be worth group by partitions, but
    # it's not clear if it's going to accelerate anything.
    total_updates = 0
    for chunk in chunk_iterable(last_dia_object_ids.items(), 10_000):
        batch = cassandra.query.BatchStatement()
        for diaObjectId, apdb_part in chunk:
            if (validity := validity_start_map.get(diaObjectId)) is not None:
                batch.add(update_stmt, (validity, apdb_part, diaObjectId))
        _LOG.info("Updating %d records.", len(batch))
        total_updates += len(batch)
        ctx.update(batch)

    _LOG.info("Updated %d records in total.", total_updates)
