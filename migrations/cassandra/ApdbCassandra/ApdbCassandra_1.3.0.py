"""Migration script for ApdbCassandra 1.3.0.

Revision ID: ApdbCassandra_1.3.0
Revises: ApdbCassandra_1.2.0
Create Date: 2025-10-13 16:01:11.609198
"""

import logging

import cassandra.query

from lsst.dax.apdb_migrate.cassandra.context import Context
from lsst.utils.iteration import chunk_iterable

# revision identifiers, used by Alembic.
revision = "ApdbCassandra_1.3.0"
down_revision = "ApdbCassandra_1.2.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

_TABLE_NAME = "DiaObjectDedup"

_CREATE_TABLE = """\
CREATE TABLE "{keyspace}"."{table_name}" (
  dedup_part smallint,
  "diaObjectId" bigint,
  "validityStartMjdTai" double,
  ra double,
  dec double,
  "nDiaSources" int,
  PRIMARY KEY (dedup_part, "diaObjectId", "validityStartMjdTai")
)
"""

_COLUMNS = ("diaObjectId", "validityStartMjdTai", "ra", "dec", "nDiaSources")


def upgrade() -> None:
    """Upgrade 'ApdbCassandra' tree from 1.2.0 to 1.3.0 (ticket ...).

    Summary of changes:
      - Create a "nightly" subset of DiaObject table.
    """
    with Context(revision) as ctx:
        source = ctx.get_mig_option("data-source")
        if source not in ("DiaObject", "DiaObjectLast"):
            raise ValueError(
                "This migration script requires name of the table used as source of data. "
                "Please use `--options data-source=SOURCE` command line option, "
                "where SOURCE is one one of DiaObjectLast, DiaObject, or none."
            )
        num_part_str = ctx.get_mig_option("num-partitions")
        if num_part_str is None or not num_part_str.isdigit():
            raise ValueError(
                "This migration script requires number of partitions for data in the new table. "
                "Please use `--options num-partitions=NNN` command line option."
            )
        num_part = int(num_part_str)

        if source == "none":
            source_tables = []
        else:
            # Get the list of source tables.
            source_tables = ctx.schema.tables_for_schema(
                "DiaObject", include_replica=False, include_obj_last=True
            )
            if source == "DiaObjectLast":
                source_tables = [table for table in source_tables if table == "DiaObjectLast"]
            else:
                # Could be either DiaObject or DiaObject_NNN
                if "DiaObject" in source_tables:
                    source_tables = ["DiaObject"]
                else:
                    source_tables = [table for table in source_tables if table.startswith("DiaObject_")]
            if not source_tables:
                raise LookupError(f"Table {source} does not exist in this database.")

        # Create table.
        _LOG.info("Creating table %s", _TABLE_NAME)
        query = _CREATE_TABLE.format(keyspace=ctx.keyspace, table_name=_TABLE_NAME)
        ctx.session.execute(query)

        if source_tables:
            _populate(ctx, source_tables, num_part)

        # Update configuration.
        updates = {"num_part_dedup": num_part}
        ctx.metadata.update_config(updates)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        # Drop table.
        _LOG.info("Dropping table %s", _TABLE_NAME)
        query = f'DROP TABLE "{ctx.keyspace}"."{_TABLE_NAME}"'
        ctx.session.execute(query)

        # Update configuration.
        deletes = ["num_part_dedup"]
        ctx.metadata.update_config(deletes=deletes)


def _populate(ctx: Context, source_tables: list[str], num_part: int) -> None:
    """Populate new table from one or more other tables."""
    column_list = ", ".join(f'"{column}"' for column in _COLUMNS)

    # Prepare insert query.
    placeholders = ",".join(["?"] * (len(_COLUMNS) + 1))
    insert = (
        f'INSERT INTO "{ctx.keyspace}"."{_TABLE_NAME}" (dedup_part, {column_list}) VALUES ({placeholders})'
    )
    insert_stmt = ctx.session.prepare(insert)

    total_count = 0
    partition = 0
    for table in sorted(source_tables):
        _LOG.info("Populating %s from %s", _TABLE_NAME, table)

        query = f'SELECT {column_list} FROM "{ctx.keyspace}"."{table}" ALLOW FILTERING'
        # This can take some time, make sure we do not timeout.
        result = ctx.session.execute(query, timeout=3600)
        # Somehow itertools don't work on result, need to make a list first
        result = list(result)

        count = 0
        # Make batches of 1k inserts and send them to the same partition.
        for row_chunk in chunk_iterable(result, 1_000):
            batch = cassandra.query.BatchStatement()
            for row in row_chunk:
                count += 1
                params = [partition] + list(row)
                batch.add(insert_stmt, params)
            ctx.session.execute(batch)

            # Move to next partition.
            partition = (partition + 1) % num_part

        _LOG.info("Inserted %d records from table %s", count, table)
        total_count += count

    _LOG.info("Inserted %d records total", total_count)
