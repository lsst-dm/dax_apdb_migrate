"""Migration script for schema 9.1.1.

Revision ID: schema_9.1.1
Revises: schema_9.1.0
Create Date: 2025-12-04 17:08:17.648160
"""

import logging
from collections import defaultdict

import cassandra.query
from lsst.dax.apdb_migrate.cassandra.context import Context
from lsst.utils.iteration import chunk_iterable

# revision identifiers, used by Alembic.
revision = "schema_9.1.1"
down_revision = "schema_9.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

_COLUMNS = {
    "DiaObject": (
        "u_psfFluxNdata",
        "g_psfFluxNdata",
        "r_psfFluxNdata",
        "i_psfFluxNdata",
        "z_psfFluxNdata",
        "y_psfFluxNdata",
    ),
    "DiaSource": (
        "psfNdata",
        "trailNdata",
        "dipoleNdata",
        "bboxSize",
    ),
}


def upgrade() -> None:
    """Upgrade 'schema' tree from 9.1.0 to 9.1.1 (ticket DM-53543).

    Summary of changes:
      - A number of integer columns are now NOT NULL, this does not mean
        anything for Cassandra as it does not have that sort of constraint.
      - The columns need to be populated with 0 if they are NULL.
    """
    with Context(revision) as ctx:
        # DiaObject tables.
        updated_columns = _COLUMNS["DiaObject"]
        primary_key: tuple[str, ...]
        if ctx.schema.check_table("DiaObject"):
            primary_key = ("apdb_part", "apdb_time_part", "diaObjectId", "validityStartMjdTai")
            _upgrade_table(ctx, "DiaObject", primary_key, updated_columns)
        elif patritioned_tables := ctx.schema.partitioned_tables("DiaObject"):
            primary_key = ("apdb_part", "diaObjectId", "validityStartMjdTai")
            for table in patritioned_tables:
                _upgrade_table(ctx, table, primary_key, updated_columns)
        if replica_tables := ctx.schema.replica_tables("DiaObject"):
            for table in replica_tables:
                if table.endswith("2"):
                    primary_key = (
                        "apdb_replica_chunk",
                        "apdb_replica_subchunk",
                        "diaObjectId",
                        "validityStartMjdTai",
                    )
                else:
                    primary_key = ("apdb_replica_chunk", "diaObjectId", "validityStartMjdTai")
                _upgrade_table(ctx, table, primary_key, updated_columns)

        # DiaSource tables.
        updated_columns = _COLUMNS["DiaSource"]
        if ctx.schema.check_table("DiaSource"):
            primary_key = ("apdb_part", "apdb_time_part", "diaSourceId")
            _upgrade_table(ctx, "DiaSource", primary_key, updated_columns)
        elif patritioned_tables := ctx.schema.partitioned_tables("DiaSource"):
            primary_key = ("apdb_part", "diaSourceId")
            for table in patritioned_tables:
                _upgrade_table(ctx, table, primary_key, updated_columns)
        if replica_tables := ctx.schema.replica_tables("DiaSource"):
            for table in replica_tables:
                if table.endswith("2"):
                    primary_key = ("apdb_replica_chunk", "apdb_replica_subchunk", "diaSourceId")
                else:
                    primary_key = ("apdb_replica_chunk", "diaSourceId")
                _upgrade_table(ctx, table, primary_key, updated_columns)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        _LOG.info("Downgrading to %s is no-op", down_revision)


def _upgrade_table(
    ctx: Context, table_name: str, primary_key: tuple[str, ...], columns: tuple[str, ...]
) -> None:
    """Upgrade a single table."""
    # Cassandra does not support NOT NULL constraint, but we need to fill NULLs
    # in the affected columns with zeroes. The only way to do it is to do whole
    # table scan and find rows with NULLs and do UPDATE/INSERT for those rows.
    pk_len = len(primary_key)

    _LOG.debug("Scanning table %s", table_name)
    select_columns = ctx.qoute_ids(primary_key + columns)
    select_columns_str = ", ".join(select_columns)
    select = f'SELECT {select_columns_str} FROM "{ctx.keyspace}"."{table_name}" ALLOW FILTERING'
    result = ctx.query(select, timeout=None)

    # Collect all PKs by column name for which column value is NULL.
    null_pk_by_column: dict[str, set[tuple]] = defaultdict(set)
    count = 0
    for row in result:
        count += 1
        pk_value = row[:pk_len]
        for col_name, col_value in zip(columns, row[pk_len:], strict=True):
            if col_value is None:
                null_pk_by_column[col_name].add(pk_value)
    _LOG.debug("Scanned %d rows in table %s", count, table_name)
    _LOG.debug(
        "  Counts of NULLs by columns: %s", {column: len(pks) for column, pks in null_pk_by_column.items()}
    )

    if null_pk_by_column:
        all_pks = set.union(*null_pk_by_column.values())
        _LOG.info("Will update %d records in table %s", len(all_pks), table_name)

    # Update NULLs trying also to minimize the number of updates:
    # 1. First find a set of PKs for which all columns are NULL and update all
    #    columns in one query (per PK).
    # 2. Remove that set of PK from each column's PKs.
    # 3. If there is a column with zero PKs then remove it from the dict and
    #    go to 1.
    # 4. Otherwise find a column with shortest list if PKs and update that
    #    column only and remove from the dict and go to 1.
    while null_pk_by_column:
        # Find PKs for which all columns are NULL.
        common_pks: set[tuple] | None = None
        for column_pks in null_pk_by_column.values():
            if common_pks is None:
                common_pks = set(column_pks)
            else:
                common_pks &= column_pks
        assert common_pks is not None
        _LOG.debug("Found %d common PKs", len(common_pks))

        if common_pks:
            insert_columns = ctx.qoute_ids(primary_key) + ctx.qoute_ids(null_pk_by_column)
            insert_columns_str = ", ".join(insert_columns)
            placeholders = ", ".join(["?"] * len(insert_columns))
            stmt = ctx.session.prepare(
                f'INSERT INTO "{ctx.keyspace}"."{table_name}" ({insert_columns_str}) VALUES ({placeholders})'
            )

            column_values = (0,) * len(null_pk_by_column)
            # Do it by chunks and sorting PKs may help.
            for pk_chunk in chunk_iterable(sorted(common_pks), 10_000):
                batch = cassandra.query.BatchStatement()
                for pk in pk_chunk:
                    values = pk + column_values
                    batch.add(stmt, values)
                ctx.update(batch)

            columns_to_drop = []
            for column, column_pks in null_pk_by_column.items():
                column_pks -= common_pks
                if not column_pks:
                    columns_to_drop.append(column)
            for column in columns_to_drop:
                del null_pk_by_column[column]
            if columns_to_drop:
                _LOG.debug("Columns %s are done", columns_to_drop)
                # Restart with smaller set of columns
                continue

        else:
            # Select a column with shortest pk list.
            column = sorted((len(pks), column) for column, pks in null_pk_by_column.items())[0][1]
            _LOG.debug("Updating column %s", column)

            insert_columns = ctx.qoute_ids(primary_key) + ctx.qoute_ids([column])
            insert_columns_str = ", ".join(insert_columns)
            placeholders = ", ".join(["?"] * len(insert_columns))
            stmt = ctx.session.prepare(
                f'INSERT INTO "{ctx.keyspace}"."{table_name}" ({insert_columns_str}) VALUES ({placeholders})'
            )

            for pk_chunk in chunk_iterable(sorted(null_pk_by_column[column]), 10_000):
                batch = cassandra.query.BatchStatement()
                for pk in pk_chunk:
                    values = pk + (0,)
                    batch.add(stmt, values)
                ctx.update(batch)

            del null_pk_by_column[column]
            _LOG.debug("Column %s is done", column)
