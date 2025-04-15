"""Migration script for ApdbCassandraReplica 1.1.0.

Revision ID: ApdbCassandraReplica_1.1.0
Revises: ApdbCassandraReplica_1.0.0
Create Date: 2025-04-14 16:19:00.653340
"""

import logging
from typing import Any, NamedTuple

from lsst.dax.apdb_migrate.cassandra.context import Context, _Context

# revision identifiers, used by Alembic.
revision = "ApdbCassandraReplica_1.1.0"
down_revision = "ApdbCassandraReplica_1.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


class _Column(NamedTuple):
    column_name: str
    type: str
    kind: str
    position: int
    clustering_order: str | None


def upgrade() -> None:
    """Upgrade 'ApdbCassandraReplica' tree from 1.0.0 to 1.1.0 (ticket
    DM-50093).

    Summary of changes:

      - New set of replica chunk tables is added with an additional
        partitioning key `apdb_replica_subchunk`. Names of the tables look like
        "DiaObjectsChunks2".
      - ApdbReplicaChunks table adds a boolean column `has_subchunks`.
      - DiaSourceToPartition table adds int column `apdb_replica_subchunk`.
    """
    with Context(revision) as ctx:
        table = "ApdbReplicaChunks"
        column = "has_subchunks"
        _LOG.info("Adding column %s to table %s", column, table)
        query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD "{column}" BOOLEAN'
        ctx.update(query)

        table = "DiaSourceToPartition"
        column = "apdb_replica_subchunk"
        _LOG.info("Adding column %s to table %s", column, table)
        query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD "{column}" INT'
        ctx.update(query)

        _make_new_chunks_table(ctx, "DiaObjectChunks")
        _make_new_chunks_table(ctx, "DiaSourceChunks")
        _make_new_chunks_table(ctx, "DiaForcedSourceChunks")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    # Undoing would need merging contents of two chunk tables (if old tables
    # exist), will implement it later if really needed.
    raise NotImplementedError()


def _make_new_chunks_table(ctx: _Context, table_name: str) -> None:
    """Create new replica chunk table based on the existing table schema.

    New table adds `apdb_replica_subchunk` column which also becomes a part of
    partitiong key. The rest of the columns and table options are copied from
    the existing table.
    """
    # Get the list of columns from existing table.
    query = (
        "select column_name, type, kind, position, clustering_order "
        "from system_schema.columns "
        "where keyspace_name = %s and table_name=%s"
    )
    columns: list[_Column] = list(ctx.query(query, (ctx.keyspace, table_name)))

    # Make a list of new partitioning columns, we expect that old table must
    # have exactly one partitioning column (apdb_replica_chunk).
    part_columns = [column.column_name for column in columns if column.kind == "partition_key"]
    if len(part_columns) != 1:
        raise TypeError(f"Unexpected number of columns in partition key: {part_columns}")
    new_column = _Column(
        column_name="apdb_replica_subchunk",
        type="int",
        kind="partition_key",
        position=99,
        clustering_order=None,
    )
    part_columns.append(new_column.column_name)
    columns.append(new_column)

    # Make an ordered list of clustering columns.
    clustering_columns = [column for column in columns if column.kind == "clustering"]
    clustering_columns.sort(key=lambda col: col.position)

    # We do not expect DESC order on clustering column. Potentially we could
    # add CLUSTERING_ORDER option but it's unlikely we need it.
    if any(column.clustering_order != "asc" for column in clustering_columns):
        raise TypeError("Non-ascending clustering order is not supported")

    # Make new primary key.
    part_columns = [f'"{name}"' for name in part_columns]
    part_key = f"({', '.join(part_columns)})"
    pk_list = [part_key] + [f'"{column.column_name}"' for column in clustering_columns]
    pk = f"PRIMARY KEY ({', '.join(pk_list)})"

    def _column_sort_key(column: _Column) -> tuple:
        """Define colum ordering , partitoning columns first, then clustering,
        then all the rest.
        """
        key = []
        if column.kind == "partition_key":
            key.append(0)
            key.append(column.position)
        elif column.kind == "clustering":
            key.append(1)
            key.append(column.position)
        else:
            key.append(2)
            key.append(column.column_name)
        return tuple(key)

    # Sort all columns.
    columns.sort(key=_column_sort_key)

    # Generate table DDL.
    table_column_defs = [f'"{column.column_name}" {column.type}' for column in columns]
    table_column_defs.append(pk)
    column_defs = ",\n    ".join(table_column_defs)

    new_table_name = f"{table_name}2"
    table_def = f'create table "{ctx.keyspace}"."{new_table_name}" (\n    {column_defs}\n)'

    # Also copy applicable table options.
    query = "select * from system_schema.tables where keyspace_name = %s and table_name = %s"
    rows = list(ctx.query(query, (ctx.keyspace, table_name)))
    options_tuple = rows[0]

    def _quote(value: Any) -> str:
        """Render values in options declaration."""
        if isinstance(value, (str, int, float)):
            return repr(value)
        return str(value)

    # Names of options we want to copy.
    option_names = (
        "additional_write_policy",
        "allow_auto_snapshot",
        "bloom_filter_fp_chance",
        "caching",
        "cdc",
        "comment",
        "compaction",
        "compression",
        "crc_check_chance",
        "default_time_to_live",
        "extensions",
        "gc_grace_seconds",
        "incremental_backups",
        "max_index_interval",
        "memtable_flush_period_in_ms",
        "min_index_interval",
        "read_repair",
        "speculative_retry",
    )

    options = []
    for option_name in option_names:
        option_value = getattr(options_tuple, option_name, None)
        if option_value is not None:
            options.append(f"{option_name} = {_quote(option_value)}")

    options_str = "\n    AND ".join(options)
    table_def = f"{table_def} with {options_str}"

    _LOG.info("Creating table %s", new_table_name)
    ctx.update(table_def)
