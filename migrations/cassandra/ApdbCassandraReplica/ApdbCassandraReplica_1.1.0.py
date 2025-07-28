"""Migration script for ApdbCassandraReplica 1.1.0.

Revision ID: ApdbCassandraReplica_1.1.0
Revises: ApdbCassandraReplica_1.0.0
Create Date: 2025-04-14 16:19:00.653340
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context
from lsst.dax.apdb_migrate.cassandra.table_schema import Column, TableSchema

# revision identifiers, used by Alembic.
revision = "ApdbCassandraReplica_1.1.0"
down_revision = "ApdbCassandraReplica_1.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


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
        # Make new tables first, this has higher chance of failure.
        _make_new_chunks_table(ctx, "DiaObjectChunks")
        _make_new_chunks_table(ctx, "DiaSourceChunks")
        _make_new_chunks_table(ctx, "DiaForcedSourceChunks")

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

        _update_frozen_config(ctx)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    # Undoing would need merging contents of two chunk tables (if old tables
    # exist), will implement it later if really needed.
    raise NotImplementedError()


def _make_new_chunks_table(ctx: Context, table_name: str) -> None:
    """Create new replica chunk table based on the existing table schema.

    New table adds `apdb_replica_subchunk` column which also becomes a part of
    partitiong key. The rest of the columns and table options are copied from
    the existing table.
    """
    table_schema = TableSchema.from_table(ctx, table_name)
    if len(table_schema.partitioning_columns) != 1:
        raise TypeError(f"Unexpected number of columns in partition key: {table_schema.partitioning_columns}")

    # New column to add to partitioning key.
    new_column = Column(
        column_name="apdb_replica_subchunk",
        type="int",
        kind="partition_key",
        position=1,
    )
    table_schema.columns.append(new_column)

    table_schema.table_name = f"{table_schema.table_name}2"
    table_ddl = table_schema.make_ddl()

    _LOG.info("Creating table %s", table_schema.table_name)
    ctx.update(table_ddl)


def _update_frozen_config(ctx: Context) -> None:
    """Update configuration stored in metadata."""
    config = ctx.get_apdb_config()
    # Use default value for this parameter.
    config["replica_sub_chunk_count"] = 64
    ctx.store_apdb_config(config)
