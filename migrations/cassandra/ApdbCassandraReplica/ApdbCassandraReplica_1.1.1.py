"""Migration script for ApdbCassandraReplica 1.1.1.

Revision ID: ApdbCassandraReplica_1.1.1
Revises: ApdbCassandraReplica_1.1.0
Create Date: 2026-01-29 16:27:12.737739
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbCassandraReplica_1.1.1"
down_revision = "ApdbCassandraReplica_1.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

CREATE_TABLE = """
CREATE TABLE "{keyspace}"."ApdbUpdateRecordChunks" (
    apdb_replica_chunk bigint,
    apdb_replica_subchunk int,
    update_time_ns bigint,
    update_order int,
    update_unique_id uuid,
    update_payload text,
    PRIMARY KEY ((apdb_replica_chunk, apdb_replica_subchunk), update_time_ns, update_order, update_unique_id)
)"""


def upgrade() -> None:
    """Upgrade 'ApdbCassandraReplica' tree from 1.1.0 to 1.1.1 (ticket
    DM-50190).

    Summary of changes:
      - Create table ApdbUpdateRecordChunks.
    """
    with Context(revision) as ctx:
        query = CREATE_TABLE.format(keyspace=ctx.keyspace)
        ctx.update(query)
        _LOG.info("Created ApdbUpdateRecordChunks table")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        query = f'DROP TABLE "{ctx.keyspace}"."ApdbUpdateRecordChunks"'
        ctx.update(query)
        _LOG.info("Dropped ApdbUpdateRecordChunks table")
