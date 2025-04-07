"""Migration script for ApdbCassandra 0.1.1.

Revision ID: ApdbCassandra_0.1.1
Revises: ApdbCassandra_0.1.0
Create Date: 2025-04-02 10:17:16.450959
"""

import logging
from collections import defaultdict

import cassandra.query
from lsst.dax.apdb_migrate.cassandra.context import Context
from lsst.utils.iteration import chunk_iterable

# revision identifiers, used by Alembic.
revision = "ApdbCassandra_0.1.1"
down_revision = "ApdbCassandra_0.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'ApdbCassandra' tree from 0.1.0 to 0.1.1 (ticket DM-45646).

    Summary of changes:
      - Add table DiaObjectLastToPartition.
      - Fill that new table with the data from DiaObjectLast table.
    """
    with Context(revision) as ctx:
        query = (
            f'CREATE TABLE "{ctx.keyspace}"."DiaObjectLastToPartition" '
            '("diaObjectId" bigint, "apdb_part" bigint, PRIMARY KEY ("diaObjectId"))'
        )
        ctx.update(query)
        _LOG.info("Created DiaObjectLastToPartition table")

        # Populate it from contents of DiaObjectLast, and also cleanup
        # duplicates in DiaObjectLast.
        query = (
            'SELECT "diaObjectId", "apdb_part", "lastNonForcedSource" '
            f'FROM "{ctx.keyspace}"."DiaObjectLast" ALLOW FILTERING'
        )
        result = ctx.query(query)
        # Group results by objectId.
        obj_id_map = defaultdict(list)
        for obj_id, apdb_part, lastTime in result:
            obj_id_map[obj_id].append((lastTime, apdb_part))

        obj_id_partitions = []
        to_drop = []
        for obj_id, partitions in obj_id_map.items():
            if len(partitions) > 1:
                # Sort by time and keep the latest.
                partitions.sort()
                obj_id_partitions.append((obj_id, partitions[-1][1]))
                del partitions[-1]
                for _, part in partitions[-1]:
                    to_drop.append((part, obj_id))
            else:
                obj_id_partitions.append((obj_id, partitions[0][1]))

        if to_drop:
            _LOG.info("Will remove %d rows from DiaObjectLast table", len(to_drop))
        if obj_id_partitions:
            _LOG.info("Will insert %d rows into DiaObjectLastToPartition table", len(obj_id_partitions))
        else:
            return

        if ctx.dry_run:
            _LOG.info("Skipping updates due to dry-run.")
            return

        stmt = ctx.session.prepare(
            f'INSERT INTO "{ctx.keyspace}"."DiaObjectLastToPartition" '
            '("diaObjectId", "apdb_part") VALUES (?, ?)'
        )

        batches = []
        for rec_chunk in chunk_iterable(obj_id_partitions, 50_000):
            batch = cassandra.query.BatchStatement()
            for values in rec_chunk:
                batch.add(stmt, values)
            batches.append(batch)

        _LOG.info("Executing batch insert for DiaObjectLastToPartition.")
        for batch in batches:
            ctx.update(batch)

        if to_drop:
            stmt = ctx.session.prepare(
                f'DELETE FROM "{ctx.keyspace}"."DiaObjectLast" WHERE "apdb_part" = ? AND "diaObjectId"= ?'
            )

            batches = []
            for rec_chunk in chunk_iterable(to_drop, 50_000):
                batch = cassandra.query.BatchStatement()
                for values in rec_chunk:
                    batch.add(stmt, values)
                batches.append(batch)

            _LOG.info("Executing batch delete from DiaObjectLast.")
            for batch in batches:
                ctx.update(batch)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        query = f'DROP TABLE "{ctx.keyspace}"."DiaObjectLastToPartition"'
        ctx.update(query)
        _LOG.info("Dropped DiaObjectLastToPartition table")
