"""Migration script for schema 4.0.0.

Revision ID: schema_4.0.0
Revises: schema_3.0.0
Create Date: 2025-04-02 10:26:04.735025
"""

import logging
from collections import Counter

import cassandra.query
from lsst.dax.apdb_migrate.cassandra.context import Context
from lsst.utils.iteration import chunk_iterable

# revision identifiers, used by Alembic.
revision = "schema_4.0.0"
down_revision = "schema_3.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 3.0.0 to 4.0.0 (ticket DM-44098).

    Summary of changes:
      - Add nDiaSources column to DiaObjectLast table.
    """
    _migrate(True, revision)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    _migrate(False, down_revision)


def _migrate(add: bool, final_revision: str) -> None:
    # Do schema migrations.
    with Context(final_revision) as ctx:
        table = "DiaObjectLast"
        column = "nDiaSources"
        if add:
            _LOG.info("Adding column %s to table %s", column, table)
            query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD "{column}" INT'
        else:
            _LOG.info("Dropping column %s from table %s", column, table)
            query = f'ALTER TABLE "{ctx.keyspace}"."{table}" DROP "{column}"'
        ctx.update(query)

        if add:
            # Populate new column.
            query = (
                'SELECT apdb_part, "diaSourceId", "diaObjectId" '
                f'FROM "{ctx.keyspace}"."DiaSource" ALLOW FILTERING'
            )
            result = ctx.query(query)

            counter: Counter = Counter()
            counter.update(row[2] for row in result)
            _LOG.info("Found %s DiaObjects in DiaSources table", len(counter))

            query = f'SELECT apdb_part, "diaObjectId" FROM "{ctx.keyspace}"."DiaObjectLast"  ALLOW FILTERING'
            result = ctx.query(query)
            last_ids = sorted((row[0], row[1]) for row in result)

            _LOG.info("Found %s DiaObjects in DiaObjectLast table", len(last_ids))
            last_obj_ids = set(r[1] for r in last_ids)
            _LOG.info("Found %s unique DiaObjects in DiaObjectLast table", len(last_obj_ids))

            if ctx.dry_run:
                _LOG.info("Skipping updates due to dry-run.")
                return

            update_query = (
                f'UPDATE "{ctx.keyspace}"."DiaObjectLast" '
                'SET "nDiaSources" = ? WHERE apdb_part = ? AND "diaObjectId" = ?'
            )
            stmt = ctx.session.prepare(update_query)

            batches = []
            for rec_chunk in chunk_iterable(last_ids, 50_000):
                batch = cassandra.query.BatchStatement()
                for apdb_part, dia_obj_id in rec_chunk:
                    values = counter[dia_obj_id], apdb_part, dia_obj_id
                    batch.add(stmt, values)
                batches.append(batch)

            for batch in batches:
                _LOG.info("Executing batch update for nDiaSources.")
                ctx.update(batch)
