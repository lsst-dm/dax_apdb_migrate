"""Migration script for ApdbCassandra 0.1.2.

Revision ID: ApdbCassandra_0.1.2
Revises: ApdbCassandra_0.1.1
Create Date: 2025-06-09 11:56:11.672135
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbCassandra_0.1.2"
down_revision = "ApdbCassandra_0.1.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'ApdbCassandra' tree from 0.1.1 to 0.1.2 (ticket DM-51262).

    Summary of changes:
      - Add table ApdbVisitDetector.
      - No data is inserted initially into this table.
    """
    with Context(revision) as ctx:
        query = (
            f'CREATE TABLE "{ctx.keyspace}"."ApdbVisitDetector" '
            "(visit bigint, detector smallint, PRIMARY KEY ((visit, detector))) "
            "WITH default_time_to_live=86400 AND gc_grace_seconds=86400"
        )
        ctx.update(query)
        _LOG.info("Created ApdbVisitDetector table")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        query = f'DROP TABLE "{ctx.keyspace}"."ApdbVisitDetector"'
        ctx.update(query)
        _LOG.info("Dropped ApdbVisitDetector table")
