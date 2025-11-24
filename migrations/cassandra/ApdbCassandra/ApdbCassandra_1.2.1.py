"""Migration script for ApdbCassandra 1.2.1.

Revision ID: ApdbCassandra_1.2.1
Revises: ApdbCassandra_1.2.0
Create Date: 2025-11-24 10:19:05.979758
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbCassandra_1.2.1"
down_revision = "ApdbCassandra_1.2.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'ApdbCassandra' tree from 1.2.0 to 1.2.1 (ticket DM-53385).

    Summary of changes:
      - Drop table SSObject.
    """
    with Context(revision) as ctx:
        _LOG.info("Dropping table SSObject")
        query = f'DROP TABLE "{ctx.keyspace}"."SSObject"'
        ctx.update(query)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    # I do not want to read schema to re-create all columns. As this table
    # is not actually used by anything I create a dummy table with a single
    # column.
    with Context(down_revision) as ctx:
        _LOG.info("Creating table SSObject")
        query = (
            f'CREATE TABLE "{ctx.keyspace}"."SSObject" '
            "(dummy_migration int, PRIMARY KEY (dummy_migration))"
        )
        ctx.update(query)
