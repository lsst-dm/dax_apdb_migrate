"""Migration script for ApdbCassandra 0.1.1.

Revision ID: ApdbCassandra_0.1.1
Revises: ApdbCassandra_0.1.0
Create Date: 2025-04-02 10:17:16.450959
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

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
    with Context(revision) as ctx:  # noqa: F841
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")
