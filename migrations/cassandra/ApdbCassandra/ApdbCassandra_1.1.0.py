"""Migration script for ApdbCassandra 1.1.0.

Revision ID: ApdbCassandra_1.1.0
Revises: ApdbCassandra_1.0.0
Create Date: 2025-08-27 14:38:54.455801
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbCassandra_1.1.0"
down_revision = "ApdbCassandra_1.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'ApdbCassandra' tree from 1.0.0 to 1.1.0 (ticket DM-52287).

    Summary of changes:

        - Only the version is changed, but this migration depends on
          `schema_9.0.0` and here we check that `schema` is at that version.
    """
    with Context(revision) as ctx:
        try:
            ctx.require_revision("schema_9.0.0")
        except ValueError as exc:
            raise ValueError("Schema version needs to be upgraded to 8.0.0") from exc


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision):
        pass
