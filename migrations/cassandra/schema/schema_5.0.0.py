"""Migration script for schema 5.0.0.

Revision ID: schema_5.0.0
Revises: schema_4.0.0
Create Date: 2025-04-02 10:26:08.489268
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_5.0.0"
down_revision = "schema_4.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 4.0.0 to 5.0.0 (ticket DM-48437).

    Summary of changes:
      - Add empty is_negative column.
    """
    with Context(revision) as ctx:  # noqa: F841
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")
