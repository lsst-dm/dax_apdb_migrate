"""Migration script for schema 2.0.0.

Revision ID: schema_2.0.0
Revises: schema_1.1.0
Create Date: 2025-04-02 10:25:45.596548
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_2.0.0"
down_revision = "schema_1.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 1.1.0 to 2.0.0 (ticket DM-44620).

    Summary of changes:

      - Drop x/y columns from DiaForcedSource table
      - Add ra/dec columns to DiaForcedSource table
      - Populate new ra/dec columns from their matching DiaObject values.
    """
    with Context(revision) as ctx:  # noqa: F841
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")
