"""Migration script for schema 1.1.0.

Revision ID: schema_1.1.0
Revises: schema_1.0.0
Create Date: 2025-04-02 10:25:41.974054
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_1.1.0"
down_revision = "schema_1.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 1.0.0 to 1.1.0 (ticket DM-44092).

    Summary of changes:

      - Remove columns {b}_lcPeriodic and {b}_lcNonPeriodic from DiaObject
        table ({b} is one of six bands {ugrizy}).
    """
    with Context(revision) as ctx:  # noqa: F841
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")
