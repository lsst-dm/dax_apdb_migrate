"""Migration script for schema 0.1.1.

Revision ID: schema_0.1.1
Revises: schema_0.1.0
Create Date: 2025-04-02 10:25:27.202771
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_0.1.1"
down_revision = "schema_0.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 0.1.0 to 0.1.1.

    Summary of changes:
      - Add table `metadata` with columns `name` and `value`
      - Fill `metadata` table with relevant data.
    """
    with Context(revision) as ctx:  # noqa: F841
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")
