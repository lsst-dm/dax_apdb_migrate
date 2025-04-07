"""Migration script for schema 2.0.1.

Revision ID: schema_2.0.1
Revises: schema_2.0.0
Create Date: 2025-04-02 10:25:49.069698
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_2.0.1"
down_revision = "schema_2.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 2.0.0 to 2.0.1 (ticket DM-48106).

    Summary of changes:
      - Add empty pixel scale column.
    """
    with Context(revision) as ctx:  # noqa: F841
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")
