"""Migration script for schema 7.0.1.

Revision ID: schema_7.0.1
Revises: schema_7.0.0
Create Date: 2025-07-28 10:49:03.828289
"""

import logging

from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_7.0.1"
down_revision = "schema_7.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 7.0.0 to 7.0.1 (ticket DM-51985).

    Summary of changes:

      - MPCORB table schema updated. As we do not have MPCORB table in any
        existing APDB instances, this migration does not need to do anything.
    """
    with Context(revision) as ctx:  # noqa: F841
        _LOG.info("No changes necessary.")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        _LOG.info("No changes necessary.")
