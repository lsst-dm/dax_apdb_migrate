"""Migration script for schema 9.0.0.

Revision ID: schema_9.0.0
Revises: schema_8.0.0
Create Date: 2025-08-27 14:38:32.139347
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_9.0.0"
down_revision = "schema_8.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 8.0.0 to 9.0.0 (ticket DM-52287).

    Summary of changes:
      - Timestamp columns' type changed from native timestamp to MJD TAI.
      - Columns have been renamed to have 'MjdTai' suffix.

    NOTE: Schema upgrade for cassandra would require rewriting huge amount of
    data and cannot be done without additional tools like ``dsbulk``. For now
    the only way to use schema 9.0.0 with Cassandra is to create an empty
    database with that schema version.
    """
    with Context(revision):
        raise NotImplementedError("Schema upgrade from 8.0.0 to 9.0.0 is not implemented for Cassandra.")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision):
        raise NotImplementedError("Schema вщцтgrade from 9.0.0 to 8.0.0 is not implemented for Cassandra.")
