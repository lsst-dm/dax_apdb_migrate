"""Migration script for ApdbCassandra 1.0.0.

Revision ID: ApdbCassandra_1.0.0
Revises: ApdbCassandra_0.1.3
Create Date: 2025-08-13 20:09:30.583159
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbCassandra_1.0.0"
down_revision = "ApdbCassandra_0.1.3"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'ApdbCassandra' tree from 0.1.2 to 1.0.0 (ticket DM-52186).

    Summary of changes:

        - Only version is changed, but this migration depends on`schema_8.0.0`
          and here we check that `schema` is at that version.

    Note that it may be possible to use `depends_on` for this purpose, but
    creates a confusing migration tree, see alembic docs for details.
    """
    with Context(revision) as ctx:
        schema_version = ctx.metadata.get("version:schema")
        if schema_version is None:
            raise LookupError("Cannot find 'version:schema' in metadata.")
        major_version = int(schema_version.split(".")[0])
        if major_version < 8:
            raise ValueError(
                f"Schema version needs to be upgraded to 8.0.0, current version: {schema_version}"
            )


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision):
        pass
