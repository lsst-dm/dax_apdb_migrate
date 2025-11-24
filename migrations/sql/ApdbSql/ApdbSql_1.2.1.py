"""Migration script for ApdbSql 1.2.1.

Revision ID: ApdbSql_1.2.1
Revises: ApdbSql_1.2.0
Create Date: 2025-11-24 09:55:34.252065
"""

import logging

from alembic import op
from sqlalchemy import Column, Integer
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbSql_1.2.1"
down_revision = "ApdbSql_1.2.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'ApdbSql' tree from 1.2.0 to 1.2.1 (ticket DM-53385).

    Summary of changes:
      - Drop table SSObject.
    """
    with Context(revision) as ctx:
        _LOG.info("Dropping table SSObject")
        op.drop_table("SSObject", schema=ctx.schema)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    # I do not want to read schema to re-create all columns. As this table
    # is not actually used by anything I create a dummy table with a single
    # column.
    with Context(down_revision) as ctx:
        _LOG.info("Creating table SSObject")
        op.create_table(
            "SSObject",
            Column("dummy_migration", Integer, primary_key=True),
            schema=ctx.schema,
        )
