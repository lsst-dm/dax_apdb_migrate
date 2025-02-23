"""Migration script for schema 3.0.0.

Revision ID: schema_3.0.0
Revises: schema_2.0.1
Create Date: 2024-12-19 14:35:42.740977

"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_3.0.0"
down_revision = "schema_2.0.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 2.0.1 to 3.0.0 (ticket DM-48106).
    Summary of changes:
      - Add empty dipoleFitAttempted column.
    """
    ctx = Context()

    # Alter table schema.
    _LOG.info("Adding column to DiaSource table.")
    try:
        summary = ctx.get_table("DiaSource")
        with ctx.batch_alter_table("DiaSource", copy_from=summary) as batch_op:
            # add empty column.
            batch_op.add_column(
                sqlalchemy.Column("dipoleFitAttempted", sqlalchemy.types.Boolean, nullable=True)
            )
    except sqlalchemy.exc.NoSuchTableError:
        pass
    # Update metadata version.
    tree, _, version = revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)


def downgrade() -> None:
    """Downgrade 'schema' tree from 3.0.0 to 2.0.1 (ticket DM-48106).
    Summary of changes:
      - Remove empty dipoleFitAttempted column.
    """
    ctx = Context()

    # Alter table schema.
    _LOG.info("Dropping column to DiaSource table.")
    try:
        summary = ctx.get_table("DiaSource")
        with ctx.batch_alter_table("DiaSource", copy_from=summary) as batch_op:
            # drop pixelScale column.
            batch_op.drop_column("dipoleFitAttempted")
    except sqlalchemy.exc.NoSuchTableError:
        pass
    # Update metadata version.
    tree, _, version = down_revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)
