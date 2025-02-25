"""Migration script for schema 6.0.0.

Revision ID: schema_6.0.0
Revises: schema_6.0.0
Create Date: 2025-02-18 12:06:42.740977

"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_6.0.0"
down_revision = "schema_5.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 5.0.0 to 6.0.0 (ticket DM-45621).
    Summary of changes:
      - Add empty pixelFlags_nodata and pixelFlags_nodataCenter columns.
    """
    ctx = Context()

    # Alter table schema.
    _LOG.info("Adding columns to DiaSource table.")
    try:
        summary = ctx.get_table("DiaSource")
        with ctx.batch_alter_table("DiaSource", copy_from=summary) as batch_op:
            # add empty column.
            batch_op.add_column(
                sqlalchemy.Column("pixelFlags_nodata", sqlalchemy.types.Boolean, nullable=True)
            )
            batch_op.add_column(
                sqlalchemy.Column("pixelFlags_nodataCenter", sqlalchemy.types.Boolean, nullable=True)
            )
    except sqlalchemy.exc.NoSuchTableError:
        pass
    # Update metadata version.
    tree, _, version = revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)


def downgrade() -> None:
    """Downgrade 'schema' tree from 6.0.0 to 5.0.0 (ticket DM-45621).
    Summary of changes:
      - Remove pixelFlags_nodata and pixelFlags_nodataCenter columns.
    """
    ctx = Context()

    # Alter table schema.
    _LOG.info("Dropping columns from DiaSource table.")
    try:
        summary = ctx.get_table("DiaSource")
        with ctx.batch_alter_table("DiaSource", copy_from=summary) as batch_op:
            # drop pixelScale column.
            batch_op.drop_column("pixelFlags_nodata")
            batch_op.drop_column("pixelFlags_nodataCenter")
    except sqlalchemy.exc.NoSuchTableError:
        pass
    # Update metadata version.
    tree, _, version = down_revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)
