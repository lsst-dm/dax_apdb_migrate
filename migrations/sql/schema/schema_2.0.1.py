"""Migration script for schema 2.0.1.

Revision ID: schema_2.0.1
Revises: schema_2.0.0
Create Date: 2024-12-19 13:35:12.934383

"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

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
    ctx = Context()

    # Alter table schema.
    _LOG.info("Adding column to DetectorVisitProcessingSummary table.")
    try:
        summary = ctx.get_table("DetectorVisitProcessingSummary")
        with ctx.batch_alter_table("DetectorVisitProcessingSummary", copy_from=summary) as batch_op:
            # add empty column.
            batch_op.add_column(sqlalchemy.Column("pixelScale", sqlalchemy.types.Float, nullable=True))
    # the table is not instantiated at this time, so the exception is expected
    except sqlalchemy.exc.NoSuchTableError:
        pass
    # Update metadata version.
    tree, _, version = revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)


def downgrade() -> None:
    """Downgrade 'schema' tree from 2.0.1 to 2.0.0 (ticket DM-48106).
    Summary of changes:
      - Remove empty pixel scale column.
    """
    ctx = Context()

    # Alter table schema.
    _LOG.info("Dropping column to DetectorVisitProcessingSummary table.")
    try:
        summary = ctx.get_table("DetectorVisitProcessingSummary")
        with ctx.batch_alter_table("DetectorVisitProcessingSummary", copy_from=summary) as batch_op:
            # drop pixelScale column.
            batch_op.drop_column("pixelScale")
    except sqlalchemy.exc.NoSuchTableError:
        pass
    # Update metadata version.
    tree, _, version = down_revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)
