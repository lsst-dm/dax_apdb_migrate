"""Migration script for schema 5.0.0.

Revision ID: schema_5.0.0
Revises: schema_4.0.0
Create Date: 2025-01-15 12:06:42.740977

"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_5.0.0"
down_revision = "schema_4.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 4.0.0 to 5.0.0 (ticket DM-48437).
    Summary of changes:
      - Add empty is_negative column.
    """
    ctx = Context()

    # Alter table schema.
    _LOG.info("Adding column to DiaSource table.")
    try:
        summary = ctx.get_table("DiaSource")
        with ctx.batch_alter_table("DiaSource", copy_from=summary) as batch_op:
            # add empty column.
            batch_op.add_column(
                sqlalchemy.Column(
                    "is_negative", sqlalchemy.types.Boolean, nullable=True
                )
            )
    except sqlalchemy.exc.NoSuchTableError:
        pass
    # Update metadata version.
    tree, _, version = revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)


def downgrade() -> None:
    """Downgrade 'schema' tree from 5.0.0 to 4.0.0 (ticket DM-48437).
    Summary of changes:
      - Remove empty is_negative column.
    """
    ctx = Context()

    # Alter table schema.
    _LOG.info("Dropping column from DiaSource table.")
    try:
        summary = ctx.get_table("DiaSource")
        with ctx.batch_alter_table("DiaSource", copy_from=summary) as batch_op:
            # drop pixelScale column.
            batch_op.drop_column("is_negative")
    except sqlalchemy.exc.NoSuchTableError:
        pass
    # Update metadata version.
    tree, _, version = down_revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)
