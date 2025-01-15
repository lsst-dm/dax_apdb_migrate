"""Migration script for schema 4.0.0.

Revision ID: schema_4.0.0
Revises: schema_3.0.0
Create Date: 2025-01-14 15:54:47.948297

"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_4.0.0"
down_revision = "schema_3.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 3.0.0 to 4.0.0 (ticket DM-44098).

    Summary of changes:
      - Add nDiaSources column to DiaObjectLast table, only if table exists.
    """
    ctx = Context()

    try:
        objects_last = ctx.get_table("DiaObjectLast")
    except sqlalchemy.exc.NoSuchTableError:
        objects_last = None
        _LOG.info("DiaObjectLast table does not exist, nothing to do.")

    if objects_last is not None:
        with ctx.batch_alter_table("DiaObjectLast", copy_from=objects_last) as batch_op:
            # Add empty column, make it nullable before we fill it.
            _LOG.info("Adding nDiaSources column to DiaObjectLast table.")
            batch_op.add_column(sqlalchemy.Column("nDiaSources", sqlalchemy.types.Integer, nullable=True))

        # We need to fill it with realistic counts.
        _LOG.info("Filling nDiaSources column from DiaSource counts.")
        objects_last = ctx.get_table("DiaObjectLast", reload=True)
        sources = ctx.get_table("DiaSource")
        subq = (
            sqlalchemy.select(sqlalchemy.func.count())
            .select_from(sources)
            .where(sources.columns["diaObjectId"] == objects_last.columns["diaObjectId"])
        )
        sql = objects_last.update().values(nDiaSources=subq.scalar_subquery())
        ctx.bind.execute(sql)
        # There may be some objects without sources, set nDiaSources to 0.
        sql = (
            objects_last.update()
            .values(nDiaSources=sqlalchemy.literal(0))
            .where(objects_last.columns["nDiaSources"].is_(None))
        )
        ctx.bind.execute(sql)

        _LOG.info("Making nDiaSources column non-nullable.")
        with ctx.batch_alter_table("DiaObjectLast", copy_from=objects_last) as batch_op:
            batch_op.alter_column("nDiaSources", nullable=False)

    # Update metadata version.
    tree, _, version = revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)


def downgrade() -> None:
    """Downgrade 'schema' tree from 4.0.0 to 3.0.0 (ticket DM-44098).

    Summary of changes:
      - Drop nDiaSources column from DiaObjectLast table.
    """
    ctx = Context()

    try:
        objects_last = ctx.get_table("DiaObjectLast")
    except sqlalchemy.exc.NoSuchTableError:
        objects_last = None
        _LOG.info("DiaObjectLast table does not exist, nothing to do.")

    if objects_last is not None:
        with ctx.batch_alter_table("DiaObjectLast", copy_from=objects_last) as batch_op:
            _LOG.info("Dropping nDiaSources column from DiaObjectLast table.")
            batch_op.drop_column("nDiaSources")

    # Update metadata version.
    tree, _, version = down_revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)
