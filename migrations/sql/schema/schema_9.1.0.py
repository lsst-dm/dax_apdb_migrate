"""Migration script for schema 9.1.0.

Revision ID: schema_9.1.0
Revises: schema_9.0.0
Create Date: 2025-10-15 10:33:46.492125
"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_9.1.0"
down_revision = "schema_9.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 9.0.0 to 9.1.0 (ticket DM-52827).

    Summary of changes:

        - Add `validityStartMjdTai` column to `DiaObjectLast` table, if that
          table exists.
        - Populate new column with data from `DiaObject` table.
    """
    with Context(revision) as ctx:
        table_name = "DiaObjectLast"
        column_name = "validityStartMjdTai"

        try:
            table = ctx.get_table(table_name)
        except sqlalchemy.exc.NoSuchTableError:
            # Table does not exist, nothing to do.
            _LOG.info("Table %s is not in the schema, nothing to do.", table_name)
            return

        with ctx.batch_alter_table(table_name, copy_from=table) as batch_op:
            # Add a column, initially NULL.
            _LOG.info("Adding %s column to %s table", column_name, table_name)
            batch_op.add_column(sqlalchemy.Column(column_name, sqlalchemy.types.Double, nullable=True))

        _populate(ctx)

        table = ctx.get_table(table_name, reload=True)
        with ctx.batch_alter_table(table_name, copy_from=table) as batch_op:
            # Set it as NOT NULL.
            _LOG.info("Set column %s NOT NULL", column_name)
            batch_op.alter_column(column_name, nullable=False)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        table_name = "DiaObjectLast"
        column_name = "validityStartMjdTai"

        try:
            table = ctx.get_table(table_name)
        except sqlalchemy.exc.NoSuchTableError:
            # Table does not exist, nothing to do.
            _LOG.info("Table %s is not in the schema, nothing to do.", table_name)
            return

        table = ctx.get_table(table_name)
        with ctx.batch_alter_table(table_name, copy_from=table) as batch_op:
            _LOG.info("Dropping %s column from %s table", column_name, table_name)
            batch_op.drop_column(column_name)


def _populate(ctx: Context) -> None:
    """Populate DiaObjectLast.validityStartMjdTai from DiaObject table as a
    max value of the matching column and id.
    """
    # Update query uses correlated sub-query:
    #
    # update "DiaObjectLast" set "validityStartMjdTai" = (
    #     select MAX("validityStartMjdTai")
    #     from "DiaObject"
    #     where "DiaObject"."diaObjectId" = "DiaObjectLast"."diaObjectId"
    #     group by "DiaObject"."diaObjectId"
    # )

    dia_object = ctx.get_table("DiaObject")
    # Need to reload schema as there is a new column.
    dia_object_last = ctx.get_table("DiaObjectLast", reload=True)
    subquery = (
        sqlalchemy.select(sqlalchemy.sql.func.max(dia_object.columns["validityStartMjdTai"]))
        .select_from(dia_object)
        .where(dia_object.columns["diaObjectId"] == dia_object_last.columns["diaObjectId"])
        .group_by(dia_object.columns["diaObjectId"])
    )
    update = dia_object_last.update().values(validityStartMjdTai=subquery.scalar_subquery())
    _LOG.info("Populating new column with data from DiaObject table.")
    result = ctx.bind.execute(update)
    _LOG.info("Updated %d records in DiaObjectLast table.", result.rowcount)
