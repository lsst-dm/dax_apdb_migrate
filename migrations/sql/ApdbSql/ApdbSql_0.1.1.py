"""Migration script for ApdbSql 0.1.1.

Revision ID: ApdbSql_0.1.1
Revises: ApdbSql_0.1.0
Create Date: 2024-07-29 11:02:45.959048

"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbSql_0.1.1"
down_revision = "ApdbSql_0.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Change all TIMESTAMP columns to TIMESTAMP WITH TIMEZONE."""
    version = revision.split("_")[1]
    _migrate(sqlalchemy.types.TIMESTAMP(timezone=True), version)


def downgrade() -> None:
    """Change all TIMESTAMP WITH TIMEZONE columns to TIMESTAMP."""
    version = down_revision.split("_")[1]
    _migrate(sqlalchemy.types.TIMESTAMP(timezone=False), version)


def _migrate(type_: type, to_version: str) -> None:
    ctx = Context()

    # SQLite has no TIMEZONE, this migration is not needed.
    if not ctx.is_sqlite:
        with ctx.reflection_bind() as bind:
            inspector = sqlalchemy.inspect(bind)
            tables = inspector.get_table_names(schema=ctx.schema)
            _LOG.info("All table names: %s", tables)

            table_columns = {}
            for table in tables:
                columns_to_update = []
                for column in inspector.get_columns(table, schema=ctx.schema):
                    if isinstance(column["type"], sqlalchemy.types.TIMESTAMP):
                        columns_to_update.append(column["name"])
                if columns_to_update:
                    table_columns[table] = columns_to_update

        for table, columns in table_columns.items():
            _LOG.info("Updating table %s, columns: %s", table, columns)
            with ctx.batch_alter_table(table) as batch_op:
                for column_name in columns:
                    batch_op.alter_column(column_name, type_=type_)

    # Update metadata version.
    ctx.apdb_meta.update_tree_version("ApdbSql", to_version)
