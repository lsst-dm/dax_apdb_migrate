"""Migration script for schema 1.1.0.

Revision ID: schema_1.1.0
Revises: schema_1.0.0
Create Date: 2024-05-09 11:51:35.137715
"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_1.1.0"
down_revision = "schema_1.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 1.0.0 to 1.1.0 (ticket DM-44092).

    Summary of changes:

      - Remove columns {b}_lcPeriodic and {b}_lcNonPeriodic from DiaObject
        table ({b} is one of six bands {ugrizy}).
    """
    ctx = Context()

    # In addition to DiaObject table there could be DiaObjectChunks, though it
    # is unlikely that SQL backend will ever be configured for replication.
    tables = ["DiaObject"]
    inspect = sqlalchemy.inspect(ctx.bind)
    chunks_table = "DiaObjectChunks"
    if inspect.has_table(chunks_table, schema=ctx.schema):
        _LOG.info("Found replication table %s", chunks_table)
        tables.append(chunks_table)

    for table in tables:
        with ctx.batch_alter_table(table) as batch_op:
            for column in ("lcPeriodic", "lcNonPeriodic"):
                for band in "ugrizy":
                    column_name = f"{band}_{column}"
                    _LOG.info("Dropping column %s.%s", table, column_name)
                    batch_op.drop_column(column_name)

    # Update metadata version.
    version = revision.split("_")[-1]
    ctx.apdb_meta.update_tree_version("schema", version)


def downgrade() -> None:
    """Downgrade is not needed as those columns were never used."""
    raise NotImplementedError()
