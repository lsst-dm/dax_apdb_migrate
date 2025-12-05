"""Migration script for schema 9.1.1.

Revision ID: schema_9.1.1
Revises: schema_9.1.0
Create Date: 2025-12-04 16:35:32.207801
"""

import logging

from alembic import op
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_9.1.1"
down_revision = "schema_9.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

_COLUMNS = {
    "DiaObject": (
        "u_psfFluxNdata",
        "g_psfFluxNdata",
        "r_psfFluxNdata",
        "i_psfFluxNdata",
        "z_psfFluxNdata",
        "y_psfFluxNdata",
    ),
    "DiaSource": (
        "psfNdata",
        "trailNdata",
        "dipoleNdata",
        "bboxSize",
    ),
}


def upgrade() -> None:
    """Upgrade 'schema' tree from 9.1.0 to 9.1.1 (ticket DM-53543).

    Summary of changes:
      - A number of integer columns are now NOT NULL.
      - The columns need to be populated with 0 if they are NULL.
    """
    with Context(revision) as ctx:
        # Replace NULLs with 0.
        for table_name, columns in _COLUMNS.items():
            table = ctx.get_table(table_name)
            for column in columns:
                _LOG.info("Update NULL values in column %s.%s", table_name, column)
                values = {column: 0}
                update = table.update().values(**values).where(table.columns[column].is_(None))
                op.execute(update)

        # Change columns to NOT-NULL.
        for table_name, columns in _COLUMNS.items():
            with ctx.batch_alter_table(table_name, copy_from=table) as batch_op:
                for column in columns:
                    _LOG.info("Make column %s.%s non-nullable", table_name, column)
                    batch_op.alter_column(column, nullable=False)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        # Change columns to nullable, keep values.
        for table_name, columns in _COLUMNS.items():
            with ctx.batch_alter_table(table_name) as batch_op:
                for column in columns:
                    _LOG.info("Make column %s.%s nullable", table_name, column)
                    batch_op.alter_column(column, nullable=True)
