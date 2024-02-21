"""Migration script for schema 0.1.1.

Revision ID: schema_0.1.1
Revises: schema_0.1.0
Create Date: 2023-11-15 22:38:14.292151
"""

import logging

import sqlalchemy
from alembic import context, op
from lsst.dax.apdb_migrate.sql.apdb_metadata import ApdbMetadata
from sqlalchemy.schema import Column

# revision identifiers, used by Alembic.
revision = "schema_0.1.1"
down_revision = "schema_0.1.0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade 'schema' tree from 0.1.0 to 0.1.1.

    Summary of changes:

      - Add table `metadata` with columns `name` and `value`
      - Add two records to that table:
        - "version:schema": "0.1.1"
        - "version:ApdbSql": "0.1.0", this value can be changes with the
          command line option, e.g. `--options apdb_sql_version=1.1.1`; this
          must correspond to what is currently in `alembic_version` table.
    """
    config = context.config
    apdb_sql_version = config.get_section_option("dax_apdb_migrate_options", "apdb_sql_version")
    if apdb_sql_version is None:
        apdb_sql_version = "0.1.0"
    else:
        logging.info("Will use version %s for ApdbSql.", apdb_sql_version)

    # When we use schemas in postgres then all tables belong to the same schema
    # so we can use alembic's version_table_schema to see where everything goes
    mig_context = context.get_context()
    schema = mig_context.version_table_schema

    # Create metadata table, should fail if for some reason it already exists.
    op.create_table(
        "metadata",
        Column("name", sqlalchemy.Text, primary_key=True),
        Column("value", sqlalchemy.Text),
        schema=schema,
    )

    metadata = ApdbMetadata(op.get_bind(), schema)
    metadata.update_tree_version("schema", "0.1.1", insert=True)
    metadata.update_tree_version("ApdbSql", apdb_sql_version, insert=True)


def downgrade() -> None:
    """Not implemented, there is no reason to undo this migration."""
    raise NotImplementedError()
