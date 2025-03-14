"""Migration script for ApdbSql 0.1.2.

Revision ID: ApdbSql_0.1.2
Revises: ApdbSql_0.1.1
Create Date: 2025-03-13 11:36:11.916739

"""

import logging
from typing import Any

import sqlalchemy
import yaml
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbSql_0.1.2"
down_revision = "ApdbSql_0.1.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'ApdbSql' tree from 0.1.1 to 0.1.2 (DM-49455).

    Summary of changes:
      - Change the type of columns that are defined as `float` in felis
        to `sqlalchemy.types.REAL`.
    """
    _migrate(True, revision)


def downgrade() -> None:
    """Undo migration."""
    _migrate(False, down_revision)


def _migrate(do_upgrade: bool, to_revision: str) -> None:
    ctx = Context()

    if not ctx.is_postgres:
        _LOG.info("Non-Postgres database, nothing to do.")
        return

    # The complex part here is to know which columns to change, for that we
    # need to know felis schema and which tables exist in the database.
    # For start we need to know the schema version in the database.
    schema_version = ctx.apdb_meta.get("version:schema")
    if schema_version is None:
        raise ValueError(
            "Database metadata does not define schema version. "
            "Make sure that schema name is correct and database is initialized."
        )

    schema_file_path = ctx.get_mig_option("schema-file")
    if not schema_file_path:
        raise ValueError(
            f"This migration script requires sdm_schemas file for APDB schema version {schema_version}. "
            "Please use `--options schema-file=PATH command line option."
        )

    # Read schema YAML file and make Felis schema.
    felis_model = _load_schema_model(schema_file_path)

    # Check that versions match.
    if felis_model.version != schema_version:
        raise ValueError(
            f"Version in the sschema file {felis_model.version} "
            f"does not match database schema version {schema_version}."
        )

    # Find float columns in the schema.
    float_columns = _find_float_columns(felis_model)

    # Find matching tables in the database.
    table_columns: dict[str, list[str]] = {}
    with ctx.reflection_bind() as bind:
        inspector = sqlalchemy.inspect(bind)
        for table_name, columns in float_columns.items():
            if inspector.has_table(table_name, ctx.schema):
                _LOG.debug("found table %s", table_name)
                table_columns[table_name] = columns
            if table_name == "DiaObject":
                extra_table = f"{table_name}Last"
                if inspector.has_table(extra_table, ctx.schema):
                    # DiaObjectLast has a subset of columns of ObjectTable.
                    table_columns[extra_table] = [
                        col.name
                        for col in inspector.get_columns(extra_table, ctx.schema)
                        if col.name in columns
                    ]

    # Do actual migration.
    column_type = sqlalchemy.types.REAL if do_upgrade else sqlalchemy.types.Float
    for table_name, columns in table_columns.items():
        _LOG.info("Updating %d columns in table %s", len(columns), table_name)
        _LOG.debug("Columns: %s", columns)
        with ctx.batch_alter_table(table_name) as batch_op:
            for column in columns:
                batch_op.alter_column(column, type_=column_type)

    # Update metadata version.
    tree, _, version = to_revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)


def _load_schema_model(schema_file: str) -> Any:
    # This migration is special as it needs access to the column
    # definition. For that we need to import felis model and here
    # we make sure that felis is setup.
    try:
        import felis.datamodel
    except ImportError:
        raise ImportError(
            "This migration requires `felis` package, please setup it before running this script."
        )

    with open(schema_file) as yaml_stream:
        schemas_list = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
        if len(schemas_list) > 1:
            raise ValueError(f"Schema file {schema_file!r} defines multiple schemas.")
        felis_schema = felis.datamodel.Schema.model_validate(schemas_list[0])

    return felis_schema


def _find_float_columns(felis_model: Any) -> dict[str, list[str]]:
    """Extract list of columns that have `float` type."""
    result = {}
    for table in felis_model.tables:
        columns = [column.name for column in table.columns if column.datatype.name == "float"]
        if columns:
            result[table.name] = columns
    return result
