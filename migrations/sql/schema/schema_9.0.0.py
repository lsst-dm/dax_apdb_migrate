"""Migration script for schema 9.0.0.

Revision ID: schema_9.0.0
Revises: schema_8.0.0
Create Date: 2025-08-25 11:38:59.590064
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any, NamedTuple, overload

import sqlalchemy
from astropy.time import Time
from lsst.dax.apdb_migrate.sql.context import Context
from lsst.utils.iteration import chunk_iterable

# revision identifiers, used by Alembic.
revision = "schema_9.0.0"
down_revision = "schema_8.0.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


class _Column(NamedTuple):
    """Column name before/after migration and mullable flag."""

    old_name: str
    new_name: str
    nullable: bool
    new_type: type = sqlalchemy.types.Double

    def downgrade(self) -> _Column:
        """Make a copy of column definition used for downgrade."""
        if self.new_type is sqlalchemy.types.Double:
            return _Column(self.new_name, self.old_name, self.nullable, sqlalchemy.types.TIMESTAMP)
        else:
            # Just in case someone applies downgrade() more than once.
            return self

    @overload
    def convert_value(self, value: None) -> None: ...

    @overload
    def convert_value(self, value: datetime) -> float: ...

    @overload
    def convert_value(self, value: float) -> datetime: ...

    def convert_value(self, value: datetime | float | None) -> datetime | float | None:
        if value is None:
            return None
        if self.new_type is sqlalchemy.types.Double:
            time = Time(value, format="datetime", scale="tai")
            return float(time.mjd)
        else:
            time = Time(value, format="mjd", scale="tai")
            # This returns naive datetiem, this is how it was done in ApdbSql.
            return time.datetime


class _Index(NamedTuple):
    """Index description."""

    name: str
    columns: tuple[str, ...]


# Column name and nullable flag.
COLUMNS: dict[str, tuple[_Column, ...]] = {
    "DiaObject": (
        _Column("validityStart", "validityStartMjdTai", False),
        _Column("validityEnd", "validityEndMjdTai", True),
    ),
    "DiaObjectChunks": (_Column("validityStart", "validityStartMjdTai", False),),
    "DiaSource": (
        _Column("ssObjectReassocTime", "ssObjectReassocTimeMjdTai", True),
        _Column("time_processed", "timeProcessedMjdTai", False),
        _Column("time_withdrawn", "timeWithdrawnMjdTai", True),
    ),
    "DiaForcedSource": (
        _Column("time_processed", "timeProcessedMjdTai", False),
        _Column("time_withdrawn", "timeWithdrawnMjdTai", True),
    ),
}

OLD_PK_COLUMNS = {
    "DiaObject": ("diaObjectId", "validityStart"),
    "DiaObjectChunks": ("diaObjectId", "validityStart"),
    "DiaSource": ("diaSourceId",),
    "DiaForcedSource": ("diaObjectId", "visit", "detector"),
}

NEW_PK_COLUMNS = {
    "DiaObject": ("diaObjectId", "validityStartMjdTai"),
    "DiaObjectChunks": ("diaObjectId", "validityStartMjdTai"),
    "DiaSource": ("diaSourceId",),
    "DiaForcedSource": ("diaObjectId", "visit", "detector"),
}

# Indices that need to be replaced.
INDICES = {
    "DiaObject": {
        _Index("IDX_DiaObject_validityStart", ("validityStart",)): _Index(
            "IDX_DiaObject_validityStartMjdTai", ("validityStartMjdTai",)
        ),
    },
}


def upgrade() -> None:
    """Upgrade 'schema' tree from 8.0.0 to 9.0.0 (ticket DM-52287).

    Summary of changes:
      - Timestamp columns' type changed from native timestamp to MJD TAI.
      - Columns have been renamed to have 'MjdTai' suffix.
      - This also means that some indices and PKs need to be recreated.

    The migration is quite slow as it updates big tables. There maybe more
    optimal way by re-calculating timestamps using SQL functions, but it is
    backend-specific and not implemented at this point.
    """
    with Context(revision) as ctx:
        _migrate_default(ctx, True)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        _migrate_default(ctx, False)


def _migrate_default(ctx: Context, upgrade: bool) -> None:
    column_type = sqlalchemy.types.Double if upgrade else sqlalchemy.types.TIMESTAMP

    for table, columns in COLUMNS.items():
        # Some tables may be missing
        try:
            ctx.get_table(table)
        except sqlalchemy.exc.NoSuchTableError:
            if table == "DiaObjectChunks":
                continue
            raise

        _LOG.info("Processing table %s", table)

        if not upgrade:
            columns = tuple(column.downgrade() for column in columns)

        # Create new columns first as nullable.
        with ctx.batch_alter_table(table) as batch_op:
            _LOG.info("Adding new columns: %s", [column.new_name for column in columns])
            for column in columns:
                batch_op.add_column(sqlalchemy.Column(column.new_name, column.new_type, nullable=True))

        # Populate new columns with data from old ones.
        if upgrade:
            pk_columns = OLD_PK_COLUMNS[table]
        else:
            pk_columns = NEW_PK_COLUMNS[table]
        _LOG.info("Populating new columns with data")
        _populate(ctx, table, columns, column_type, pk_columns, upgrade)

        # Drop existing indices and columns
        with ctx.batch_alter_table(table) as batch_op:
            # May need to drop PK as well.
            if not set(pk_columns).isdisjoint(column.old_name for column in columns):
                # Postgres drops PK when the column is dropped, but sqlite
                # complains that column cannot be dropped if it's in PK. The
                # workaround is to create PK with new columns, but Postgres
                # also requires existing PK to be dropped first, and in sqlite
                # we do not even have name for PK constraint, so it cannot be
                # dropped explicitly.
                pk_name = f"{table}_pkey"
                if ctx.is_postgres:
                    _LOG.info("Dropping %s constraint", pk_name)
                    batch_op.drop_constraint(pk_name)
                _LOG.info("Add %s constraint", pk_name)
                new_pk_columns = NEW_PK_COLUMNS[table] if upgrade else OLD_PK_COLUMNS[table]
                batch_op.create_primary_key(pk_name, list(new_pk_columns))

            # Drop indices.
            for old_index, new_index in INDICES.get(table, {}).items():
                if not upgrade:
                    old_index, new_index = new_index, old_index
                _LOG.info("Dropping index %s", old_index.name)
                batch_op.drop_index(old_index.name)
                _LOG.info("Creating index %s", new_index.name)
                batch_op.create_index(new_index.name, list(new_index.columns))

            # Drop columns.
            _LOG.info("Dropping columns %s", [column.old_name for column in columns])
            for column in columns:
                batch_op.drop_column(column.old_name)


def _populate(
    ctx: Context,
    table_name: str,
    columns: tuple[_Column, ...],
    column_type: type,
    pk_columns: tuple[str, ...],
    upgrade: bool,
) -> None:
    table = ctx.get_table(table_name, reload=True)
    pk_column_defs = [
        sqlalchemy.schema.Column(name, table.columns[name].type, primary_key=True) for name in pk_columns
    ]

    # Create a temporary table which includes PK columns from original table
    # a new column that we are going to populate.
    tmp_columns = pk_column_defs + [
        sqlalchemy.schema.Column(column.new_name, column_type) for column in columns
    ]
    tmp_table = sqlalchemy.schema.Table(
        f"_{table_name}_migration_tmp",
        ctx.metadata,
        *tmp_columns,
        prefixes=["TEMPORARY"],
        schema=sqlalchemy.schema.BLANK_SCHEMA,
    )
    tmp_table.create(ctx.bind)

    # Select all records where timestamp is not None. Hopefully SQL database
    # does not have too many records and we can fit everything into memory.
    select_columns = [table.columns[name] for name in pk_columns]
    select_columns += [table.columns[column.old_name] for column in columns]
    query = sqlalchemy.select(*select_columns).select_from(table)

    column_converters: list[Callable] = [lambda x: x] * len(pk_columns)
    column_converters += [column.convert_value for column in columns]

    def _convert_row(row: Any) -> tuple:
        return tuple(converter(value) for converter, value in zip(column_converters, row, strict=True))

    with ctx.reflection_bind() as bind:
        result = bind.execute(query)
        rows = [_convert_row(row) for row in result]

    count = 0
    for chunk in chunk_iterable(rows, 10_000):
        insert = tmp_table.insert().values(chunk)
        result = ctx.bind.execute(insert)
        count += result.rowcount
    _LOG.info("Inserted %s rows into a temporary table for %s", count, table_name)

    pk_where = sqlalchemy.and_(*(tmp_table.columns[name] == table.columns[name] for name in pk_columns))
    if count > 0:
        for column in columns:
            _LOG.info("Populating column %s", column.new_name)
            update = table.update().values(
                {
                    column.new_name: sqlalchemy.select(tmp_table.columns[column.new_name])
                    .where(pk_where)
                    .scalar_subquery()
                }
            )
            result = bind.execute(update)
