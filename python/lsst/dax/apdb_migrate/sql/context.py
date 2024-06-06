# This file is part of dax_apdb_migrate.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("Context",)

import contextlib
from collections.abc import Iterator
from typing import Any

import alembic
import alembic.operations
import sqlalchemy

from .apdb_metadata import ApdbMetadata


class Context:
    """Provides access to commonly-needed objects derived from the alembic
    migration context.
    """

    def __init__(self) -> None:
        # Alembic migration context for the DB being migrated.
        self.mig_context = alembic.context.get_context()
        # When we use schemas in postgres then all tables belong to the same
        # schema so we can use alembic's version_table_schema to see where
        # everything goes.
        self.schema = self.mig_context.version_table_schema
        bind = self.mig_context.bind
        assert bind is not None, "Can't run offline -- need access to database to migrate data."
        # A SQLAlchemy connection for the database being migrated.
        self.bind = bind
        # SQLAlchemy dialect for the database being migrated.
        self.dialect = self.bind.dialect.name
        # True if the database being migrated is SQLite.
        self.is_sqlite = self.dialect == "sqlite"
        # True if the database being migrated is PostgreSQL.
        self.is_postgres = self.dialect == "postgresql"
        # SQLAlchemy MetaData object for the DB being migrated.
        self.metadata = sqlalchemy.schema.MetaData(schema=self.schema)
        # APDB metadata interface.
        self.apdb_meta = ApdbMetadata(self.bind, self.schema)

    def get_table(self, table_name: str, reload: bool = False) -> sqlalchemy.Table:
        """Return SQLAlchemy table object for the current database.

        Parameters
        ----------
        table_name : `str`
            Name of the table.

        Returns
        -------
        table : `sqlalchemy.Table`
            Table object.
        """
        if reload:
            for table in self.metadata.tables.values():
                if table.name == table_name:
                    self.metadata.remove(table)
                    break
        with self._reflection_bind() as bind:
            return sqlalchemy.schema.Table(table_name, self.metadata, autoload_with=bind, schema=self.schema)

    def get_mig_option(self, option: str) -> str | None:
        """Retrieve option that was passed on the command line.

        Options are passed to migration script using
        `--options option_name=option_value` syntax, multiple `--options` can
        be specified.

        Parameters
        ----------
        option : `str`
            The name of the option.

        Returns
        -------
        option_value : `str` or `None`
            Option value or `None` if option was not provided.
        """
        assert self.mig_context.config is not None
        return self.mig_context.config.get_section_option("dax_apdb_migrate_options", option)

    def batch_alter_table(
        self, table: str, **kwargs: Any
    ) -> contextlib.AbstractContextManager[alembic.operations.BatchOperations]:
        """Context manager for batch operations.

        This is a shortcut for alembic method, is main purpose is not to forget
        to pass schema name.
        """
        return alembic.op.batch_alter_table(table, schema=self.schema, **kwargs)

    @contextlib.contextmanager
    def _reflection_bind(self) -> Iterator[sqlalchemy.engine.Connection]:
        """Return database connection to be used for reflection. In online mode
        this returns connection instantiated by Alembic, in offline mode it
        creates new engine using configured URL.

        Yields
        ------
        connection : `sqlalchemy.engine.Connection`
            Actual connection to database to use for reflection.
        """
        if alembic.context.is_offline_mode():
            assert self.mig_context.config is not None
            url = self.mig_context.config.get_main_option("sqlalchemy.url")
            if url is None:
                raise ValueError("sqlalchemy.url is missing from config")
            engine = sqlalchemy.create_engine(url)
            with engine.connect() as connection:
                yield connection
        else:
            yield self.bind
