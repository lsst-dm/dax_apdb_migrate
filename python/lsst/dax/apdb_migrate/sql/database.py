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

import logging
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import cast

import sqlalchemy
from alembic.runtime.migration import MigrationContext

from .. import revision

_LOG = logging.getLogger(__name__)


class RevisionConsistencyError(Exception):
    """Exception raised when metadata and alembic_version tables are in
    inconsistent state.
    """


class Database:
    """Class implementing methods for database access needed for migrations.

    Parameters
    ----------
    db_url : `str`
        Database URL.
    schema : `str`, optional
        Database schema/namespace.
    """

    def __init__(self, db_url: str, schema: str | None = None):
        self._db_url = sqlalchemy.engine.make_url(db_url)
        self._schema = schema

    @property
    def db_url(self) -> str:
        """URL for registry database (`str`)"""
        return str(self._db_url)

    @property
    def schema(self) -> str | None:
        """Schema (namespace) name (`str`)"""
        return self._schema

    @contextmanager
    def connect(self) -> Iterator[sqlalchemy.engine.Connection]:
        """Context manager for database connection."""
        engine = sqlalchemy.engine.create_engine(self._db_url)
        with engine.connect() as connection:
            yield connection

    def tree_versions(self) -> Mapping[str, tuple[str, str]]:
        """Retrieve current versions stored in metadata table.

        Returns
        -------
        versions : `dict` [ `str`, `tuple` ] or `None`
            Mapping whose key is a tree name and value is a tuple consisting of
            version string, and revision ID string/hash.
        """
        engine = sqlalchemy.engine.create_engine(self._db_url)

        meta = sqlalchemy.schema.MetaData(schema=self._schema)
        table = sqlalchemy.schema.Table(
            "Metadata",
            meta,
            sqlalchemy.schema.Column("name", sqlalchemy.Text),
            sqlalchemy.schema.Column("value", sqlalchemy.Text),
        )

        # Parse table contents.
        versions: dict[str, str] = {}
        sql = sqlalchemy.sql.select(table.columns.name, table.columns.value)
        with engine.connect() as connection:
            result = connection.execute(sql)
            for name, value in result:
                if name.startswith("version:"):
                    versions[name.partition(":")[-1]] = value

        revisions: dict[str, tuple[str, str]] = {}
        for tree, version in versions.items():
            # for revision ID we use tree name and version
            rev_id_str = revision.rev_id(tree, version)
            revisions[tree] = (version, rev_id_str)

        return revisions

    def alembic_revisions(self) -> list[str]:
        """Return a list of current revision numbers from database

        Returns
        -------
        revisions : `list` [ `str` ]
            Returned list is empty if alembic version table does not exist or
            is empty.
        """
        engine = sqlalchemy.engine.create_engine(self._db_url)
        with engine.connect() as connection:
            ctx = MigrationContext.configure(
                connection=connection, opts={"version_table_schema": self._schema}
            )
            return list(ctx.get_current_heads())

    def validate_revisions(self, base_revisions: Iterable[str] | None = None) -> None:
        """Verify consistency of alembic revisions and butler versions.

        Revisions in alembic table must match either a version of a tree in
        metadata or base revision (for tree that did not make yet
        into metadata).

        Parameters
        ----------
        base_revisions : `iterable` [`str`], optional
            Optional base revisions of the migration trees.

        Raises
        ------
        RevisionConsistencyError
            Raised if contents of the two tables is not consistent. Exception
            message contains details of differences.
        """
        # TODO: possible optimization to reuse a connection to database
        try:
            tree_versions = self.tree_versions()
        except sqlalchemy.exc.OperationalError:
            raise RevisionConsistencyError("metadata table does not exist")
        alembic_revisions = self.alembic_revisions()

        if tree_versions and not alembic_revisions:
            raise RevisionConsistencyError("alembic_version table does not exist or is empty")
        if alembic_revisions and not tree_versions:
            raise RevisionConsistencyError("metadata table is empty")

        alembic_revs = set(alembic_revisions)
        tree_revs: dict[str, tuple[str, str]] = {}
        for tree, (version, rev_id) in sorted(tree_versions.items()):
            tree_revs[rev_id] = (tree, version)
        manager_revs_set = set(tree_revs.keys())

        alembic_only = alembic_revs - manager_revs_set
        if base_revisions:
            alembic_only = alembic_only - set(base_revisions)
        manager_only = manager_revs_set - alembic_revs

        if alembic_only or manager_only:
            msg = "Butler and alembic revisions are inconsistent --"
            sep = ""
            if alembic_only:
                alembic_only_str = ",".join(alembic_only)
                msg += f" revisions in alembic only: {alembic_only_str}"
                sep = ";"
            if manager_only:
                msg += sep + " revisions in butler only:"
                for rev in manager_only:
                    msg += f" {rev}={tree_revs[rev]}"
            raise RevisionConsistencyError(msg)

    def dump_schema(self, tables: list[str] | None) -> None:
        """Dump the schema of the whole database.

        Parameters
        ----------
        tables: `list`, optional
            List of the tables, if missing or empty then schema for all tables
            is printed.
        """
        engine = sqlalchemy.engine.create_engine(self._db_url)
        inspector = sqlalchemy.inspect(engine)
        table_names = sorted(inspector.get_table_names(schema=self._schema))
        for table in table_names:
            if tables and table not in tables:
                continue

            print(f"table={table}")

            column_list = inspector.get_columns(table, schema=self._schema)
            column_list.sort(key=lambda c: c["name"])
            for col in column_list:
                print(
                    f"  column={col['name']} type={col['type']} nullable={col['nullable']}"
                    f" default={col['default']} [table={table}]"
                )

            pk = inspector.get_pk_constraint(table, schema=self._schema)
            if pk:
                columns = ",".join(pk["constrained_columns"])
                print(f"  PK name={pk['name']} columns=({columns}) [table={table}]")

            uniques = inspector.get_unique_constraints(table, schema=self._schema)
            uniques.sort(key=lambda uq: cast(str, uq["name"]))
            for uq in uniques:
                columns = ",".join(uq["column_names"])
                print(f"  UNIQUE name={uq['name']} columns=({columns}) [table={table}]")

            fks = inspector.get_foreign_keys(table, schema=self._schema)
            fks.sort(key=lambda fk: cast(str, fk["name"]))
            for fk in fks:
                columns = ",".join(fk["constrained_columns"])
                ref_columns = ",".join(fk["referred_columns"])
                print(
                    f"  FK name={fk['name']} ({columns}) -> {fk['referred_table']}({ref_columns})"
                    f" [table={table}]"
                )

            checks = inspector.get_check_constraints(table, schema=self._schema)
            checks.sort(key=lambda chk: cast(str, chk["name"]))
            for check in checks:
                print(f"  CHECK name={check['name']} sqltext={check['sqltext']} [table={table}]")

            indices = inspector.get_indexes(table, schema=self._schema)
            indices.sort(key=lambda idx: cast(str, idx["name"]))
            for idx in indices:
                columns = ",".join(cast(list[str], idx["column_names"]))
                print(
                    f"  INDEX name={idx['name']} columns=({columns}) unique={idx['unique']} [table={table}]"
                )
