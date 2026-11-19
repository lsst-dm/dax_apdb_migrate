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
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import Any

import sqlalchemy
from cassandra.cluster import Session

from lsst.dax.apdb import ApdbConfig
from lsst.dax.apdb.cassandra import ApdbCassandraConfig
from lsst.dax.apdb.cassandra.sessionFactory import SessionContext

from .. import revision
from .apdb_metadata import ApdbMetadata

_LOG = logging.getLogger(__name__)


def _dump_query(rf: Any) -> None:
    """Dump cassandra query to debug log."""
    _LOG.debug("Cassandra query: %s", rf.query)


class Database:
    """Class implementing methods for database access needed for migrations.

    Parameters
    ----------
    config_uri : `str`
        Path or URI of APDB configuration file.
    """

    metadata_table_name = "metadata"
    """Name of the metadata table holding versions."""

    def __init__(self, config_uri: str):
        config = ApdbConfig.from_uri(config_uri)
        if not isinstance(config, ApdbCassandraConfig):
            raise TypeError(f"Configuration file is for non-Cassandra implementation: {config_uri}")
        if config.prefix:
            raise ValueError(
                f"Config file defines prefix={config.prefix}, "
                "but Cassandra migrations do not support non-empty table prefixes."
            )
        # Make timeouts arbitrary large in case some queries take too long.
        config.connection_config.read_timeout = 3600
        config.connection_config.write_timeout = 3600
        self._config = config

    @property
    def keyspace(self) -> str:
        """Keyspace name (`str`)."""
        return self._config.keyspace

    def tree_versions(self) -> Mapping[str, tuple[str, str]]:
        """Retrieve current versions stored in metadata table.

        Returns
        -------
        versions : `dict` [ `str`, `tuple` ] or `None`
            Mapping whose key is a tree name and value is a tuple consisting of
            version string, and revision ID string/hash.
        """
        versions = {}
        with self.make_session() as session:
            meta = ApdbMetadata(session, self.keyspace)
            for name, value in meta.items():
                if name.startswith("version:") and value is not None:
                    versions[name.partition(":")[-1]] = value

        revisions: dict[str, tuple[str, str]] = {}
        for tree, version in versions.items():
            # for revision ID we use tree name and version
            rev_id_str = revision.rev_id(tree, version)
            revisions[tree] = (version, rev_id_str)

        return revisions

    @contextmanager
    def make_session(self) -> Generator[Session]:
        """Make Cassandra session."""
        with SessionContext(self._config) as session:
            # Dump queries if debug level is enabled.
            if _LOG.isEnabledFor(logging.DEBUG):
                session.add_request_init_listener(_dump_query)

            # Disable result paging
            session.default_fetch_size = None

            yield session
            del session

    def make_alembic_db(self) -> sqlalchemy.Engine:
        """Make in-memory SQLite database with populated alembic_version table.

        Returns
        -------
        engine : `sqlalchemy.Engine`
        """
        # Get revisions from cassandra
        revisions = [revision for _, revision in self.tree_versions().values()]
        _LOG.debug("Found existing revisions in Cassandra: %s", revisions)

        # Make in-memory database and table.
        engine = sqlalchemy.create_engine("sqlite://")
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.schema.Table(
            "alembic_version",
            metadata,
            sqlalchemy.Column("version_num", sqlalchemy.types.String(32), primary_key=True),
        )
        metadata.create_all(engine)
        _LOG.debug("Created in-n memory alembic table: %s", table)

        # Copy revisions.
        insert = table.insert().values([{"version_num": revision} for revision in revisions])
        with engine.begin() as conn:
            conn.execute(insert)

        _LOG.debug("populated in-memory alembic table with revisions: %s", revisions)

        return engine
