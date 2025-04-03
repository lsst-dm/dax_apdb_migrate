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
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

import cassandra.query
import sqlalchemy
from cassandra import ConsistencyLevel
from cassandra.auth import AuthProvider, PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import RoundRobinPolicy
from lsst.utils.db_auth import DbAuth, DbAuthNotFoundError

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
    host : `str`
        Cassandra server host name.
    port : `int`, optional
        Port number for Cassandra connection.
    keyspace : `str`
        Cassandra keyspace name.
    username : `str`, optional
        Username to use for authetication, not needed if dbauth.yaml defines
        user name.
    """

    metadata_table_name = "metadata"
    """Name of the metadata table holding versions."""

    def __init__(self, host: str, keyspace: str, port: int | None = None, username: str | None = None):
        self._host = host
        self._keyspace = keyspace
        self._port = port if port is not None else 9042
        self._username = username

    @property
    def keyspace(self) -> str:
        """Keyspace name (`str`)"""
        return self._keyspace

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

            meta = ApdbMetadata(session, self._keyspace)
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
    def make_session(self) -> Iterator[Session]:
        """Make Cassandra session."""
        cluster = Cluster(
            execution_profiles=self._make_profiles(),
            contact_points=[self._host],
            port=self._port,
            auth_provider=self._make_auth_provider(),
            protocol_version=5,
        )
        try:
            session = cluster.connect()
            # Dump queries if debug level is enabled.
            if _LOG.isEnabledFor(logging.DEBUG):
                session.add_request_init_listener(_dump_query)

            # Disable result paging
            session.default_fetch_size = None

            yield session
            del session

        finally:

            cluster.shutdown()
            del cluster

    def _make_profiles(self) -> Mapping[Any, ExecutionProfile]:
        loadBalancePolicy = RoundRobinPolicy()
        # Use a very long timeout just in case our queries are not efficient.
        default_profile = ExecutionProfile(
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=3600.0,
            row_factory=cassandra.query.tuple_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        return {
            EXEC_PROFILE_DEFAULT: default_profile,
        }

    def _make_auth_provider(self) -> AuthProvider | None:
        """Make Cassandra authentication provider instance."""
        try:
            dbauth = DbAuth()
        except DbAuthNotFoundError:
            # Credentials file doesn't exist, use anonymous login.
            return None

        try:
            username, password = dbauth.getAuth(
                "cassandra",
                self._username,
                self._host,
                self._port,
                self._keyspace,
            )
            if username:
                return PlainTextAuthProvider(username=username, password=password)
            else:
                _LOG.warning(
                    f"Credentials file ({dbauth.db_auth_path}) provided password but not "
                    "user name, anonymous Cassandra logon will be attempted."
                )
        except DbAuthNotFoundError:
            pass

        return None

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
