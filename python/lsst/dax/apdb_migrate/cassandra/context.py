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

import json
import logging
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, Literal

import alembic

from .. import revision
from .apdb_metadata import ApdbMetadata
from .config import ApdbMigConfigCassandra
from .schema import Schema

if TYPE_CHECKING:
    import cassandra.query
    from cassandra.cluster import Session

_LOG = logging.getLogger(__name__)


class DryRunSession:
    """A replacement for Cassandra session that prints queries instead of
    executing them.
    """

    def execute(self, query: Any, parameters: Any) -> Any:
        _LOG.info("Query: '%s', parameters: %s", query, parameters)

    def prepare(self, query: str) -> Any:
        raise NotImplementedError()


class Context:
    """Provides access to commonly-needed objects derived from the alembic
    migration context.

    Parameters
    ----------
    revision_or_tree: `str`
        This can be either full revision string, if ``version`` is not
        specified, or revision tree name otherwise. If ``version`` is not given
        then tree name and version are extracted from the revision.
    version_str : `str`, optional
        Version, must be specified if packed revision name cannot be unpacked.

    Notes
    -----
    query_session : `cassandra.cluster.Session`
        Cassandra database connection used for SELECT queries.
    update_session : `cassandra.cluster.Session`
        Cassandra database connection used for modifying queries.
    db : `Database`
        Database interface.
    config : `ApdbMigConfigCassandra`
        Migration configuration.
    """

    metadataConfigKey = "config:apdb-cassandra.json"

    def __init__(self, revision_or_tree: str, version_str: str | None = None):
        # First make sure that revision string looks reasonable.
        if version_str:
            self._tree = revision_or_tree
            self._version = version_str
        else:
            unpacked_tree, unpacked_version = revision.unpack_revision(revision_or_tree)
            if unpacked_tree is None or unpacked_version is None:
                raise ValueError(f"Unsupported fromat of the revision string: {revision_or_tree}")
            self._version = unpacked_version
            self._tree = unpacked_tree

        config = alembic.context.config
        assert isinstance(config, ApdbMigConfigCassandra), "Expecting ApdbMigConfigCassandra"
        self.config = config

        # Alembic migration context for the DB being migrated.
        self.mig_context = alembic.context.get_context()
        self._query_session: Session | None = None
        self._update_session: Session | DryRunSession | None = None
        assert config.db is not None
        self.db = config.db
        self._stack = ExitStack()

    def __enter__(self) -> Context:
        session = self._stack.enter_context(self.db.make_session())
        self._query_session = session
        self._update_session = session
        if self.dry_run:
            # Use query-printing session instead of a real one.
            self._update_session = DryRunSession()
        else:
            self._update_session = session
        return self

    def __exit__(self, exc_type: type | None, exc_value: Any, traceback: Any) -> Literal[False]:
        # If it ran to completion store new version number.
        assert self._query_session is not None
        if exc_type is None:
            self.update_tree_version(self._tree, self._version)
        self._stack.__exit__(exc_type, exc_value, traceback)
        return False

    def _check_context(self) -> None:
        if self._query_session is None:
            raise TypeError("Cannot use this object outside context.")

    @property
    def dry_run(self) -> bool:
        """True when the dry-run option is set."""
        return self.config.dry_run

    @property
    def session(self) -> Session | DryRunSession:
        """Session used for running database updates."""
        self._check_context()
        return self._update_session

    @property
    def metadata(self) -> ApdbMetadata:
        """Metadata table interface (`ApdbMetadata`)."""
        self._check_context()
        assert self._query_session is not None
        return ApdbMetadata(self._query_session, self.keyspace, update_session=self._update_session)

    @property
    def schema(self) -> Schema:
        """Helper instance for schema queries (`Scchema`)."""
        self._check_context()
        assert self._query_session is not None
        return Schema(self._query_session, self.keyspace, self.get_apdb_config())

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
        return self.config.get_section_option("dax_apdb_migrate_options", option)

    @property
    def keyspace(self) -> str:
        """Keyspace name (`str`)"""
        return self.db.keyspace

    def query(
        self, query: str | cassandra.query.Statement, parameters: Sequence | Mapping | None = None
    ) -> Any:
        """Run a query against Cassandra backend, should only be used to
        execute SELECT queries.

        Parameters
        ----------
        query : `str` or `cassandra.query.Statement`
            Query string or `cassandra.query.Statement` instance. Only
            SELECT queries are allowed here.
        parameters : `~collections.abc.Sequence` or `~collections.abc.Mapping`
            Query parameters.
        """
        self._check_context()
        assert self._query_session is not None
        return self._query_session.execute(query, parameters)

    def update(
        self, query: str | cassandra.query.Statement, parameters: Sequence | Mapping | None = None
    ) -> Any:
        """Run a query against Cassandra backend or print the query if dry-run
        option is set, should be used to execute all modifying queries.

        Parameters
        ----------
        query : `str` or `cassandra.query.Statement`
            Query string or `cassandra.query.Statement` instance.
        parameters : `~collections.abc.Sequence` or `~collections.abc.Mapping`
            Query parameters.
        """
        self._check_context()
        assert self._update_session is not None
        return self._update_session.execute(query, parameters)

    def get_apdb_config(self) -> dict[str, Any]:
        """Return frozen part of APDB config from metadata."""
        config_json = self.metadata.get(self.metadataConfigKey)
        if not config_json:
            raise LookupError(f"Cannot find {self.metadataConfigKey} in metadata table.")
        return json.loads(config_json)

    def store_apdb_config(self, config: dict[str, Any]) -> None:
        """Store frozen part of APDB config to metadata."""
        json_str = json.dumps(config)
        self.metadata.insert(self.metadataConfigKey, json_str)

    def has_replicas(self) -> bool:
        """Return True if replication is enabled."""
        apdb_config = self.get_apdb_config()
        return apdb_config["enable_replica"]

    def update_tree_version(self, tree: str, version: str) -> None:
        """Update version for the specified tree.

        Parameters
        ----------
        tree : `str`
            Tree name.
        value : `str`
            New version string.
        """
        self.metadata.insert(f"version:{tree}", version)
