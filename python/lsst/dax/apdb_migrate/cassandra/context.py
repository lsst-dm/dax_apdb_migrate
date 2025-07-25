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
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import alembic

from .. import revision
from .apdb_metadata import ApdbMetadata
from .config import ApdbMigConfigCassandra
from .database import Database

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


class _Context:
    """Provides access to commonly-needed objects derived from the alembic
    migration context.

    Parameters
    ----------
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

    def __init__(
        self,
        query_session: Session,
        update_session: Session | DryRunSession,
        db: Database,
        config: ApdbMigConfigCassandra,
    ) -> None:
        # Alembic migration context for the DB being migrated.
        self.mig_context = alembic.context.get_context()
        self._query_session = query_session
        self._update_session = update_session
        self.db = db
        self.config = config

    @property
    def dry_run(self) -> bool:
        """True when the dry-run option is set."""
        return self.config.dry_run

    @property
    def session(self) -> Session | DryRunSession:
        """Session used for running database updates."""
        return self._update_session

    @property
    def metadata(self) -> ApdbMetadata:
        """Metadata table interface (`ApdbMetadata`)."""
        return ApdbMetadata(self._query_session, self.keyspace, update_session=self._update_session)

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


@contextmanager
def Context(revision_or_tree: str, version_str: str | None = None) -> Iterator[_Context]:
    """Create context manager for migration operations.

    Parameters
    ----------
    revision_or_tree: `str`
        This can be either full revision string, if ``version`` is not
        specified, or revision tree name otherwise. If ``version`` is not given
        then tree name and version are extracted from the revision.
    version_str : `str`, optional
        Version, must be specified if packed revision name cannot be unpacked.

    Yeilds
    ------
    context : `_Context`
        Object providing interface for migration operations.
    """
    # First maek sure that revision string looks reasonable.
    if version_str:
        tree = revision_or_tree
        version = version_str
    else:
        unpacked_tree, unpacked_version = revision.unpack_revision(revision_or_tree)
        if unpacked_tree is None or unpacked_version is None:
            raise ValueError(f"Unsupported fromat of the revision string: {revision_or_tree}")
        version = unpacked_version
        tree = unpacked_tree

    config = alembic.context.config
    assert isinstance(config, ApdbMigConfigCassandra), "Expecting ApdbMigConfigCassandra"
    assert config.db is not None

    with config.db.make_session() as session:
        update_session: Session | DryRunSession = session
        if config.dry_run:
            # Use query-printing session instead of a real one.
            update_session = DryRunSession()

        ctx = _Context(session, update_session, config.db, config)
        yield ctx

        # If it ran to completion store new version number.
        ctx.update_tree_version(tree, version)
