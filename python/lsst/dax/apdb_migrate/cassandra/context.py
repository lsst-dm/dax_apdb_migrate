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

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import alembic
import alembic.operations

from .. import revision
from .apdb_metadata import ApdbMetadata
from .config import ApdbMigConfigCassandra
from .database import Database

if TYPE_CHECKING:
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
    session : `cassandra.cluster.Session`
        Cassandra database connection.
    db : `Database`
        Database interface.
    config : `ApdbMigConfigCassandra`
        Migration configuration.
    """

    def __init__(self, session: Session, db: Database, config: ApdbMigConfigCassandra) -> None:
        # Alembic migration context for the DB being migrated.
        self.mig_context = alembic.context.get_context()
        self.session = session
        self.db = db
        self.config = config

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

    def execute(self, query: Any, parameters: Any) -> Any:
        return self.session.execute(query, parameters)

    def update_tree_version(self, tree: str, version: str) -> None:
        """Update version for the specified tree.

        Parameters
        ----------
        tree : `str`
            Tree name.
        value : `str`
            New version string.
        """
        meta = ApdbMetadata(self.session, self.keyspace)
        meta.insert(f"version:{tree}", version)


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

    if config.dry_run:
        # Use query-printing session instead of makeing a real one.
        session = DryRunSession()
        ctx = _Context(session, config.db, config)
        yield ctx

    else:
        # Make actual Cassandra session.
        with config.db.make_session() as session:
            ctx = _Context(session, config.db, config)
            yield ctx

    # If it ran to completion store new version number.
    ctx.update_tree_version(tree, version)
