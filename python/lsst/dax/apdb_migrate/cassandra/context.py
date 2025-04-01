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

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import alembic
import alembic.operations

from .config import ApdbMigConfigCassandra

if TYPE_CHECKING:
    from cassandra.cluster import Session


class Context:
    """Provides access to commonly-needed objects derived from the alembic
    migration context.
    """

    def __init__(self) -> None:
        # Alembic migration context for the DB being migrated.
        self.mig_context = alembic.context.get_context()

        assert isinstance(alembic.context.config, ApdbMigConfigCassandra), "Expecting ApdbMigConfigCassandra"
        self.config = alembic.context.config

        assert self.config.db is not None
        self.db = self.config.db

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

    @property
    def keyspace(self) -> str:
        """Keyspace name (`str`)"""
        return self.db.keyspace

    @contextmanager
    def session(self) -> Iterator[Session]:
        """Make Cassandra session."""
        with self.db.make_session() as session:
            yield session
