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

__all__ = ["ApdbMetadata"]

import json
from typing import Any, Protocol


class Session(Protocol):
    """Protocol for Cassandra Session, implementations will exist that just
    print queries instead of executing them.
    """

    def execute(self, query: Any, parameters: Any) -> Any: ...


class ApdbMetadata:
    """Helper class to manipulate the contents of APDB metadata table.

    Parameters
    ----------
    session : `cassandra.cluster.Session`
        Database session.
    keyspace : `str`
        Name of Cassandra keyspace containing metadata table.
    """

    def __init__(self, session: Session, keyspace: str, *, update_session: Session | None = None):
        self._session = session
        self._update_session = update_session if update_session is not None else session
        self._keyspace = keyspace
        self._part = 0

    def items(self) -> list[tuple[str, str | None]]:
        """Return all items in metadata table.

        Returns
        -------
        items : `list` [`tuple`]
            List of name/value pairs.
        """
        query = f'SELECT name, value FROM "{self._keyspace}".metadata WHERE meta_part = %s'
        return [(name, value) for name, value in self._session.execute(query, (self._part,))]

    def get(self, key: str) -> str | None:
        """Retrieve values of the specified key.

        Parameters
        ----------
        key : `str`
            Metadata key.

        Returns
        -------
        value : `str` or `None`
            Value for the key value, `None` is returned if attribute does not
            exist.
        """
        query = f'SELECT value FROM "{self._keyspace}".metadata WHERE meta_part = %s and name = %s'
        for row in self._session.execute(query, (self._part, key)):
            return row[0]
        return None

    def insert(self, name: str, value: str) -> None:
        """Insert new parameter in metadata table.

        Parameters
        ----------
        name : `str`
            New attribute name.
        value : `str`
            Attribute value.
        """
        query = f'INSERT INTO "{self._keyspace}".metadata (meta_part, name, value) VALUES (%s, %s, %s)'
        self._update_session.execute(query, (self._part, name, value))

    def delete(self, name: str) -> None:
        """Delete parameter from metadata table.

        Parameters
        ----------
        name : `str`
            Metadata key name.
        """
        query = f'DELETE FROM "{self._keyspace}".metadata WHERE meta_part = %s and name = %s'
        self._update_session.execute(query, (self._part, name))

    def update_tree_version(self, tree: str, version: str) -> None:
        """Update version for the specified tree.

        Parameters
        ----------
        tree : `str`
            Tree name.
        value : `str`
            New version string.
        """
        self.insert(f"version:{tree}", version)

    def update_config(
        self,
        updates: dict[str, Any] | None = None,
        *,
        deletes: list[str] | None = None,
        config_name: str = "apdb-cassandra.json",
    ) -> None:
        """Update configuration stored as JSON string in metadata.

        Parameters
        ----------
        updates : `dict` [`str`, `Any`], optional
            Key-value pairs to update configuration with. Existing key will be
            overwritten.
        deletes : `list` [`str`], optional
            List of keys to remove from configuration. The keys may not exist
            in the configuration.
        config_name : `str`, optional
            Name of the configuration record. The metadata key will be this
            name prefixed with "config:". Corresponding key must already exist
            in metadata.
        """
        config_key = f"config:{config_name}"

        # Read existing config.
        config_str = self.get(config_key)
        if config_str is None:
            raise LookupError(f"Key '{config_key}' does not exist in metadata.")
        config_obj = json.loads(config_str)
        assert isinstance(config_obj, dict), "All config keys must be dictionaries"

        # Do all updates.
        if updates:
            config_obj.update(updates)

        # Do all deletes.
        if deletes:
            for key in deletes:
                config_obj.pop(key, None)

        # Write it back.
        config_str = json.dumps(config_obj)
        self.insert(config_key, config_str)
