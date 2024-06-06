# This file is part of dax_apdb.
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

import sqlalchemy


class ApdbMetadata:
    """Helper class to manipulate the contents of APDB metadata table.

    Parameters
    ----------
    engine : `sqlalchemy.engine.Engine`
        Database access engine.
    schema : `ApdbSqlSchema`
        Object providing access to schema details.
    """

    def __init__(self, connection: sqlalchemy.engine.Connection, schema: str | None = None):
        self._connection = connection
        metadata = sqlalchemy.schema.MetaData(schema=schema)
        self._table = sqlalchemy.schema.Table(
            "metadata",
            metadata,
            sqlalchemy.schema.Column("name", sqlalchemy.Text, primary_key=True),
            sqlalchemy.schema.Column("value", sqlalchemy.Text, nullable=False),
            schema=schema,
        )

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
        sql = sqlalchemy.select(self._table.columns["value"]).where(self._table.columns["name"] == key)
        result = self._connection.execute(sql)
        return result.scalar()

    def insert(self, name: str, value: str) -> None:
        """Insert new parameter in butler_attributes table.

        Parameters
        ----------
        name : `str`
            New attribute name.
        value : `str`
            Attribute value.
        """
        sql = self._table.insert().values(name=name, value=value)
        self._connection.execute(sql)

    def update(self, key: str, value: str) -> int:
        """Update the value of existing key in metadata table.

        Parameters
        ----------
        key : `str`
            Key name.
        value : `str`
            New value.

        Returns
        -------
        updates : `int`
            Number of updated rows, 0 if no matching attribute found, 1
            otherwise.
        """
        # update version
        sql = self._table.update().where(self._table.columns.name == key).values(value=value)
        result = self._connection.execute(sql)
        # result may be None in offline mode, assume that we updated something
        return 1 if result is None else result.rowcount

    def update_tree_version(self, tree: str, version: str, *, insert: bool = False) -> None:
        """Update version for the specified tree.

        Parameters
        ----------
        tree : `str`
            Tree name.
        value : `str`
            New version string.
        insert : `bool`
            Assume key does not exist and has to be created.

        Raises
        ------
        LookupError
            Raised if tree version is not found in the table (or already found
            if ``insert`` is `True`).
        """
        tree_key = f"version:{tree}"
        if insert:
            try:
                self.insert(tree_key, version)
            except sqlalchemy.exc.IntegrityError as exc:
                raise LookupError(f"Tree version key {tree_key} already exists in metadata table.") from exc
        else:
            count = self.update(tree_key, version)
            if count != 1:
                raise LookupError(f"Tree version key {tree_key} is not found in metadata table.")
