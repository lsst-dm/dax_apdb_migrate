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

__all__ = ["Schema"]

from typing import Any, Protocol


class Session(Protocol):
    """Protocol for Cassandra Session, implementations will exist that just
    print queries instead of executing them.
    """

    def execute(self, query: Any, parameters: Any) -> Any: ...


class Schema:
    """Class with methods for schema inspection.

    Parameters
    ----------
    session : `cassandra.cluster.Session`
        Database session.
    keyspace : `str`
        Name of Cassandra keyspace containing metadata table.
    apdb_config : `dict`
        Frozen part of APDB config from metadata.
    """

    def __init__(self, session: Session, keyspace: str, apdb_config: dict[str, Any]):
        self._session = session
        self._keyspace = keyspace
        self._config = apdb_config

    @property
    def _has_replicas(self) -> bool:
        return self._config["enable_replica"]

    @property
    def _has_partitioned_tables(self) -> bool:
        return self._config["time_partition_tables"]

    def all_tables(self) -> list[str]:
        """Return names of all tables in the keyspace.

        Returns
        -------
        items : `list` [`str`]
            List of table names.
        """
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
        result = self._session.execute(query, (self._keyspace,))
        return [row[0] for row in result.all()]

    def tables_for_schema(
        self, schema_kind: str, *, include_replica: bool = True, include_obj_last: bool = False
    ) -> list[str]:
        """Return list of existing tables that share the same table schema.

        Parameters
        ----------
        schema_kind : `str`
            Table schema, one of "DiaObject", "DiaSource", "DiaForcedSorce".
            Other kinds may exist that also can be specified.
        include_replica : `bool`, optional
            If `True` then include replica tables if replication is enabled
            and table is replicated.
        include_obj_last : `bool`, optional
            If `True` and ``schema_kind`` is "DiaObject" then include
            "DiaObjectLast" table in the result.

        Returns
        -------
        names : `list` [`str`]
            Names of the tables.
        """
        has_replicas = self._has_replicas
        has_partitioned_tables = self._has_partitioned_tables

        all_tables = set(self.all_tables())
        check_tables = []
        check_partitions = False
        if schema_kind in ("DiaObject", "DiaSource", "DiaForcedSource"):
            if has_replicas and include_replica:
                check_tables += [f"{schema_kind}Chunks", f"{schema_kind}Chunks2"]
            if has_partitioned_tables:
                check_partitions = True
        if schema_kind == "DiaObject" and include_obj_last:
            check_tables.append("DiaObjectLast")

        # Try to check table name specifically.
        check_tables.append(schema_kind)

        result = [table for table in check_tables if table in all_tables]
        if check_partitions:
            for table in all_tables:
                kind, _, part = table.partition("_")
                if kind == schema_kind and part.isdigit():
                    result.append(table)

        return result
