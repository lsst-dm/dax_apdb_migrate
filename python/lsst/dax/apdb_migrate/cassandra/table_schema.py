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

__all__ = ("Column", "TableSchema")

import dataclasses
from typing import Any, NamedTuple, cast

from .context import Context


@dataclasses.dataclass
class Column:
    """Representation of table column definition."""

    column_name: str
    type: str
    kind: str
    position: int = -1
    clustering_order: str | None = None

    @property
    def is_partitioning(self) -> bool:
        """True if this column is in partitioning key (`bool`)."""
        return self.kind == "partition_key"

    @property
    def is_clustering(self) -> bool:
        """True if this column is in clustering key (`bool`)."""
        return self.kind == "clustering"

    @property
    def ordering_key(self) -> tuple:
        """Key used for ordering (`tuple`)."""
        key: list = []
        if self.is_partitioning:
            key.append(0)
            key.append(self.position)
        elif self.is_clustering:
            key.append(1)
            key.append(self.position)
        else:
            key.append(2)
            key.append(self.column_name)
        return tuple(key)

    @classmethod
    def order_columns(cls, columns: list[Column]) -> None:
        """Order columns in place."""
        columns.sort(key=lambda column: column.ordering_key)


@dataclasses.dataclass
class TableSchema:
    """Representation of Cassandra table schema."""

    keyspace: str
    table_name: str
    columns: list[Column]
    table_options: dict[str, Any]

    @classmethod
    def from_table(cls, ctx: Context, table_name: str) -> TableSchema:
        """Contruct table schema from its database definition."""
        # Get the list of columns from existing table.
        query = (
            "select column_name, type, kind, position, clustering_order "
            "from system_schema.columns "
            "where keyspace_name = %s and table_name=%s"
        )
        # Rows are returned as namedtuples.
        columns: list[Column] = []
        for row in cast(NamedTuple, ctx.query(query, (ctx.keyspace, table_name))):
            columns.append(Column(**row._asdict()))

        # Sort all columns.
        Column.order_columns(columns)

        # Query table options.
        query = "select * from system_schema.tables where keyspace_name = %s and table_name = %s"
        rows = list(ctx.query(query, (ctx.keyspace, table_name)))
        options_tuple = cast(NamedTuple, rows[0])
        table_options = options_tuple._asdict()
        # Drop some keys that cannot appear in DDL.
        for key in ("keyspace_name", "table_name", "id", "memtable", "flags"):
            table_options.pop(key, None)

        return TableSchema(
            keyspace=ctx.keyspace,
            table_name=table_name,
            columns=columns,
            table_options=table_options,
        )

    @property
    def partitioning_columns(self) -> list[Column]:
        """Ordered list of columns in partitiong key (`list`[`Column`])."""
        columns = [column for column in self.columns if column.is_partitioning]
        columns.sort(key=lambda column: column.position)
        return columns

    @property
    def clustering_columns(self) -> list[Column]:
        """Ordered list of columns in clustering key (`list`[`Column`])."""
        columns = [column for column in self.columns if column.is_clustering]
        columns.sort(key=lambda column: column.position)
        return columns

    def make_ddl(self) -> str:
        """Return DDL for the table as a string."""
        # Order columns again in case column list has changed.
        Column.order_columns(self.columns)

        # Make primary key, quote all column names.
        part_columns = [f'"{column.column_name}"' for column in self.partitioning_columns]
        if len(part_columns) == 1:
            part_key = part_columns[0]
        elif len(part_columns) > 1:
            part_key = f"({', '.join(part_columns)})"
        else:
            raise ValueError("No partitiong columns defined.")
        pk_list = [part_key] + [f'"{column.column_name}"' for column in self.clustering_columns]
        pk = f"PRIMARY KEY ({', '.join(pk_list)})"

        # Generate table DDL.
        table_column_defs = [f'"{column.column_name}" {column.type}' for column in self.columns]
        table_column_defs.append(pk)
        column_defs = ",\n    ".join(table_column_defs)

        table_def = f'CREATE TABLE "{self.keyspace}"."{self.table_name}" (\n    {column_defs}\n)'

        # Also copy applicable table options.
        def _quote(value: Any) -> str:
            """Render values in options declaration."""
            if isinstance(value, (str, int, float)):
                return repr(value)
            return str(value)

        # Names of options we do not copy.
        skip_options = {
            "dclocal_read_repair_chance",
            "read_repair_chance",
        }
        options = []
        for option_name, option_value in self.table_options.items():
            if option_name not in skip_options and option_value is not None:
                options.append(f"{option_name} = {_quote(option_value)}")

        if any(column.clustering_order == "desc" for column in self.clustering_columns):
            # Add clustering order.
            column_order = [
                f'"{column.column_name}" {column.clustering_order}' for column in self.clustering_columns
            ]
            order_clause = ", ".join(column_order)
            options.append(f"CLUSTERING ORDER BY ({order_clause})")

        options_str = "\n    AND ".join(options)
        table_def = f"{table_def} WITH {options_str}"

        return table_def
