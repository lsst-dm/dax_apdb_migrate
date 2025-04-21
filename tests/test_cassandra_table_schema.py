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


import unittest

from lsst.dax.apdb_migrate.cassandra.table_schema import Column, TableSchema


class TableSchemaTestCase(unittest.TestCase):
    """Tests for table_schema module"""

    def setUp(self) -> None:
        super().setUp()
        self.columns = {
            "b": Column(column_name="b", type="varchar", kind="regular"),
            "a": Column(column_name="a", type="int", kind="regular"),
            "clust1": Column(
                column_name="clust1", type="int", kind="clustering", position=1, clustering_order="acs"
            ),
            "clust2": Column(
                column_name="clust2", type="int", kind="clustering", position=5, clustering_order="asc"
            ),
            "clust3": Column(
                column_name="clust3", type="int", kind="clustering", position=2, clustering_order="desc"
            ),
            "part2": Column(column_name="part2", type="int", kind="partition_key", position=2),
            "part1": Column(column_name="part1", type="longint", kind="partition_key", position=1),
        }

    def test_columns(self) -> None:
        """Test Column methods."""
        column = self.columns["part1"]
        self.assertTrue(column.is_partitioning)
        self.assertFalse(column.is_clustering)
        self.assertEqual(column.ordering_key, (0, 1))

        column = self.columns["part2"]
        self.assertTrue(column.is_partitioning)
        self.assertFalse(column.is_clustering)
        self.assertEqual(column.ordering_key, (0, 2))

        column = self.columns["clust1"]
        self.assertFalse(column.is_partitioning)
        self.assertTrue(column.is_clustering)
        self.assertEqual(column.ordering_key, (1, 1))

        column = self.columns["clust2"]
        self.assertFalse(column.is_partitioning)
        self.assertTrue(column.is_clustering)
        self.assertEqual(column.ordering_key, (1, 5))

        column = self.columns["clust3"]
        self.assertFalse(column.is_partitioning)
        self.assertTrue(column.is_clustering)
        self.assertEqual(column.ordering_key, (1, 2))

        column = self.columns["a"]
        self.assertFalse(column.is_partitioning)
        self.assertFalse(column.is_clustering)
        self.assertEqual(column.ordering_key, (2, "a"))

        column = self.columns["b"]
        self.assertFalse(column.is_partitioning)
        self.assertFalse(column.is_clustering)
        self.assertEqual(column.ordering_key, (2, "b"))

    def test_column_sort(self) -> None:
        """Test Column sorting."""
        columns = list(self.columns.values())
        Column.order_columns(columns)

        self.assertEqual(len(columns), 7)
        self.assertIs(columns[0], self.columns["part1"])
        self.assertIs(columns[1], self.columns["part2"])
        self.assertIs(columns[2], self.columns["clust1"])
        self.assertIs(columns[3], self.columns["clust3"])
        self.assertIs(columns[4], self.columns["clust2"])
        self.assertIs(columns[5], self.columns["a"])
        self.assertIs(columns[6], self.columns["b"])

    def test_make_ddl(self) -> None:
        """Test make_ddl method."""
        columns = [
            self.columns["part1"],
            self.columns["a"],
            self.columns["b"],
        ]
        table_options = {
            "additional_write_policy": "99p",
            "compression": {"chunk_length_in_kb": "16", "class": "LZ4Compressor"},
            "gc_grace_seconds": 0,
        }

        schema = TableSchema(
            keyspace="spacekey", table_name="table1", columns=columns, table_options=table_options
        )
        ddl = schema.make_ddl()
        expected_ddl = """\
CREATE TABLE "spacekey"."table1" (
    "part1" longint,
    "a" int,
    "b" varchar,
    PRIMARY KEY ("part1")
) WITH additional_write_policy = '99p'
    AND compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'}
    AND gc_grace_seconds = 0\
"""
        self.assertEqual(ddl, expected_ddl)

        columns = [
            self.columns["part2"],
            self.columns["part1"],
            self.columns["b"],
            self.columns["a"],
        ]
        schema = TableSchema(
            keyspace="spacekey", table_name="table2", columns=columns, table_options=table_options
        )
        ddl = schema.make_ddl()
        expected_ddl = """\
CREATE TABLE "spacekey"."table2" (
    "part1" longint,
    "part2" int,
    "a" int,
    "b" varchar,
    PRIMARY KEY (("part1", "part2"))
) WITH additional_write_policy = '99p'
    AND compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'}
    AND gc_grace_seconds = 0\
"""
        self.assertEqual(ddl, expected_ddl)

        columns = [
            self.columns["part2"],
            self.columns["part1"],
            self.columns["clust2"],
            self.columns["clust1"],
            self.columns["b"],
            self.columns["a"],
        ]
        schema = TableSchema(
            keyspace="spacekey", table_name="table3", columns=columns, table_options=table_options
        )
        ddl = schema.make_ddl()
        expected_ddl = """\
CREATE TABLE "spacekey"."table3" (
    "part1" longint,
    "part2" int,
    "clust1" int,
    "clust2" int,
    "a" int,
    "b" varchar,
    PRIMARY KEY (("part1", "part2"), "clust1", "clust2")
) WITH additional_write_policy = '99p'
    AND compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'}
    AND gc_grace_seconds = 0\
"""
        self.assertEqual(ddl, expected_ddl)

        columns = [
            self.columns["part1"],
            self.columns["clust1"],
            self.columns["clust3"],
            self.columns["b"],
            self.columns["a"],
        ]
        schema = TableSchema(
            keyspace="spacekey", table_name="table3", columns=columns, table_options=table_options
        )
        ddl = schema.make_ddl()
        expected_ddl = """\
CREATE TABLE "spacekey"."table3" (
    "part1" longint,
    "clust1" int,
    "clust3" int,
    "a" int,
    "b" varchar,
    PRIMARY KEY ("part1", "clust1", "clust3")
) WITH additional_write_policy = '99p'
    AND compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'}
    AND gc_grace_seconds = 0
    AND CLUSTERING ORDER BY ("clust1" acs, "clust3" desc)\
"""
        self.assertEqual(ddl, expected_ddl)


if __name__ == "__main__":
    unittest.main()
