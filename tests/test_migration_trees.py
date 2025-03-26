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

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from lsst.dax.apdb_migrate.trees import MigrationTrees


class MigrationTreesTestCase(unittest.TestCase):
    """Tests for MigrationTrees class."""

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_migrations_folder(self) -> None:
        """Test migrations_folder method."""
        with patch.dict(os.environ, {"DAX_APDB_MIGRATE_MIGRATIONS": "/migrations"}, clear=True):
            self.assertEqual(MigrationTrees.migrations_folder("any"), "/migrations")

        with patch.dict(os.environ, {"DAX_APDB_MIGRATE_DIR": "/dax_apd_migrate"}, clear=True):
            self.assertEqual(MigrationTrees.migrations_folder("any"), "/dax_apd_migrate/migrations/any")

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                print(MigrationTrees.migrations_folder("any"))

    def test_methods(self) -> None:
        """Test various methods of the class."""
        with patch.dict(os.environ, {"DAX_APDB_MIGRATE_DIR": self.tempdir}, clear=True):
            trees = MigrationTrees("sql")

            sql_migrations = os.path.join(self.tempdir, "migrations", "sql")

            self.assertEqual(trees.alembic_folder(), "_alembic")
            self.assertEqual(
                trees.alembic_folder(relative=False),
                os.path.join(sql_migrations, "_alembic"),
            )

            self.assertEqual(trees.version_location("ApdbSql"), "ApdbSql")
            self.assertEqual(
                trees.version_location("ApdbSql", relative=False),
                os.path.join(sql_migrations, "ApdbSql"),
            )

            for folder in ("_alembic", "schema", "something"):
                os.makedirs(os.path.join(sql_migrations, folder))

            self.assertEqual(set(trees.version_locations()), {"schema", "something"})
            self.assertEqual(
                set(trees.version_locations(relative=False)),
                {
                    os.path.join(sql_migrations, "schema"),
                    os.path.join(sql_migrations, "something"),
                },
            )


if __name__ == "__main__":
    unittest.main()
