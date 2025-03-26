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

from lsst.dax.apdb_migrate.config import ApdbMigConfig


class ApdbMigConfigTestCase(unittest.TestCase):
    """Tests for ApdbMigConfig class."""

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.migrations = os.path.join(self.tempdir, "sql")
        os.makedirs(os.path.join(self.migrations, "schema"))
        os.makedirs(os.path.join(self.migrations, "ApdbSql"))

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_config(self) -> None:
        """Test migrations_folder method."""
        with patch.dict(os.environ, {"DAX_APDB_MIGRATE_DIR": "/dax_apd_migrate"}, clear=True):
            config = ApdbMigConfig(self.migrations, "sql")

            self.assertEqual(
                config.get_main_option("script_location"), os.path.join(self.migrations, "_alembic")
            )

            expected_locations = set(
                [os.path.join(self.migrations, "schema"), os.path.join(self.migrations, "ApdbSql")]
            )
            version_locations = config.get_main_option("version_locations")
            assert version_locations is not None
            self.assertEqual(set(version_locations.split()), expected_locations)

            self.assertEqual(config.get_template_directory(), "/dax_apd_migrate/templates")

    def test_config_single_tree(self) -> None:
        """Test migrations_folder method."""
        with patch.dict(os.environ, {"DAX_APDB_MIGRATE_DIR": "/dax_apd_migrate"}, clear=True):
            config = ApdbMigConfig(self.migrations, "sql", single_tree="schema")

            self.assertEqual(
                config.get_main_option("script_location"), os.path.join(self.migrations, "_alembic")
            )
            self.assertEqual(
                config.get_main_option("version_locations"), os.path.join(self.migrations, "schema")
            )

    def test_config_options(self) -> None:
        """Test migrations_folder method."""
        with patch.dict(os.environ, {"DAX_APDB_MIGRATE_DIR": "/dax_apd_migrate"}, clear=True):
            options = {"option1": "test1", "option2": "test2"}

            config = ApdbMigConfig(self.migrations, "sql", migration_options=options)

            for key, value in options.items():
                self.assertEqual(config.get_section_option("dax_apdb_migrate_options", key), value)


if __name__ == "__main__":
    unittest.main()
