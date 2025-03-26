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

__all__ = ["ApdbMigConfig"]

import logging
import os

from alembic.config import Config

from . import trees

_LOG = logging.getLogger(__name__)

_MIGRATE_PACKAGE_ENV = "DAX_APDB_MIGRATE_DIR"


class ApdbMigConfig(Config):
    """Implementation of alembic config class which overrides few methods.

    Parameters
    ----------
    mig_path : `str`
        Filesystem path to location of revision trees.
    single_tree : `str`, optional
        If provided then Alembic will be configured with a single version
        tree only.
    migration_options : `dict`, optional
        Additional options that can be passed to migration script via the
        configuration object, in a section "dax_apdb_migrate_options".
    """

    def __init__(
        self,
        mig_path: str,
        *,
        single_tree: str | None = None,
        migration_options: dict[str, str] | None = None,
    ):
        super().__init__()

        alembic_folder = os.path.join(mig_path, "_alembic")
        self.set_main_option("script_location", alembic_folder)
        self.set_section_option("alembic", "file_template", "%%(rev)s")
        self.set_section_option("alembic", "prepend_sys_path", ".")
        _LOG.debug("alembic_folder: %r, single_tree: %r", alembic_folder, single_tree)

        migrate_trees = trees.MigrationTrees(mig_path=mig_path)
        if single_tree:
            version_locations = [migrate_trees.version_location(single_tree, relative=False)]
        else:
            version_locations = migrate_trees.version_locations(relative=False)
        _LOG.debug("version_locations: %r", version_locations)
        self.set_main_option("version_locations", " ".join(version_locations))

        # override default file template
        self.set_main_option("file_template", "%%(rev)s")

        # we do not use these options, this is just to make sure that
        # their sections exists.
        self.set_section_option("dax_apdb_migrate", "_dax_apdb_migrate", "")
        self.set_section_option("dax_apdb_migrate_options", "_dax_apdb_migrate_options", "")

        if migration_options:
            for key, value in migration_options.items():
                self.set_section_option("dax_apdb_migrate_options", key, value)

    def get_template_directory(self) -> str:
        """Return the directory where Alembic setup templates are found.

        This overrides method from alembic Config to copy templates for our own
        location.
        """
        package_dir = os.environ.get(_MIGRATE_PACKAGE_ENV)
        if not package_dir:
            raise ValueError(f"{_MIGRATE_PACKAGE_ENV} environment variable is not defined")

        return os.path.join(package_dir, "templates")
