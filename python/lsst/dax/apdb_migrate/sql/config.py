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

import logging
import os

from alembic.config import Config

from .. import trees
from . import database

_LOG = logging.getLogger(__name__)

_MIGRATE_PACKAGE_ENV = "DAX_APDB_MIGRATE_DIR"


class MigAlembicConfig(Config):
    """Implementation of alembic config class which overrides few methods."""

    @classmethod
    def from_mig_path(
        cls,
        mig_path: str,
        *,
        db: database.Database | None = None,
        single_tree: str | None = None,
        migration_options: dict[str, str] | None = None,
    ) -> MigAlembicConfig:
        """Create new configuration object.

        Parameters
        ----------
        mig_path : `str`
            Filesystem path to location of revision trees.
        db : `database.Database`
            Object encapsulating access to database information.
        single_tree : `str`, optional
            If provided then Alembic will be configured with a single version
            tree only.
        migration_options : `dict`, optional
            Additional options that can be passed to migration script via the
            configuration object, in a section "dax_apdb_migrate_options".
        """
        alembic_folder = os.path.join(mig_path, "_alembic")
        cfg = cls()
        cfg.set_main_option("script_location", alembic_folder)
        cfg.set_section_option("alembic", "file_template", "%%(rev)s")
        cfg.set_section_option("alembic", "prepend_sys_path", ".")
        _LOG.debug("alembic_folder: %r, single_tree: %r", alembic_folder, single_tree)

        migrate_trees = trees.MigrationTrees("sql", mig_path)
        if single_tree:
            version_locations = [migrate_trees.version_location(single_tree, relative=False)]
        else:
            version_locations = migrate_trees.version_locations(relative=False)
        _LOG.debug("version_locations: %r", version_locations)
        cfg.set_main_option("version_locations", " ".join(version_locations))

        # override default file template
        cfg.set_main_option("file_template", "%%(rev)s")

        # we do not use these options, this is just to make sure that
        # their sections exists.
        cfg.set_section_option("dax_apdb_migrate", "_dax_apdb_migrate", "")
        cfg.set_section_option("dax_apdb_migrate_options", "_dax_apdb_migrate_options", "")

        if db is not None:
            # URL may contain URL-encoded items which include % sign, and that
            # needs to be escaped with another % before it is passed to
            # ConfigParser.
            url = db.db_url.replace("%", "%%")
            cfg.set_main_option("sqlalchemy.url", url)
            if db.schema:
                cfg.set_section_option("dax_apdb_migrate", "schema", db.schema)

        if migration_options:
            for key, value in migration_options.items():
                cfg.set_section_option("dax_apdb_migrate_options", key, value)

        return cfg

    def get_template_directory(self) -> str:
        """Return the directory where Alembic setup templates are found.

        This overrides method from alembic Config to copy templates for our own
        location.
        """
        package_dir = os.environ.get(_MIGRATE_PACKAGE_ENV)
        if not package_dir:
            raise ValueError(f"{_MIGRATE_PACKAGE_ENV} environment variable is not defined")

        return os.path.join(package_dir, "templates")
