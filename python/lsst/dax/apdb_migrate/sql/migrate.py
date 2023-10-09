# This file is part dax_apdb_migrate.
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


class MigrationTrees:
    """Class encapsulating the knowledge of directory structures used by
    migrations.

    Parameters
    ----------
    mig_path : `str`
        Top-level folder with migrations.
    """

    _MIGRATE_FOLDER_ENV = "DAX_APDB_MIGRATE_MIGRATIONS"
    """Name of envvar that can be used to override location of top-level
    migrations folder.
    """

    _MIGRATE_PACKAGE_ENV = "DAX_APDB_MIGRATE_DIR"
    """Name of envvar for location of a package containing default migrations.
    """

    def __init__(self, mig_path: str | None = None):
        if mig_path is None:
            self.mig_path = self.migrations_folder()
        else:
            self.mig_path = mig_path

    @classmethod
    def migrations_folder(cls) -> str:
        """Return default location of top-level folder containing all
        migrations.

        Returns
        -------
        path : `str`
            Location of top-level folder containing all migrations.
        """
        loc = os.environ.get(cls._MIGRATE_FOLDER_ENV)
        if loc:
            return loc
        loc = os.environ.get(cls._MIGRATE_PACKAGE_ENV)
        if loc:
            return os.path.join(loc, "migrations/sql")
        raise ValueError(
            f"None of {cls._MIGRATE_FOLDER_ENV} or {cls._MIGRATE_PACKAGE_ENV}"
            " environment variables is defined"
        )

    def alembic_folder(self, *, relative: bool = True) -> str:
        """Return location of folder with alembic files.

        Parameters
        ----------
        relative : `bool`
            If True (default) then path relative to top-level folder is
            returned.

        Returns
        -------
        path : `str`
            Path to a folder alembic files (env.py, etc.) will be stored, path
            may not exist yet.
        """
        path = "_alembic"
        if not relative:
            path = os.path.join(self.mig_path, path)
        return path

    def version_location(self, tree: str, *, relative: bool = True) -> str:
        """Return location for regular migrations for a given tree.

        Parameters
        ----------
        tree : `str`
            Tree name, arbitrary string.
        relative : `bool`
            If True (default) then path relative to top-level folder is
            returned.

        Returns
        -------
        path : `str`
            Path to a folder where migration scripts will be stored, path may
            not exist yet.
        """
        path = tree
        if not relative:
            path = os.path.join(self.mig_path, path)
        return path

    def version_locations(self, *, relative: bool = True) -> list[str]:
        """Return locations for all migrations.

        Parameters
        ----------
        relative : `bool`
            If True (default) then locations relative to top-level folder are
            returned.

        Returns
        -------
        names : `dict` [ `str`, `str` ]
            Dictionary where key is a tree name and value is the location of a
            folder with migration scripts.
        """
        locations: dict[str, str] = {}
        for entry in os.scandir(self.mig_path):
            if entry.is_dir() and entry.name != "_alembic":
                path = entry.name
                if not relative:
                    path = os.path.join(self.mig_path, path)
                locations[entry.name] = path
        return list(locations.values())
