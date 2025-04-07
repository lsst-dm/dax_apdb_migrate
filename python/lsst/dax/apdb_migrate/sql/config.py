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

__all__ = ["ApdbMigConfigSql"]

from .. import config
from . import database


class ApdbMigConfigSql(config.ApdbMigConfig):
    """Implementation of alembic config class which adds options specific to
    SQL backend.
    """

    @classmethod
    def from_mig_path(
        cls,
        mig_path: str,
        *,
        db: database.Database | None = None,
        single_tree: str | None = None,
        migration_options: dict[str, str] | None = None,
    ) -> ApdbMigConfigSql:
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
        cfg = cls(mig_path, single_tree=single_tree, migration_options=migration_options)

        if db is not None:
            # URL may contain URL-encoded items which include % sign, and that
            # needs to be escaped with another % before it is passed to
            # ConfigParser.
            url = db.db_url.replace("%", "%%")
            cfg.set_main_option("sqlalchemy.url", url)
            if db.schema:
                cfg.set_section_option("dax_apdb_migrate", "schema", db.schema)

        return cfg
