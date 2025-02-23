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

"""Display current revisions for a database."""

from __future__ import annotations

import logging

from alembic import command

from .. import config, database, scripts

_LOG = logging.getLogger(__name__)


def migrate_upgrade(
    db_url: str, schema: str | None, revision: str, mig_path: str, sql: bool, options: dict[str, str] | None
) -> None:
    """Upgrade schema to a specified revision.

    Parameters
    ----------
    db_url : `str`
        Database URL.
    schema : `str` or `None`
        Database schema name.
    revision : `str`
        Target revision or colon-separated range for sql mode.
    mig_path : `str`
        Filesystem path to location of revisions.
    sql : `bool`
        If True dump SQL instead of executing migration on a database.
    options : `dict` or `None`
        Additional key:value options specified on command line
    """
    db = database.Database(db_url, schema)

    # Check that alembic versions exist in database, we do not support
    # migrations from empty state.
    if not db.alembic_revisions():
        raise ValueError(
            "Alembic version table does not exist, you may need to run `apdb-migrate-sql stamp` first."
        )

    cfg = config.MigAlembicConfig.from_mig_path(mig_path, db=db, migration_options=options)

    # check that alembic versions are consistent with butler
    script_info = scripts.Scripts(cfg)
    db.validate_revisions(script_info.base_revisions())

    command.upgrade(cfg, revision, sql=sql)
