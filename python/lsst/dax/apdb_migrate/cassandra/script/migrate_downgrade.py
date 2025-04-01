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

from alembic import command

from .. import config, database

_LOG = logging.getLogger(__name__)


def migrate_downgrade(
    host: str,
    port: int | None,
    keyspace: str,
    revision: str,
    mig_path: str,
    dry_run: bool,
    options: dict[str, str] | None,
) -> None:
    """Downgrade schema to a specified revision.

    Parameters
    ----------
    host : `str`
        Name of the Cassandra host to connect to.
    port : `int`, optional
        Port number.
    keyspace : `str`
        Cassandra keyspace name.
    revision : `str`
        Target revision or colon-separated range for sql mode.
    mig_path : `str`
        Filesystem path to location of revisions.
    dry_run : `bool`
        If True dump queries instead of executing migration on a database.
    options : `dict` or `None`
        Additional key:value options specified on command line
    """
    db = database.Database(host, keyspace, port)

    cfg = config.ApdbMigConfigCassandra.from_mig_path(
        mig_path, db=db, migration_options=options, dry_run=dry_run
    )

    command.downgrade(cfg, revision)
