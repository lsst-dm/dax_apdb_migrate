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

from alembic.script import ScriptDirectory

from .. import config, database

_LOG = logging.getLogger(__name__)


def migrate_current(host: str, port: int | None, keyspace: str, mig_path: str, verbose: bool) -> None:
    """Display current revisions for a database.

    Parameters
    ----------
    host : `str`
        Name of the Cassandra host to connect to.
    port : `int`, optional
        Port number.
    keyspace : `str`
        Cassandra keyspace name.
    mig_path : `str`
        Filesystem path to location of revisions.
    verbose : `bool`
        Print verbose information if this flag is true.
    """
    db = database.Database(host, keyspace, port)

    cfg = config.ApdbMigConfigCassandra.from_mig_path(mig_path)

    # Print current versions defined in butler.
    script_info = ScriptDirectory.from_config(cfg)
    heads = script_info.get_heads()
    tree_versions = db.tree_versions()
    if tree_versions:
        for tree, (version, rev_id) in sorted(tree_versions.items()):
            head = " (head)" if rev_id in heads else ""
            print(f"{tree}: {version} -> {rev_id}{head}")
    else:
        print("No versions defined in metadata table.")
