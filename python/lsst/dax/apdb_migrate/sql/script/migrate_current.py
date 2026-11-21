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
from alembic.script import ScriptDirectory

from ..config import ApdbMigConfigSql
from ..database import Database, MissingMetadataError

_LOG = logging.getLogger(__name__)


def migrate_current(config: str, mig_path: str, verbose: bool, metadata: bool, no_metadata: bool) -> None:
    """Display current revisions for a database.

    Parameters
    ----------
    config : `str`
        Path or URI of APDB configuration file.
    mig_path : `str`
        Filesystem path to location of revisions.
    verbose : `bool`
        Print verbose information if this flag is true.
    metadata : `bool`
        If True then print versions numbers from metadata, otherwise display
        information about alembic revisions.
    no_metadata : `bool`
        If True then allow for missing metadata table (pre-0.1.1 setup).
    """
    db = Database(config, no_metadata)

    cfg = ApdbMigConfigSql.from_mig_path(mig_path, db=db)
    if metadata:
        # Print current versions defined in butler.
        script_info = ScriptDirectory.from_config(cfg)
        heads = script_info.get_heads()
        try:
            tree_versions = db.tree_versions()
        except MissingMetadataError as exc:
            exc.add_note(
                "Check schema name or use --no-metadata option in case this is a pre-historic APDB "
                "instance that is missing metadata table."
            )
            raise
        if tree_versions:
            for tree, (version, rev_id) in sorted(tree_versions.items()):
                head = " (head)" if rev_id in heads else ""
                print(f"{tree}: {version} -> {rev_id}{head}")
        else:
            print("No versions defined in metadata table.")
    else:
        # Revisions from alembic
        command.current(cfg, verbose=verbose)

    # complain if alembic_version table is there but does not match manager
    # versions
    if db.alembic_revisions():
        script_info = ScriptDirectory.from_config(cfg)
        db.validate_revisions(script_info.get_bases())
