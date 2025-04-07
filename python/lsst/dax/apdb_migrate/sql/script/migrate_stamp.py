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

"""Dump configuration."""

from __future__ import annotations

import logging

from alembic import command
from alembic.script import ScriptDirectory

from ... import revision
from .. import config, database

_LOG = logging.getLogger(__name__)


def migrate_stamp(
    db_url: str, schema: str | None, mig_path: str, purge: bool, dry_run: bool, tree_name: str | None
) -> None:
    """Stamp alembic revision table with current metadata versions.

    Parameters
    ----------
    db_url : `str`
        Database URL.
    schema : `str` or `None`
        Database schema name.
    mig_path : `str`
        Filesystem path to location of revisions.
    purge : `bool`
        Delete all entries in the version table before stamping.
    dry_run : `bool`
        Skip all updates.
    tree_name: `str`, Optional
        Name of the tree to stamp, if `None` then all managers are stamped.
    """
    db = database.Database(db_url, schema)

    tree_versions = db.tree_versions()
    if not tree_versions:
        # Means that metadata table exists but is empty?
        raise ValueError("No versions defined in metadata table.")

    revisions: dict[str, str] = {}
    for tree, (version, rev_id) in tree_versions.items():
        _LOG.debug("found revision (%s, %s) -> %s", tree, version, rev_id)
        revisions[tree] = rev_id

    cfg: config.ApdbMigConfigSql | None = None
    if tree_name:
        if tree_name in revisions:
            revisions = {tree_name: revisions[tree_name]}
        else:
            # If specified manager not in the database, it may mean that an
            # initial "tree-root" revision needs to be added to alembic
            # table, if that manager is defined in the migration trees.
            cfg = config.ApdbMigConfigSql.from_mig_path(mig_path, db=db)
            script_info = ScriptDirectory.from_config(cfg)
            base_revision = revision.rev_id(tree_name)
            if base_revision not in script_info.get_bases():
                raise ValueError(f"Unknown tree name {tree_name} (not in the database or migrations)")
            revisions = {tree_name: base_revision}

    if dry_run:
        print("Will store these revisions in alembic version table:")
        for tree, rev_id in revisions.items():
            print(f"  {tree}: {rev_id}")
    else:
        if cfg is None:
            cfg = config.ApdbMigConfigSql.from_mig_path(mig_path, db=db)
        for rev in revisions.values():
            command.stamp(cfg, rev, purge=purge)
