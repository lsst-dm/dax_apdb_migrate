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

from alembic import command, util
from alembic.script import ScriptDirectory

from ... import revision
from .. import config, migrate

_LOG = logging.getLogger(__name__)


def _revision_exists(scripts: ScriptDirectory, revision: str) -> bool:
    """Check that revision exists."""
    try:
        scripts.get_revisions(revision)
        return True
    except util.CommandError:
        return False


def migrate_revision(mig_path: str, tree_name: str, version: str) -> None:
    """Create new revision.

    Parameters
    ----------
    mig_path : `str`
        Filesystem path to location of revisions.
    tree_name : `str`
        Name of the revision tree.
    version : `str`
        Version string, usually in X.Y.Z format.

    Raises
    ------
    LookupError
        Raised if given revision tree name does not exist.
    """
    # We want to keep trees in separate directories
    migrate_trees = migrate.MigrationTrees(mig_path)
    tree_folder = migrate_trees.version_location(tree_name, relative=False)

    cfg = config.MigAlembicConfig.from_mig_path(mig_path)
    scripts = ScriptDirectory.from_config(cfg)

    # make sure that tree root is defined
    root = revision.rev_id(tree_name)
    if not _revision_exists(scripts, root):
        raise LookupError(f"Revision tree {tree_name!r} does not exist.")
    head = head = f"{tree_name}@head"

    # now can make actual revision
    rev_id = revision.rev_id(tree_name, version)
    message = f"Migration script for {tree_name} {version}."
    command.revision(
        cfg,
        head=head,
        rev_id=rev_id,
        splice=False,
        version_path=tree_folder,
        message=message,
    )
