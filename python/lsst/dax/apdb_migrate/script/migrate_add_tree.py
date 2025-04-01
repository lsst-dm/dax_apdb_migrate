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

from alembic import command

from .. import config, revision
from ..trees import MigrationTrees

_LOG = logging.getLogger(__name__)


def migrate_add_tree(tree_name: str, mig_path: str, template: str = "generic") -> None:
    """Add one more revision tree.

    Parameters
    ----------
    tree_name : `str`
        Name of the revision tree.
    mig_path : `str`
        Filesystem path to location of revisions.
    template : `str`
        NAme of the templates to use.

    Raises
    ------
    ValueError
        Raised if given revision tree name already exists.

    Notes
    -----
    Migrations are stored in per-tree directories just below the top-level
    migration folder, e.g. "migrations/sql/tree1".

    This call generates a root revision for a tree. Revision ID of the root is
    generated based on tree name. For all trees the root of the tree is
    labelled with the tree name.
    """
    if "/" in tree_name:
        raise ValueError("Regular tree name cannot have slash character: f{tree_name}.")

    trees = MigrationTrees(mig_path=mig_path)

    # check that its folder does not exist yet
    tree_folder = trees.version_location(tree_name, relative=False)
    if os.access(tree_folder, os.F_OK):
        raise ValueError(f"Version tree {tree_name!r} already exists in {tree_folder}")

    cfg = config.ApdbMigConfig(mig_path, single_tree=tree_name)

    # may need to initialize the whole shebang
    alembic_folder = trees.alembic_folder(relative=False)
    if not os.access(alembic_folder, os.F_OK):
        _LOG.debug("Creating new alembic folder %r", alembic_folder)

        # `init` comand requires cfg.config_file_name.
        cfg.config_file_name = os.path.join(alembic_folder, "alembic.ini")

        # initialize tree folder
        command.init(cfg, directory=alembic_folder, template=template)

        # As we don't use config file, just drop it to avoid confusion.
        os.unlink(cfg.config_file_name)
        cfg.config_file_name = None

        # It also creates `versions` folder that we do not use, drop it too.
        os.rmdir(os.path.join(alembic_folder, "versions"))

    # create initial branch revision in a separate folder
    message = f"This is an initial pseudo-revision of the {tree_name!r} tree."
    rev_id = revision.rev_id(tree_name)
    command.revision(
        cfg, head="base", rev_id=rev_id, branch_label=tree_name, version_path=tree_folder, message=message
    )
