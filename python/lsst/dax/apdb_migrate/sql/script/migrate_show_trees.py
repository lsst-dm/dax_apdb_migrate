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

from alembic.script import Script, ScriptDirectory

from .. import config


def migrate_show_trees(mig_path: str, verbose: bool) -> None:
    """Print list of known revision trees.

    Parameters
    ----------
    mig_path : `str`
        Filesystem path to location of revision trees.
    verbose : `bool`
        Print verbose information if this flag is true.
    """
    # import pdb; pdb.set_trace()
    cfg = config.MigAlembicConfig.from_mig_path(mig_path)
    scripts = ScriptDirectory.from_config(cfg)
    bases = scripts.get_bases()

    bases_map: dict[str, Script] = {}
    for name in bases:
        revision = scripts.get_revision(name)
        assert revision is not None, "Script for a known base must exist"
        # Base revision has a random ID but its branch label is "<tree>"
        branches = revision.branch_labels
        if not branches:
            branch = name
        elif len(branches) == 1:
            branch = branches.pop()
        else:
            # Multiple branch labels, usually means that there is one
            # branch and "manager-ClassName" branch label "leaked" to the
            # root. Just use shortest name, that should be sufficient.
            _, branch = min((len(label), label) for label in branches)
        bases_map[branch] = revision

    for branch, revision in sorted(bases_map.items()):
        if verbose:
            print(revision.log_entry)
        else:
            print(branch)
