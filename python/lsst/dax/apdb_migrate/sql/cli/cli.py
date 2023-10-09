# This file is part of dax_apdb_migrate.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging
from typing import Any

import click

from .. import script
from . import arguments, options


@click.group()
def cli() -> None:
    """APDB schema migration tools for SQL backend."""
    logging.basicConfig(level=logging.INFO)


@cli.command(short_help="Create new revision tree.")
@options.mig_path_option
@arguments.tree_name_argument
def add_tree(*args: Any, **kwargs: Any) -> None:
    """Create new revision tree for a specified manager type."""
    script.migrate_add_tree(*args, **kwargs)


@cli.command(short_help="Print a list of known revision trees.")
@options.verbose_option
@options.mig_path_exist_option
def show_trees(*args: Any, **kwargs: Any) -> None:
    """Print a list of known revision trees (manager types)."""
    script.migrate_show_trees(*args, **kwargs)
