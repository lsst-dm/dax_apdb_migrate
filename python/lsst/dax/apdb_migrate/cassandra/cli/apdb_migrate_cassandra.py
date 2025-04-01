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

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import click

from ... import init_logging
from ... import script as common_script
from ...cli import common_options
from .. import script
from . import options

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
@common_options.log_level
def main(log_level: Iterable[str]) -> None:
    """APDB schema migration tools for Cassandra backend."""
    init_logging(log_level)


@main.command(short_help="Create new revision tree.")
@options.mig_path
@click.argument("tree-name")
def add_tree(*args: Any, **kwargs: Any) -> None:
    """Create new revision tree for a specified manager type.

    TREE_NAME argument provides the name of the new revision tree, it cannot
    include slash or special characters.
    """
    kwargs = dict(kwargs, template="cassandra")
    common_script.migrate_add_tree(*args, **kwargs)


@main.command(short_help="Create migration script for a new revision.")
@options.mig_path_exist
@click.argument("tree-name")
@click.argument("version")
def add_revision(*args: Any, **kwargs: Any) -> None:
    """Create new revision and its migration script.

    TREE_NAME argument provides the name of the revision tree.
    VERSION specifies new version string in MAJOR.MINOR.PATCH format.
    """
    common_script.migrate_revision(*args, **kwargs)


@main.command(short_help="Show revision history.")
@common_options.verbose
@options.mig_path_exist
@click.argument("tree-name", required=False)
def show_history(*args: Any, **kwargs: Any) -> None:
    """Display revision history.

    Optional TREE_NAME arguments specifies tree for which to display history.
    """
    common_script.migrate_history(*args, **kwargs)


@main.command(short_help="Print a list of known revision trees.")
@common_options.verbose
@options.mig_path_exist
def show_trees(*args: Any, **kwargs: Any) -> None:
    """Print a list of known revision trees (manager types)."""
    common_script.migrate_show_trees(*args, **kwargs)


@main.command(short_help="Display current revisions for a database.")
@common_options.verbose
@options.mig_path_exist
@options.port
@click.argument("host")
@click.argument("keyspace")
def show_current(*args: Any, **kwargs: Any) -> None:
    """Display current revisions stored in metadata table.

    HOST specifies Cassandra host name to connect to.
    KEYSPACE specifies Cassandra keyspace name.
    """
    script.migrate_current(*args, **kwargs)


@main.command(short_help="Upgrade schema to a specified revision.")
@options.mig_path_exist
@options.port
@common_options.dry_run
@common_options.options
@click.argument("host")
@click.argument("keyspace")
@click.argument("revision")
def upgrade(*args: Any, **kwargs: Any) -> None:
    """Upgrade schema to a specified revision.

    HOST specifies Cassandra host name to connect to.
    KEYSPACE specifies Cassandra keyspace name.
    REVISION is a target revision name.
    """
    # Convert list of key=value to dict
    options = {}
    for option in kwargs["options"]:
        key, _, value = option.partition("=")
        options[key] = value
    kwargs["options"] = options

    script.migrate_upgrade(*args, **kwargs)


@main.command(short_help="Downgrade schema to a specified revision.")
@options.mig_path_exist
@options.port
@common_options.dry_run
@common_options.options
@click.argument("host")
@click.argument("keyspace")
@click.argument("revision")
def downgrade(*args: Any, **kwargs: Any) -> None:
    """Downgrade schema to a specified revision.

    HOST specifies Cassandra host name to connect to.
    KEYSPACE specifies Cassandra keyspace name.
    REVISION is a target revision name.
    """
    # Convert list of key=value to dict
    options = {}
    for option in kwargs["options"]:
        key, _, value = option.partition("=")
        options[key] = value
    kwargs["options"] = options

    script.migrate_downgrade(*args, **kwargs)
