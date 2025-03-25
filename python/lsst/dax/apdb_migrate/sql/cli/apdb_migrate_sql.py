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
from .. import script
from . import options

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
@options.log_level
def main(log_level: Iterable[str]) -> None:
    """APDB schema migration tools for SQL backend."""
    init_logging(log_level)


@main.command(short_help="Create new revision tree.")
@options.mig_path
@click.argument("tree-name")
def add_tree(*args: Any, **kwargs: Any) -> None:
    """Create new revision tree for a specified manager type.

    TREE_NAME argument provides the name of the new revision tree, it cannot
    include slash or special characters.
    """
    script.migrate_add_tree(*args, **kwargs)


@main.command(short_help="Create migration script for a new revision.")
@options.mig_path_exist
@click.argument("tree-name")
@click.argument("version")
def add_revision(*args: Any, **kwargs: Any) -> None:
    """Create new revision and its migration script.

    TREE_NAME argument provides the name of the revision tree.
    VERSION specifies new version string in MAJOR.MINOR.PATCH format.
    """
    script.migrate_revision(*args, **kwargs)


@main.command(short_help="Display current revisions for a database.")
@options.verbose
@click.option(
    "-m",
    "--metadata",
    help=(
        "Display version numbers from metadata table. By default revisions from alembic_version "
        "table are displayed, if that table does not exist the output will be empty."
    ),
    is_flag=True,
)
@options.mig_path_exist
@options.schema
@click.argument("db-url")
def show_current(*args: Any, **kwargs: Any) -> None:
    """Display current revisions from either alembic_version or metadata
    tables.

    DB_URL specifies APDB database connection URL.
    """
    script.migrate_current(*args, **kwargs)


@main.command(short_help="Show revision history.")
@options.verbose
@options.mig_path_exist
@click.argument("tree-name", required=False)
def show_history(*args: Any, **kwargs: Any) -> None:
    """Display revision history.

    Optional TREE_NAME arguments specifies tree for which to display history.
    """
    script.migrate_history(*args, **kwargs)


@main.command(short_help="Print a list of known revision trees.")
@options.verbose
@options.mig_path_exist
def show_trees(*args: Any, **kwargs: Any) -> None:
    """Print a list of known revision trees (manager types)."""
    script.migrate_show_trees(*args, **kwargs)


@main.command(short_help="Stamp alembic revision table with current metadata versions.")
@options.mig_path_exist
@options.stamp_purge
@options.dry_run
@options.schema
@click.argument("db-url")
@click.argument("tree-name", required=False)
def stamp(*args: Any, **kwargs: Any) -> None:
    """Stamp Alembic revision table (alembic_version) with current manager
    versions from butler_attributes.

    DB_URL specifies APDB database connection URL.
    Optional TREE_NAME argument specifies tree for which to stamp revision,
    by default all trees are stamped.
    """
    script.migrate_stamp(*args, **kwargs)


@main.command(short_help="Upgrade schema to a specified revision.")
@options.mig_path_exist
@options.dump_sql
@options.schema
@options.options
@click.argument("db-url")
@click.argument("revision")
def upgrade(*args: Any, **kwargs: Any) -> None:
    """Upgrade schema to a specified revision.

    DB_URL specifies APDB database connection URL.
    REVISION is a target alembic revision, in offline mode it can also specify
    the initial revision using INITIAL:TARGET format.
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
@options.dump_sql
@options.schema
@options.options
@click.argument("db-url")
@click.argument("revision")
def downgrade(*args: Any, **kwargs: Any) -> None:
    """Downgrade schema to a specified revision.

    DB_URL specifies APDB database connection URL.
    REVISION is a target alembic revision, in offline mode it can also specify
    the initial revision using INITIAL:TARGET format.
    """
    # Convert list of key=value to dict
    options = {}
    for option in kwargs["options"]:
        key, _, value = option.partition("=")
        options[key] = value
    kwargs["options"] = options

    script.migrate_downgrade(*args, **kwargs)
