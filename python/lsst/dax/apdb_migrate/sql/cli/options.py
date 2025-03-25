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

import click

from ... import trees

_DEFAULT_MIGRATIONS = trees.MigrationTrees.migrations_folder("sql")


mig_path = click.option(
    "--mig-path",
    type=click.Path(exists=False, file_okay=False, writable=True),
    help=f"Top-level folder with migration scripts, default: {_DEFAULT_MIGRATIONS}",
    metavar="PATH",
    default=_DEFAULT_MIGRATIONS,
)

mig_path_exist = click.option(
    "--mig-path",
    type=click.Path(exists=True, file_okay=False, writable=True),
    help=f"Top-level folder with migration scripts, default: {_DEFAULT_MIGRATIONS}",
    metavar="PATH",
    default=_DEFAULT_MIGRATIONS,
)

stamp_purge = click.option(
    "--purge", help="Remove existing version table before saving new versions.", is_flag=True
)

schema = click.option("-s", "--schema", help="Database schema name.", default=None)

dump_sql = click.option(
    "--sql", help="Offline mode, dump SQL instead of executing migration on a database.", is_flag=True
)
