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

"""Module defines a set of CLick options common to all backends."""

import click

verbose = click.option("-v", "--verbose", help="Print detailed information.", is_flag=True)

dry_run = click.option("-n", "--dry-run", help="Do not execute actions, only report.", is_flag=True)

options = click.option(
    "-o",
    "--options",
    help="Options to pass to migration scripts, as a key-value pair, can be used more than once.",
    metavar="KEY=VALUE",
    multiple=True,
)

log_level = click.option(
    "--log-level",
    help="Global or per-logger logging level, comma-separated and can be specified multiple times.",
    metavar="LEVEL|LOGGER=LEVEL[,...]",
    multiple=True,
)
