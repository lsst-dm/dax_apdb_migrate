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

__all__ = ["init_logging"]

import logging
from collections.abc import Iterable

_log_format = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def init_logging(args: Iterable[str]) -> None:
    """Configure Python logging based on command line options."""
    global_level = logging.INFO
    # Silence alembic by default
    logger_levels: dict[str, int] = {"alembic": logging.WARNING}
    for level_str in args:
        for spec in level_str.split(","):
            logger_name, sep, level_name = spec.rpartition("=")
            level = logging.getLevelNamesMapping().get(level_name.upper())
            if level is None:
                raise ValueError(f"Unknown logging level {level_name!r} in {level_str!r}")
            if logger_name:
                logger_levels[logger_name] = level
            else:
                global_level = level

    logging.basicConfig(level=global_level, format=_log_format)
    for logger_name, level in logger_levels.items():
        logging.getLogger(logger_name).setLevel(level)
