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

import uuid

NS_UUID = uuid.UUID("840b31d9-05cd-5161-b2c8-00d32b280d0f")
"""Namespace UUID used for UUID5 generation. Do not change. This was
produced by `uuid.uuid5(uuid.NAMESPACE_DNS, "lsst.org")`.
"""


def rev_id(*args: str) -> str:
    """Generate revision ID from arguments.

    Parameters
    ----------
    *args : str
        Set of arbitrary strings, at least one argument is required.

    Returns
    -------
    rev_id : `str`
        Revision ID, character string. It is either a concatenation of all
        arguments separated by underscores, or, if the total length of
        concatenated string is longer than 32 characters, first 12 characters
        of the hash of that string.
    """
    if len(args) == 1:
        result = args[0] + "_root"
    else:
        result = "_".join(args)
    if len(result) > 32:
        result = uuid.uuid5(NS_UUID, result).hex[-12:]
    return result
