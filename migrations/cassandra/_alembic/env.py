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

from typing import cast

from alembic import context
from lsst.dax.apdb_migrate.cassandra.config import ApdbMigConfigCassandra

# This is the Alembic Config object, it must be ApdbMigConfigCassandra
# instance.
config = cast(ApdbMigConfigCassandra, context.config)


def run_migrations() -> None:
    """Run migrations.

    This creates a temporary in-memory table for alembic so that it knows
    what are current revisions in Cassandra. Dry-run option is handled by
    the Context class.
    """
    assert config.db is not None
    engine = config.db.make_alembic_db()
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=None)

        with context.begin_transaction():
            context.run_migrations()


run_migrations()
