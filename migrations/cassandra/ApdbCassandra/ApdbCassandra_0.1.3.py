"""Migration script for ApdbCassandra 0.1.3.

Revision ID: ApdbCassandra_0.1.3
Revises: ApdbCassandra_0.1.2
Create Date: 2025-07-23 16:51:40.237746
"""

import json
import logging
import re

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "ApdbCassandra_0.1.3"
down_revision = "ApdbCassandra_0.1.2"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

METADATA_KEY = "config:time-partition-range.json"
TABLE_MATCH_RE = re.compile(r"^DiaSource_(\d+)$")


def upgrade() -> None:
    """Upgrade 'ApdbCassandra' tree from 0.1.2 to 0.1.3 (ticket DM-51824).

    Summary of changes:
      - Add current time partition configuration to metadata table.
    """
    with Context(revision) as ctx:
        # Check that instance uses partitioned tables.
        frozen_config = ctx.get_apdb_config()
        if not frozen_config["time_partition_tables"]:
            _LOG.info("This instance does not use per-partition tables, nothing to do.")
            return

        # Look for DiaSource_* tables.
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
        result = ctx.query(query, (ctx.keyspace,))
        partitions = []
        for row in result:
            if match := TABLE_MATCH_RE.match(row[0]):
                partitions.append(int(match.group(1)))
        if not partitions:
            raise RuntimeError("Cannot find any partitioned DiaSource tables.")
        start = min(partitions)
        end = max(partitions)

        meta_value = json.dumps({"start": start, "end": end})
        _LOG.info("Adding metadata entry: %s = %s.", METADATA_KEY, meta_value)
        ctx.metadata.insert(METADATA_KEY, meta_value)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        # Check that instance uses partitioned tables.
        frozen_config = ctx.get_apdb_config()
        if not frozen_config["time_partition_tables"]:
            _LOG.info("This instance does not use per-partition tables, nothing to do.")
            return

        _LOG.info("Removing metadata key %s.", METADATA_KEY)
        ctx.metadata.delete(METADATA_KEY)
