"""Migration script for schema 1.0.0.

Revision ID: schema_1.0.0
Revises: schema_0.1.1
Create Date: 2025-04-02 10:25:38.628225
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_1.0.0"
down_revision = "schema_0.1.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 0.1.1 to 1.0.0 (DM-42435 and DM-41530).

    Summary of changes on DM-42435:

      - DiaSource and DiaForcedSource table schema changed to use
        (visit: long, detector: short) columns instead of ccdVisitId:
        - ccdVisitId was in PK of DiaForcedSource, need to rebuild PK
        - IDX_DiaSource_ccdVisitId index has to be dropped and replaced by
          IDX_DiaSource_visitDetector
        - same for IDX_DiaForcedSource_visitDetector
      - DetectorVisitProcessingSummary.detector column type changed from long
        to short, but we never had that table in the actual schema yet, nothing
        to do for this change.
      - Instrument table was dropped, but we have not actually created that
        either, nothing to do for it too.

    Summary of changes on DM-41530:

      - Dropped columns DiaObject.flags, DiaSource.flags, DiaForcedSource.flags
        and SSObject.flags
      - Added a number of boolean columns to DiaSource table
    """
    with Context(revision) as ctx:  # noqa: F841
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError("Not implemented yet")
