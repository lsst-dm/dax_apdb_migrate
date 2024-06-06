"""Migration script for schema 2.0.0.

Revision ID: schema_2.0.0
Revises: schema_1.1.0
Create Date: 2024-06-05 11:26:16.103693
"""

import logging

import alembic
import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_2.0.0"
down_revision = "schema_1.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 1.1.0 to 2.0.0 (ticket DM-44620).

    Summary of changes:

      - Drop x/y columns from DiaForcedSource table
      - Add ra/dec columns to DiaForcedSource table
      - Populate new ra/dec columns from their matching DiaObject values.
    """
    ctx = Context()

    # Alter table schema.
    _LOG.info("Dropping and adding columns to DiaForcedSource table.")
    fsources = ctx.get_table("DiaForcedSource")
    with ctx.batch_alter_table("DiaForcedSource", copy_from=fsources) as batch_op:
        batch_op.drop_column("x")
        batch_op.drop_column("y")
        # ra/dec are initially nullable, will make them not-null after filling.
        batch_op.add_column(sqlalchemy.Column("ra", sqlalchemy.types.Double, nullable=True))
        batch_op.add_column(sqlalchemy.Column("dec", sqlalchemy.types.Double, nullable=True))

    # To populate ra/dec we need to find matching DiaObject and use its ra/dec.
    # Matching DiaObject is the one with the latest validityStart which is
    # still earlier than source processing time. Complication here is that for
    # some sources their processing time happens few milliseconds earlier than
    # earliest validityStart of matching DiaObject. In that case we take
    # matching DiaObject with earliest validityStart.

    # The query to do that is rather complicated, split it into CTEs.
    objects = ctx.get_table("DiaObject")
    fsources = ctx.get_table("DiaForcedSource", reload=True)
    # In offline mode reflected schema is wrong, need a small fix.
    if alembic.context.is_offline_mode():
        fsources.append_column(sqlalchemy.Column("ra", sqlalchemy.types.Double, nullable=True))
        fsources.append_column(sqlalchemy.Column("dec", sqlalchemy.types.Double, nullable=True))

    # Scalar subquery for matching object
    o1 = objects.alias("o1")
    f1 = fsources.alias("f1")
    # Sub-query to calculate latest validityStart earlier that source.
    max_validity = (
        sqlalchemy.select(sqlalchemy.func.max(o1.columns.validityStart))
        .where(
            sqlalchemy.and_(
                o1.columns.diaObjectId == f1.columns.diaObjectId,
                o1.columns.validityStart <= f1.columns.time_processed,
            )
        )
        .scalar_subquery()
        .correlate(f1)
    )
    # Sub-query to calculate earliest validityStart.
    min_validity = (
        sqlalchemy.select(sqlalchemy.func.min(o1.columns.validityStart))
        .where(o1.columns.diaObjectId == f1.columns.diaObjectId)
        .scalar_subquery()
        .correlate(f1)
    )
    f2o = sqlalchemy.select(
        f1.columns.diaForcedSourceId,
        f1.columns.diaObjectId,
        sqlalchemy.func.coalesce(max_validity, min_validity).label("validityStart"),
    ).cte("f2o")

    o2 = objects.alias("o1")
    f2radec = (
        sqlalchemy.select(f2o.columns.diaForcedSourceId, o2.columns.ra, o2.columns.dec)
        .select_from(
            o2.join(
                f2o,
                sqlalchemy.and_(
                    o2.columns.diaObjectId == f2o.columns.diaObjectId,
                    o2.columns.validityStart == f2o.columns.validityStart,
                ),
            )
        )
        .cte("f2radec")
    )

    # Now we are ready for update
    update = (
        fsources.update()
        .values(ra=f2radec.columns.ra, dec=f2radec.columns.dec)
        .where(f2radec.columns.diaForcedSourceId == fsources.columns.diaForcedSourceId)
    )
    _LOG.info("Filling ra/dec columns in DiaForcedSource table.")
    _LOG.debug("update: %s", update)
    result = ctx.bind.execute(update)
    if not alembic.context.is_offline_mode():
        _LOG.info("Updated %s rows in DiaForcedSource table.", result.rowcount)

    # Chech that all ra/dec are filled.
    query = sqlalchemy.select(fsources.columns.diaForcedSourceId).where(
        fsources.columns.ra == None  # noqa: E711
    )
    result = ctx.bind.execute(query)
    ids = list(result.scalars())
    if ids:
        _LOG.error("Some ra/dec are not filled, count: %s", len(ids))
        _LOG.error("ids: %s", ids[:10])
        raise RuntimeError("cannot continue")

    # Make ra/dec columns not null.
    _LOG.info("Making ra/dec columns non-nullable.")
    with ctx.batch_alter_table("DiaForcedSource", copy_from=fsources) as batch_op:
        batch_op.alter_column("ra", nullable=False)
        batch_op.alter_column("dec", nullable=False)

    # Update metadata version.
    tree, _, version = revision.partition("_")
    ctx.apdb_meta.update_tree_version(tree, version)


def downgrade() -> None:
    """Downgrade is not implemented as it is impossible to recover x/y
    values after upgrade.
    """
    raise NotImplementedError()
