"""Migration script for schema 1.0.0.

Revision ID: schema_1.0.0
Revises: schema_0.1.1
Create Date: 2024-05-06 14:42:13.640471
"""

import logging
from typing import Any

import sqlalchemy
from alembic import op
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_1.0.0"
down_revision = "schema_0.1.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

ID_MAP_TABLE_NAME = "migration_ccdvisit_map"
"""Name of the temporary ccdVisitId mapping table."""

# Mapping of the flag bit number to its name (not used below) and a new column
# name for that flag.
# This mapping was produced from ap_association/data/association-flag-map.yaml
# in release w_2024_18 (ap_association/g719b61f9b9+c919bfd933).
FLAG_BITS = {
    0: ("base_PixelFlags_flag", "pixelFlags"),
    1: ("base_PixelFlags_flag_offimage", "pixelFlags_offimage"),
    2: ("base_PixelFlags_flag_edge", "pixelFlags_edge"),
    3: ("base_PixelFlags_flag_interpolated", "pixelFlags_interpolated"),
    4: ("base_PixelFlags_flag_saturated", "pixelFlags_saturated"),
    5: ("base_PixelFlags_flag_cr", "pixelFlags_cr"),
    6: ("base_PixelFlags_flag_bad", "pixelFlags_bad"),
    7: ("base_PixelFlags_flag_suspect", "pixelFlags_suspect"),
    8: ("base_PixelFlags_flag_interpolatedCenter", "pixelFlags_interpolatedCenter"),
    9: ("base_PixelFlags_flag_saturatedCenter", "pixelFlags_saturatedCenter"),
    10: ("base_PixelFlags_flag_crCenter", "pixelFlags_crCenter"),
    11: ("base_PixelFlags_flag_suspectCenter", "pixelFlags_suspectCenter"),
    12: ("slot_Centroid_flag", "centroid_flag"),
    # Bits 13 and 14 are obsolete and do not appear in the new schema.
    15: ("slot_ApFlux_flag", "apFlux_flag"),
    16: ("slot_ApFlux_flag_apertureTruncated", "apFlux_flag_apertureTruncated"),
    17: ("slot_PsfFlux_flag", "psfFlux_flag"),
    18: ("slot_PsfFlux_flag_noGoodPixels", "psfFlux_flag_noGoodPixels"),
    19: ("slot_PsfFlux_flag_edge", "psfFlux_flag_edge"),
    20: ("ip_diffim_forced_PsfFlux_flag", "forced_PsfFlux_flag"),
    21: ("ip_diffim_forced_PsfFlux_flag_noGoodPixels", "forced_PsfFlux_flag_noGoodPixels"),
    22: ("ip_diffim_forced_PsfFlux_flag_edge", "forced_PsfFlux_flag_edge"),
    23: ("slot_Shape_flag", "shape_flag"),
    24: ("slot_Shape_flag_no_pixels", "shape_flag_no_pixels"),
    25: ("slot_Shape_flag_not_contained", "shape_flag_not_contained"),
    26: ("slot_Shape_flag_parent_source", "shape_flag_parent_source"),
    27: ("ext_trailedSources_Naive_flag_edge", "trail_flag_edge"),
    28: ("base_PixelFlags_flag_streak", "pixelFlags_streak"),
    29: ("base_PixelFlags_flag_streakCenter", "pixelFlags_streakCenter"),
    30: ("base_PixelFlags_flag_injected", "pixelFlags_injected"),
    31: ("base_PixelFlags_flag_injectedCenter", "pixelFlags_injectedCenter"),
    32: ("base_PixelFlags_flag_injected_template", "pixelFlags_injected_template"),
    33: ("base_PixelFlags_flag_injected_templateCenter", "pixelFlags_injected_templateCenter"),
}

# All new boolean flag columns added to DiaSource table, this is just for
# checking that the above map is complete.
# The list is made from sdm_schemas commit 32101859.
ALL_FLAG_COLUMNS = [
    "centroid_flag",
    "apFlux_flag",
    "apFlux_flag_apertureTruncated",
    "psfFlux_flag",
    "psfFlux_flag_edge",
    "psfFlux_flag_noGoodPixels",
    "trail_flag_edge",
    "forced_PsfFlux_flag",
    "forced_PsfFlux_flag_edge",
    "forced_PsfFlux_flag_noGoodPixels",
    "shape_flag",
    "shape_flag_no_pixels",
    "shape_flag_not_contained",
    "shape_flag_parent_source",
    "pixelFlags",
    "pixelFlags_bad",
    "pixelFlags_cr",
    "pixelFlags_crCenter",
    "pixelFlags_edge",
    "pixelFlags_interpolated",
    "pixelFlags_interpolatedCenter",
    "pixelFlags_offimage",
    "pixelFlags_saturated",
    "pixelFlags_saturatedCenter",
    "pixelFlags_suspect",
    "pixelFlags_suspectCenter",
    "pixelFlags_streak",
    "pixelFlags_streakCenter",
    "pixelFlags_injected",
    "pixelFlags_injectedCenter",
    "pixelFlags_injected_template",
    "pixelFlags_injected_templateCenter",
]


def upgrade() -> None:
    """Upgrade 'schema' tree from 0.1.1 to 1.0.0. This includes changes on
    tickets DM-42435 and DM-41530.

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
    ctx = Context()

    instrument_name = ctx.get_mig_option("instrument")
    repo = ctx.get_mig_option("butler-repo")
    if not instrument_name or not repo:
        raise ValueError(
            "This migration script requires butler repository abd instrument name. "
            "Please use `--options butler-repo=REPO --options instrument=NAME` command line options."
        )

    _check_insert_ids(ctx)
    _check_flags()

    cdVisitIds = _query_ccd_visit_ids(ctx)
    _LOG.info("Found %s distinct ccdVisitIds", len(cdVisitIds))

    # Find the (un)packer for this instrument.
    packer = _make_packer(repo, instrument_name)

    # Make and fill a table mapping ccdVisit to visit+detector
    _make_map_table(ctx, cdVisitIds, packer)

    # Add and fill visit + detector columns
    _add_visit_detector_columns(ctx)

    # Drop old column and update indices.
    _drop_ccd_visit(ctx)

    # Drop mapping table.
    _LOG.info("Dropping %s table", ID_MAP_TABLE_NAME)
    op.drop_table(ID_MAP_TABLE_NAME, schema=ctx.schema)

    # Add all new flag columns to DiaSource and fill them.
    _add_flag_columns(ctx)

    # Drop old flags columns from all tables.
    _drop_flag_columns(ctx)

    # Update metadata version.
    version = revision.split("_")[-1]
    ctx.apdb_meta.update_tree_version("schema", version)


def downgrade() -> None:
    """Downgrade would be too complicated for this migration."""
    raise NotImplementedError()


def _check_insert_ids(ctx: Context) -> None:
    """Check that we do not have any InsertId tables, those need to be
    removed manually.
    """
    inspector = sqlalchemy.inspect(ctx.bind)
    names = inspector.get_table_names(schema=ctx.schema)
    _LOG.info("All table names: %s", names)
    names = [name for name in names if name.endswith("InsertId")]
    if names:
        raise RuntimeError(
            f"Schema contains InsertId tables, they have to be removed manually prior to migration: {names}"
        )


def _query_ccd_visit_ids(ctx: Context) -> set[int]:
    """Make a list of all ccdVisitId values existing in the source tables."""
    src = ctx.get_table("DiaSource")
    fsrc = ctx.get_table("DiaForcedSource")

    q1 = sqlalchemy.select(src.columns["ccdVisitId"]).distinct()
    q2 = sqlalchemy.select(fsrc.columns["ccdVisitId"]).distinct()
    result = ctx.bind.execute(q1.union(q2))
    return set(result.scalars())


def _make_packer(repo: str, instrument: str) -> Any:
    """Find Butler dimensions packer."""
    # This package does not depend on daf_butler, there is a chance that butler
    # is not setup I do not want import of this module fail (alembic will be
    # unhappy) so I delay import until this moment.
    try:
        from lsst.daf.butler import Butler
        from lsst.obs.base import Instrument
    except ImportError:
        raise ImportError("Module lsst.obs.base cannot be imported, please setup obs_base.") from None

    butler = Butler(repo)  # type: ignore[abstract]
    try:
        data_ids = list(
            butler.registry.queryDataIds("instrument", dataId={"instrument": instrument}).expanded()
        )
    except Exception as exc:
        # I do not want to import butler exception classes.
        raise ValueError(f"Butler exception, likely due to incorrect instrument name: {instrument}") from exc
    if not data_ids:
        raise ValueError(f"Cannot find instrument {instrument} in Butler.")
    data_id = data_ids[0]
    _LOG.info("Found instrument: %s", data_id)

    try:
        packer = Instrument.make_default_dimension_packer(data_id=data_id, is_exposure=False)
    except ImportError as exc:
        raise ImportError(
            "Import of instrument failed, likely additional package setup is needed. "
            "Check exception above for instrument class."
        ) from exc
    return packer


def _make_map_table(ctx: Context, cdVisitIds: set[int], packer: Any) -> None:
    """Create and fill ccdVisit -> (visit, detector) mapping table."""
    _LOG.info("creating %s table", ID_MAP_TABLE_NAME)
    op.create_table(
        ID_MAP_TABLE_NAME,
        sqlalchemy.schema.Column("ccdVisitId", sqlalchemy.BigInteger, primary_key=True, autoincrement=False),
        sqlalchemy.schema.Column("visit", sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.schema.Column("detector", sqlalchemy.SmallInteger, nullable=False),
        schema=ctx.schema,
    )

    table = ctx.get_table(ID_MAP_TABLE_NAME)

    count = 0
    batch: list[dict[str, Any]] = []
    for ccdVisitId in cdVisitIds:
        data_id = packer.unpack(ccdVisitId)
        visit = data_id["visit"]
        detector = data_id["detector"]
        batch.append({"ccdVisitId": ccdVisitId, "visit": visit, "detector": detector})

        if len(batch) >= 10000:
            op.bulk_insert(table, batch, multiinsert=True)
            _LOG.debug("inserted %s rows", len(batch))
            count += len(batch)
            batch = []

    if batch:
        op.bulk_insert(table, batch, multiinsert=True)
        _LOG.debug("inserted %s rows", len(batch))
        count += len(batch)
        batch = []

    _LOG.info("inserted total %s rows into %s", count, ID_MAP_TABLE_NAME)


def _add_visit_detector_columns(ctx: Context) -> None:
    """Add visit and detector columns to each table and populate."""
    # Reset metadata cache just in case.
    ctx.metadata.clear()
    map_table = ctx.get_table(ID_MAP_TABLE_NAME)

    for table_name in ("DiaSource", "DiaForcedSource"):
        _LOG.info("Filling new columns in %s table", table_name)

        with ctx.batch_alter_table(table_name) as batch_op:
            # New columns are nullable initially, will change after fill.
            batch_op.add_column(sqlalchemy.Column("visit", sqlalchemy.types.BigInteger, nullable=True))
            batch_op.add_column(sqlalchemy.Column("detector", sqlalchemy.types.SmallInteger, nullable=True))

        # Fill new columns. I could not make SQLAlchemy to generate a
        # correlated query updating two columns at the same time, so I have to
        # do it in two scalar queries.
        table = ctx.get_table(table_name)
        subq = sqlalchemy.select(map_table.columns["visit"]).where(
            map_table.columns["ccdVisitId"] == table.columns["ccdVisitId"]
        )
        sql = table.update().values(visit=subq.scalar_subquery())
        ctx.bind.execute(sql)
        subq = sqlalchemy.select(map_table.columns["detector"]).where(
            map_table.columns["ccdVisitId"] == table.columns["ccdVisitId"]
        )
        sql = table.update().values(detector=subq.scalar_subquery())
        ctx.bind.execute(sql)

        # Change both columns to be NOT NULL
        with ctx.batch_alter_table(table_name) as batch_op:
            batch_op.alter_column("visit", nullable=False)
            batch_op.alter_column("detector", nullable=False)


def _drop_ccd_visit(ctx: Context) -> None:
    """Drop ccdVisitId column from source tables."""
    with ctx.batch_alter_table("DiaSource") as batch_op:
        # Drop old index.
        _LOG.info("Drop ccdVistiId index and column from DiaSource")
        batch_op.drop_index("IDX_DiaSource_ccdVisitId")  # type: ignore[attr-defined]
        batch_op.drop_column("ccdVisitId")  # type: ignore[attr-defined]
        # Create new index.
        _LOG.info("Add IDX_DiaSource_visitDetector index")
        batch_op.create_index("IDX_DiaSource_visitDetector", ["visit", "detector"])

    with ctx.batch_alter_table("DiaForcedSource") as batch_op:
        # ccdVisitId is in PK. Postgres drops PK when the column is dropped,
        # but sqlite complains that column cannot be dropped if it's in PK.
        # The workaround is to create PK with new columns, but Postgres also
        # requires existing PK to be dropped first, and in sqlite we do not
        # even have name for PK constraint, so it cannot be dropped explicitly.
        if ctx.is_postgres:
            _LOG.info("Dropping DiaForcedSource_pkey constraint")
            batch_op.drop_constraint("DiaForcedSource_pkey")  # type: ignore[attr-defined]
        _LOG.info("Add DiaForcedSource_pkey constraint")
        batch_op.create_primary_key(  # type: ignore[attr-defined]
            "DiaForcedSource_pkey", ["diaObjectId", "visit", "detector"]
        )
        # Drop old index.
        _LOG.info("Drop ccdVisitId index and column from DiaForcedSource")
        batch_op.drop_index("IDX_DiaForcedSource_ccdVisitId")  # type: ignore[attr-defined]
        batch_op.drop_column("ccdVisitId")  # type: ignore[attr-defined]
        # Create new index.
        _LOG.info("Add IDX_DiaForcedSource_visitDetector index")
        batch_op.create_index("IDX_DiaForcedSource_visitDetector", ["visit", "detector"])


def _check_flags() -> None:
    """Check that contents of FLAG_BITS is consistent with ALL_FLAG_COLUMNS."""
    flag_bits_columns = set(names[1] for names in FLAG_BITS.values())
    all_flag_columns = set(ALL_FLAG_COLUMNS)
    if flag_bits_columns != all_flag_columns:
        diff1 = flag_bits_columns - all_flag_columns
        diff2 = all_flag_columns - flag_bits_columns
        message = "Flag column lists do not match."
        if diff1:
            message += f"\n    Columns in FLAG_BITS only: {diff1}."
        if diff2:
            message += f"\n    Columns in ALL_FLAG_COLUMNS only: {diff2}."
        raise ValueError(message)


def _add_flag_columns(ctx: Context) -> None:
    """Add all new flag columns to DiaSource table."""
    # Reset metadata cache just in case.
    ctx.metadata.clear()

    columns = [names[1] for names in FLAG_BITS.values()]
    _LOG.info("Adding flag columns to DiaSource: %s", columns)
    with ctx.batch_alter_table("DiaSource") as batch_op:
        for column in columns:
            batch_op.add_column(sqlalchemy.Column(column, sqlalchemy.types.Boolean, nullable=True))

    # Now fill all of them from the `flags` column in one UPDATE query.
    _LOG.info("Filling DiaSource flag columns from `flags`")
    table = ctx.get_table("DiaSource")
    values = {}
    for bit, (flag_name, column) in FLAG_BITS.items():
        mask = 0x1 << bit
        values[column] = (table.columns["flags"].bitwise_and(mask)) != 0
    query = table.update().values(**values)
    _LOG.debug("update query: %s", query)
    ctx.bind.execute(query)


def _drop_flag_columns(ctx: Context) -> None:
    """Drop "flags" column from all tables."""
    tables = ["DiaObject", "DiaSource", "DiaForcedSource", "SSObject"]
    for table in tables:
        _LOG.info("Dropping `flags` column from %s table", table)
        with ctx.batch_alter_table(table) as batch_op:
            batch_op.drop_column("flags")  # type: ignore[attr-defined]
