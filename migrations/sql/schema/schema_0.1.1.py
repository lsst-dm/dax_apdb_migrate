"""Migration script for schema 0.1.1.

Revision ID: schema_0.1.1
Revises: schema_0.1.0
Create Date: 2023-11-15 22:38:14.292151
"""

import logging

import sqlalchemy
from alembic import op
from lsst.dax.apdb_migrate.sql.context import Context
from sqlalchemy.schema import Column

# revision identifiers, used by Alembic.
revision = "schema_0.1.1"
down_revision = "schema_0.1.0"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade 'schema' tree from 0.1.0 to 0.1.1.

    Version 0.1.0 corresponds to a number of different schemas that all existed
    before versioning was implemented for APDB. The code tries to guess all
    necessary changes to bring all those schemas into a state corresponding to
    0.1.1 version.

    Summary of changes:

      - Add table `metadata` with columns `name` and `value`
      - Add two records to that table:
        - "version:schema": "0.1.1"
        - "version:ApdbSql": "0.1.0", this value can be changes with the
          command line option, e.g. `--options apdb_sql_version=1.1.1`; this
          must correspond to what is currently in `alembic_version` table.
    """
    with Context(revision) as ctx:
        # DiaObjectLast table is optional.
        inspect = sqlalchemy.inspect(ctx.bind)
        has_dia_object_last = inspect.has_table("DiaObjectLast", schema=ctx.schema)
        _LOG.info("has_dia_object_last = %s", has_dia_object_last)

        _rename_decl_columns(ctx, has_dia_object_last)
        _rename_psf_columns(ctx)
        _rename_band_underscore(ctx)
        _rename_dipole(ctx)
        _rename_band_underscore2(ctx)
        _rename_filter(ctx)
        _rename_band_underscore_ssobject(ctx)
        _add_time_processed(ctx)
        _rename_radectai(ctx, has_dia_object_last)
        _rename_totflux(ctx)
        _rename_diffflux(ctx)
        _rename_midpointtai(ctx)
        _drop_prv_procOrder(ctx)
        _rename_mjdtai(ctx, has_dia_object_last)
        _rename_spuriousness(ctx)
        _make_nullable(ctx)

        # Last step is to create metadata table.
        _create_metadata_table(ctx)


def downgrade() -> None:
    """Not implemented, there is no reason to undo this migration."""
    raise NotImplementedError()


def _create_metadata_table(ctx: Context) -> None:
    """Make and fill metadata table."""
    apdb_sql_version = ctx.get_mig_option("apdb_sql_version")
    if apdb_sql_version is None:
        apdb_sql_version = "0.1.0"
    else:
        _LOG.info("Will use version %s for ApdbSql.", apdb_sql_version)

    # Create metadata table, should fail if for some reason it already exists.
    _LOG.info("Creating metadata table.")
    op.create_table(
        "metadata",
        Column("name", sqlalchemy.Text, primary_key=True),
        Column("value", sqlalchemy.Text),
        schema=ctx.schema,
    )

    metadata = ctx.apdb_meta
    metadata.update_tree_version("schema", "0.1.1", insert=True)
    metadata.update_tree_version("ApdbSql", apdb_sql_version, insert=True)


def _rename_columns(ctx: Context, table_name: str, column_renames: dict[str, str]) -> None:
    _LOG.info("Renaming %d columns in table %s", len(column_renames), table_name)
    with ctx.batch_alter_table(table_name) as batch_op:
        for old_column, new_column in column_renames.items():
            batch_op.alter_column(old_column, new_column_name=new_column)


def _rename_decl_columns(ctx: Context, has_dia_object_last: bool) -> None:
    """Rename 'decl' columns to 'dec'."""
    table = ctx.get_table("DiaObject", reload=True)
    if "decl" not in table.columns:
        _LOG.info("decl columns renaming is not needed")
        return

    renames = {
        "DiaObject": {
            "decl": "dec",
            "declErr": "decErr",
            "ra_decl_Cov": "ra_dec_Cov",
            "pmDecl": "pmDec",
            "pmDeclErr": "pmDecErr",
            "pmRa_pmDecl_Cov": "pmRa_pmDec_Cov",
            "pmDecl_parallax_Cov": "pmDec_parallax_Cov",
        },
        "DiaSource": {
            "decl": "dec",
            "declErr": "decErr",
            "ra_decl_Cov": "ra_dec_Cov",
            "psDecl": "psDec",
            "psDeclErr": "psDecErr",
            "psFlux_psDecl_Cov": "psFlux_psDec_Cov",
            "psRa_psDecl_Cov": "psRa_psDec_Cov",
            "trailDecl": "trailDec",
            "trailDeclErr": "trailDecErr",
            "trailFlux_trailDecl_Cov": "trailFlux_trailDec_Cov",
            "trailRa_trailDecl_Cov": "trailRa_trailDec_Cov",
            "trailDecl_trailLength_Cov": "trailDec_trailLength_Cov",
            "trailDecl_trailAngle_Cov": "trailDec_trailAngle_Cov",
            "dipDecl": "dipDec",
            "dipDeclErr": "dipDecErr",
            "dipMeanFlux_dipDecl_Cov": "dipMeanFlux_dipDec_Cov",
            "dipFluxDiff_dipDecl_Cov": "dipFluxDiff_dipDec_Cov",
            "dipRa_dipDecl_Cov": "dipRa_dipDec_Cov",
            "dipDecl_dipLength_Cov": "dipDec_dipLength_Cov",
            "dipDecl_dipAngle_Cov": "dipDec_dipAngle_Cov",
        },
        "DiaObjectLast": {
            "decl": "dec",
            "declErr": "decErr",
            "ra_decl_Cov": "ra_dec_Cov",
            "pmDecl": "pmDec",
            "pmDeclErr": "pmDecErr",
            "pmRa_pmDecl_Cov": "pmRa_pmDec_Cov",
            "pmDecl_parallax_Cov": "pmDec_parallax_Cov",
        },
    }

    for table_name, column_renames in renames.items():
        if table_name == "DiaObjectLast" and not has_dia_object_last:
            continue
        _rename_columns(ctx, table_name, column_renames)


def _rename_psf_columns(ctx: Context) -> None:
    """Rename 'PS' columns to 'Psf'."""
    table = ctx.get_table("DiaSource", reload=True)
    if "psNdata" not in table.columns:
        _LOG.info("PSF columns renaming is not needed")
        return

    renames = {
        "DiaObject": {
            "uPSFluxMean": "uPsfFluxMean",
            "uPSFluxMeanErr": "uPsfFluxMeanErr",
            "uPSFluxSigma": "uPsfFluxSigma",
            "uPSFluxChi2": "uPsfFluxChi2",
            "uPSFluxNdata": "uPsfFluxNdata",
            "gPSFluxMean": "gPsfFluxMean",
            "gPSFluxMeanErr": "gPsfFluxMeanErr",
            "gPSFluxSigma": "gPsfFluxSigma",
            "gPSFluxChi2": "gPsfFluxChi2",
            "gPSFluxNdata": "gPsfFluxNdata",
            "rPSFluxMean": "rPsfFluxMean",
            "rPSFluxMeanErr": "rPsfFluxMeanErr",
            "rPSFluxSigma": "rPsfFluxSigma",
            "rPSFluxChi2": "rPsfFluxChi2",
            "rPSFluxNdata": "rPsfFluxNdata",
            "iPSFluxMean": "iPsfFluxMean",
            "iPSFluxMeanErr": "iPsfFluxMeanErr",
            "iPSFluxSigma": "iPsfFluxSigma",
            "iPSFluxChi2": "iPsfFluxChi2",
            "iPSFluxNdata": "iPsfFluxNdata",
            "zPSFluxMean": "zPsfFluxMean",
            "zPSFluxMeanErr": "zPsfFluxMeanErr",
            "zPSFluxSigma": "zPsfFluxSigma",
            "zPSFluxChi2": "zPsfFluxChi2",
            "zPSFluxNdata": "zPsfFluxNdata",
            "yPSFluxMean": "yPsfFluxMean",
            "yPSFluxMeanErr": "yPsfFluxMeanErr",
            "yPSFluxSigma": "yPsfFluxSigma",
            "yPSFluxChi2": "yPsfFluxChi2",
            "yPSFluxNdata": "yPsfFluxNdata",
            "uPSFluxMAD": "uPsfFluxMAD",
            "uPSFluxSkew": "uPsfFluxSkew",
            "uPSFluxPercentile05": "uPsfFluxPercentile05",
            "uPSFluxPercentile25": "uPsfFluxPercentile25",
            "uPSFluxPercentile50": "uPsfFluxPercentile50",
            "uPSFluxPercentile75": "uPsfFluxPercentile75",
            "uPSFluxPercentile95": "uPsfFluxPercentile95",
            "uPSFluxMin": "uPsfFluxMin",
            "uPSFluxMax": "uPsfFluxMax",
            "uPSFluxStetsonJ": "uPsfFluxStetsonJ",
            "uPSFluxLinearSlope": "uPsfFluxLinearSlope",
            "uPSFluxLinearIntercept": "uPsfFluxLinearIntercept",
            "uPSFluxMaxSlope": "uPsfFluxMaxSlope",
            "uPSFluxErrMean": "uPsfFluxErrMean",
            "gPSFluxMAD": "gPsfFluxMAD",
            "gPSFluxSkew": "gPsfFluxSkew",
            "gPSFluxPercentile05": "gPsfFluxPercentile05",
            "gPSFluxPercentile25": "gPsfFluxPercentile25",
            "gPSFluxPercentile50": "gPsfFluxPercentile50",
            "gPSFluxPercentile75": "gPsfFluxPercentile75",
            "gPSFluxPercentile95": "gPsfFluxPercentile95",
            "gPSFluxMin": "gPsfFluxMin",
            "gPSFluxMax": "gPsfFluxMax",
            "gPSFluxStetsonJ": "gPsfFluxStetsonJ",
            "gPSFluxLinearSlope": "gPsfFluxLinearSlope",
            "gPSFluxLinearIntercept": "gPsfFluxLinearIntercept",
            "gPSFluxMaxSlope": "gPsfFluxMaxSlope",
            "gPSFluxErrMean": "gPsfFluxErrMean",
            "rPSFluxMAD": "rPsfFluxMAD",
            "rPSFluxSkew": "rPsfFluxSkew",
            "rPSFluxPercentile05": "rPsfFluxPercentile05",
            "rPSFluxPercentile25": "rPsfFluxPercentile25",
            "rPSFluxPercentile50": "rPsfFluxPercentile50",
            "rPSFluxPercentile75": "rPsfFluxPercentile75",
            "rPSFluxPercentile95": "rPsfFluxPercentile95",
            "rPSFluxMin": "rPsfFluxMin",
            "rPSFluxMax": "rPsfFluxMax",
            "rPSFluxStetsonJ": "rPsfFluxStetsonJ",
            "rPSFluxLinearSlope": "rPsfFluxLinearSlope",
            "rPSFluxLinearIntercept": "rPsfFluxLinearIntercept",
            "rPSFluxMaxSlope": "rPsfFluxMaxSlope",
            "rPSFluxErrMean": "rPsfFluxErrMean",
            "iPSFluxMAD": "iPsfFluxMAD",
            "iPSFluxSkew": "iPsfFluxSkew",
            "iPSFluxPercentile05": "iPsfFluxPercentile05",
            "iPSFluxPercentile25": "iPsfFluxPercentile25",
            "iPSFluxPercentile50": "iPsfFluxPercentile50",
            "iPSFluxPercentile75": "iPsfFluxPercentile75",
            "iPSFluxPercentile95": "iPsfFluxPercentile95",
            "iPSFluxMin": "iPsfFluxMin",
            "iPSFluxMax": "iPsfFluxMax",
            "iPSFluxStetsonJ": "iPsfFluxStetsonJ",
            "iPSFluxLinearSlope": "iPsfFluxLinearSlope",
            "iPSFluxLinearIntercept": "iPsfFluxLinearIntercept",
            "iPSFluxMaxSlope": "iPsfFluxMaxSlope",
            "iPSFluxErrMean": "iPsfFluxErrMean",
            "zPSFluxMAD": "zPsfFluxMAD",
            "zPSFluxSkew": "zPsfFluxSkew",
            "zPSFluxPercentile05": "zPsfFluxPercentile05",
            "zPSFluxPercentile25": "zPsfFluxPercentile25",
            "zPSFluxPercentile50": "zPsfFluxPercentile50",
            "zPSFluxPercentile75": "zPsfFluxPercentile75",
            "zPSFluxPercentile95": "zPsfFluxPercentile95",
            "zPSFluxMin": "zPsfFluxMin",
            "zPSFluxMax": "zPsfFluxMax",
            "zPSFluxStetsonJ": "zPsfFluxStetsonJ",
            "zPSFluxLinearSlope": "zPsfFluxLinearSlope",
            "zPSFluxLinearIntercept": "zPsfFluxLinearIntercept",
            "zPSFluxMaxSlope": "zPsfFluxMaxSlope",
            "zPSFluxErrMean": "zPsfFluxErrMean",
            "yPSFluxMAD": "yPsfFluxMAD",
            "yPSFluxSkew": "yPsfFluxSkew",
            "yPSFluxPercentile05": "yPsfFluxPercentile05",
            "yPSFluxPercentile25": "yPsfFluxPercentile25",
            "yPSFluxPercentile50": "yPsfFluxPercentile50",
            "yPSFluxPercentile75": "yPsfFluxPercentile75",
            "yPSFluxPercentile95": "yPsfFluxPercentile95",
            "yPSFluxMin": "yPsfFluxMin",
            "yPSFluxMax": "yPsfFluxMax",
            "yPSFluxStetsonJ": "yPsfFluxStetsonJ",
            "yPSFluxLinearSlope": "yPsfFluxLinearSlope",
            "yPSFluxLinearIntercept": "yPsfFluxLinearIntercept",
            "yPSFluxMaxSlope": "yPsfFluxMaxSlope",
            "yPSFluxErrMean": "yPsfFluxErrMean",
        },
        "DiaSource": {
            "psFlux": "psfFlux",
            "psFluxErr": "psfFluxErr",
            "psRa": "psfRa",
            "psRaErr": "psfRaErr",
            "psDec": "psfDec",
            "psDecErr": "psfDecErr",
            "psFlux_psRa_Cov": "psfFlux_psfRa_Cov",
            "psFlux_psDec_Cov": "psfFlux_psfDec_Cov",
            "psRa_psDec_Cov": "psfRa_psfDec_Cov",
            "psLnL": "psfLnL",
            "psChi2": "psfChi2",
            "psNdata": "psfNdata",
        },
        "DiaForcedSource": {
            "psFlux": "psfFlux",
            "psFluxErr": "psfFluxErr",
        },
    }

    for table_name, column_renames in renames.items():
        _rename_columns(ctx, table_name, column_renames)


def _rename_band_underscore(ctx: Context) -> None:
    """Separate band name with underscore."""
    table = ctx.get_table("DiaObject", reload=True)
    if "uPsfFluxMean" not in table.columns:
        _LOG.info("Band columns renaming is not needed")
        return

    column_renames = {
        "uPsfFluxMean": "u_psfFluxMean",
        "uPsfFluxMeanErr": "u_psfFluxMeanErr",
        "uPsfFluxSigma": "u_psfFluxSigma",
        "uPsfFluxChi2": "u_psfFluxChi2",
        "uPsfFluxNdata": "u_psfFluxNdata",
        "gPsfFluxMean": "g_psfFluxMean",
        "gPsfFluxMeanErr": "g_psfFluxMeanErr",
        "gPsfFluxSigma": "g_psfFluxSigma",
        "gPsfFluxChi2": "g_psfFluxChi2",
        "gPsfFluxNdata": "g_psfFluxNdata",
        "rPsfFluxMean": "r_psfFluxMean",
        "rPsfFluxMeanErr": "r_psfFluxMeanErr",
        "rPsfFluxSigma": "r_psfFluxSigma",
        "rPsfFluxChi2": "r_psfFluxChi2",
        "rPsfFluxNdata": "r_psfFluxNdata",
        "iPsfFluxMean": "i_psfFluxMean",
        "iPsfFluxMeanErr": "i_psfFluxMeanErr",
        "iPsfFluxSigma": "i_psfFluxSigma",
        "iPsfFluxChi2": "i_psfFluxChi2",
        "iPsfFluxNdata": "i_psfFluxNdata",
        "zPsfFluxMean": "z_psfFluxMean",
        "zPsfFluxMeanErr": "z_psfFluxMeanErr",
        "zPsfFluxSigma": "z_psfFluxSigma",
        "zPsfFluxChi2": "z_psfFluxChi2",
        "zPsfFluxNdata": "z_psfFluxNdata",
        "yPsfFluxMean": "y_psfFluxMean",
        "yPsfFluxMeanErr": "y_psfFluxMeanErr",
        "yPsfFluxSigma": "y_psfFluxSigma",
        "yPsfFluxChi2": "y_psfFluxChi2",
        "yPsfFluxNdata": "y_psfFluxNdata",
        "uPsfFluxMAD": "u_psfFluxMAD",
        "uPsfFluxSkew": "u_psfFluxSkew",
        "uPsfFluxPercentile05": "u_psfFluxPercentile05",
        "uPsfFluxPercentile25": "u_psfFluxPercentile25",
        "uPsfFluxPercentile50": "u_psfFluxPercentile50",
        "uPsfFluxPercentile75": "u_psfFluxPercentile75",
        "uPsfFluxPercentile95": "u_psfFluxPercentile95",
        "uPsfFluxMin": "u_psfFluxMin",
        "uPsfFluxMax": "u_psfFluxMax",
        "uPsfFluxStetsonJ": "u_psfFluxStetsonJ",
        "uPsfFluxLinearSlope": "u_psfFluxLinearSlope",
        "uPsfFluxLinearIntercept": "u_psfFluxLinearIntercept",
        "uPsfFluxMaxSlope": "u_psfFluxMaxSlope",
        "uPsfFluxErrMean": "u_psfFluxErrMean",
        "gPsfFluxMAD": "g_psfFluxMAD",
        "gPsfFluxSkew": "g_psfFluxSkew",
        "gPsfFluxPercentile05": "g_psfFluxPercentile05",
        "gPsfFluxPercentile25": "g_psfFluxPercentile25",
        "gPsfFluxPercentile50": "g_psfFluxPercentile50",
        "gPsfFluxPercentile75": "g_psfFluxPercentile75",
        "gPsfFluxPercentile95": "g_psfFluxPercentile95",
        "gPsfFluxMin": "g_psfFluxMin",
        "gPsfFluxMax": "g_psfFluxMax",
        "gPsfFluxStetsonJ": "g_psfFluxStetsonJ",
        "gPsfFluxLinearSlope": "g_psfFluxLinearSlope",
        "gPsfFluxLinearIntercept": "g_psfFluxLinearIntercept",
        "gPsfFluxMaxSlope": "g_psfFluxMaxSlope",
        "gPsfFluxErrMean": "g_psfFluxErrMean",
        "rPsfFluxMAD": "r_psfFluxMAD",
        "rPsfFluxSkew": "r_psfFluxSkew",
        "rPsfFluxPercentile05": "r_psfFluxPercentile05",
        "rPsfFluxPercentile25": "r_psfFluxPercentile25",
        "rPsfFluxPercentile50": "r_psfFluxPercentile50",
        "rPsfFluxPercentile75": "r_psfFluxPercentile75",
        "rPsfFluxPercentile95": "r_psfFluxPercentile95",
        "rPsfFluxMin": "r_psfFluxMin",
        "rPsfFluxMax": "r_psfFluxMax",
        "rPsfFluxStetsonJ": "r_psfFluxStetsonJ",
        "rPsfFluxLinearSlope": "r_psfFluxLinearSlope",
        "rPsfFluxLinearIntercept": "r_psfFluxLinearIntercept",
        "rPsfFluxMaxSlope": "r_psfFluxMaxSlope",
        "rPsfFluxErrMean": "r_psfFluxErrMean",
        "iPsfFluxMAD": "i_psfFluxMAD",
        "iPsfFluxSkew": "i_psfFluxSkew",
        "iPsfFluxPercentile05": "i_psfFluxPercentile05",
        "iPsfFluxPercentile25": "i_psfFluxPercentile25",
        "iPsfFluxPercentile50": "i_psfFluxPercentile50",
        "iPsfFluxPercentile75": "i_psfFluxPercentile75",
        "iPsfFluxPercentile95": "i_psfFluxPercentile95",
        "iPsfFluxMin": "i_psfFluxMin",
        "iPsfFluxMax": "i_psfFluxMax",
        "iPsfFluxStetsonJ": "i_psfFluxStetsonJ",
        "iPsfFluxLinearSlope": "i_psfFluxLinearSlope",
        "iPsfFluxLinearIntercept": "i_psfFluxLinearIntercept",
        "iPsfFluxMaxSlope": "i_psfFluxMaxSlope",
        "iPsfFluxErrMean": "i_psfFluxErrMean",
        "zPsfFluxMAD": "z_psfFluxMAD",
        "zPsfFluxSkew": "z_psfFluxSkew",
        "zPsfFluxPercentile05": "z_psfFluxPercentile05",
        "zPsfFluxPercentile25": "z_psfFluxPercentile25",
        "zPsfFluxPercentile50": "z_psfFluxPercentile50",
        "zPsfFluxPercentile75": "z_psfFluxPercentile75",
        "zPsfFluxPercentile95": "z_psfFluxPercentile95",
        "zPsfFluxMin": "z_psfFluxMin",
        "zPsfFluxMax": "z_psfFluxMax",
        "zPsfFluxStetsonJ": "z_psfFluxStetsonJ",
        "zPsfFluxLinearSlope": "z_psfFluxLinearSlope",
        "zPsfFluxLinearIntercept": "z_psfFluxLinearIntercept",
        "zPsfFluxMaxSlope": "z_psfFluxMaxSlope",
        "zPsfFluxErrMean": "z_psfFluxErrMean",
        "yPsfFluxMAD": "y_psfFluxMAD",
        "yPsfFluxSkew": "y_psfFluxSkew",
        "yPsfFluxPercentile05": "y_psfFluxPercentile05",
        "yPsfFluxPercentile25": "y_psfFluxPercentile25",
        "yPsfFluxPercentile50": "y_psfFluxPercentile50",
        "yPsfFluxPercentile75": "y_psfFluxPercentile75",
        "yPsfFluxPercentile95": "y_psfFluxPercentile95",
        "yPsfFluxMin": "y_psfFluxMin",
        "yPsfFluxMax": "y_psfFluxMax",
        "yPsfFluxStetsonJ": "y_psfFluxStetsonJ",
        "yPsfFluxLinearSlope": "y_psfFluxLinearSlope",
        "yPsfFluxLinearIntercept": "y_psfFluxLinearIntercept",
        "yPsfFluxMaxSlope": "y_psfFluxMaxSlope",
        "yPsfFluxErrMean": "y_psfFluxErrMean",
    }

    _rename_columns(ctx, "DiaObject", column_renames)


def _rename_dipole(ctx: Context) -> None:
    """Rename 'dip' columns to 'dipole'."""
    table = ctx.get_table("DiaSource", reload=True)
    if "dipMeanFlux" not in table.columns:
        _LOG.info("Dipole columns renaming is not needed")
        return

    column_renames = {
        "dipMeanFlux": "dipoleMeanFlux",
        "dipMeanFluxErr": "dipoleMeanFluxErr",
        "dipFluxDiff": "dipoleFluxDiff",
        "dipFluxDiffErr": "dipoleFluxDiffErr",
        "dipRa": "dipoleRa",
        "dipRaErr": "dipoleRaErr",
        "dipDec": "dipoleDec",
        "dipDecErr": "dipoleDecErr",
        "dipLength": "dipoleLength",
        "dipLengthErr": "dipoleLengthErr",
        "dipAngle": "dipoleAngle",
        "dipAngleErr": "dipoleAngleErr",
        "dipMeanFlux_dipFluxDiff_Cov": "dipoleMeanFlux_dipoleFluxDiff_Cov",
        "dipMeanFlux_dipRa_Cov": "dipoleMeanFlux_dipoleRa_Cov",
        "dipMeanFlux_dipDec_Cov": "dipoleMeanFlux_dipoleDec_Cov",
        "dipMeanFlux_dipLength_Cov": "dipoleMeanFlux_dipoleLength_Cov",
        "dipMeanFlux_dipAngle_Cov": "dipoleMeanFlux_dipoleAngle_Cov",
        "dipFluxDiff_dipRa_Cov": "dipoleFluxDiff_dipoleRa_Cov",
        "dipFluxDiff_dipDec_Cov": "dipoleFluxDiff_dipoleDec_Cov",
        "dipFluxDiff_dipLength_Cov": "dipoleFluxDiff_dipoleLength_Cov",
        "dipFluxDiff_dipAngle_Cov": "dipoleFluxDiff_dipoleAngle_Cov",
        "dipRa_dipDec_Cov": "dipoleRa_dipoleDec_Cov",
        "dipRa_dipLength_Cov": "dipoleRa_dipoleLength_Cov",
        "dipRa_dipAngle_Cov": "dipoleRa_dipoleAngle_Cov",
        "dipDec_dipLength_Cov": "dipoleDec_dipoleLength_Cov",
        "dipDec_dipAngle_Cov": "dipoleDec_dipoleAngle_Cov",
        "dipLength_dipAngle_Cov": "dipoleLength_dipoleAngle_Cov",
        "dipLnL": "dipoleLnL",
        "dipChi2": "dipoleChi2",
        "dipNdata": "dipoleNdata",
    }

    _rename_columns(ctx, "DiaSource", column_renames)


def _rename_band_underscore2(ctx: Context) -> None:
    """Separate band name with underscore."""
    table = ctx.get_table("DiaObject", reload=True)
    if "uFPFluxMean" not in table.columns:
        _LOG.info("Band columns renaming is not needed")
        return

    column_renames = {
        "uFPFluxMean": "u_fpFluxMean",
        "uFPFluxMeanErr": "u_fpFluxMeanErr",
        "uFPFluxSigma": "u_fpFluxSigma",
        "gFPFluxMean": "g_fpFluxMean",
        "gFPFluxMeanErr": "g_fpFluxMeanErr",
        "gFPFluxSigma": "g_fpFluxSigma",
        "rFPFluxMean": "r_fpFluxMean",
        "rFPFluxMeanErr": "r_fpFluxMeanErr",
        "rFPFluxSigma": "r_fpFluxSigma",
        "iFPFluxMean": "i_fpFluxMean",
        "iFPFluxMeanErr": "i_fpFluxMeanErr",
        "iFPFluxSigma": "i_fpFluxSigma",
        "zFPFluxMean": "z_fpFluxMean",
        "zFPFluxMeanErr": "z_fpFluxMeanErr",
        "zFPFluxSigma": "z_fpFluxSigma",
        "yFPFluxMean": "y_fpFluxMean",
        "yFPFluxMeanErr": "y_fpFluxMeanErr",
        "yFPFluxSigma": "y_fpFluxSigma",
        "uLcPeriodic": "u_lcPeriodic",
        "gLcPeriodic": "g_lcPeriodic",
        "rLcPeriodic": "r_lcPeriodic",
        "iLcPeriodic": "i_lcPeriodic",
        "zLcPeriodic": "z_lcPeriodic",
        "yLcPeriodic": "y_lcPeriodic",
        "uLcNonPeriodic": "u_lcNonPeriodic",
        "gLcNonPeriodic": "g_lcNonPeriodic",
        "rLcNonPeriodic": "r_lcNonPeriodic",
        "iLcNonPeriodic": "i_lcNonPeriodic",
        "zLcNonPeriodic": "z_lcNonPeriodic",
        "yLcNonPeriodic": "y_lcNonPeriodic",
        "uTOTFluxMean": "u_totFluxMean",
        "uTOTFluxMeanErr": "u_totFluxMeanErr",
        "uTOTFluxSigma": "u_totFluxSigma",
        "gTOTFluxMean": "g_totFluxMean",
        "gTOTFluxMeanErr": "g_totFluxMeanErr",
        "gTOTFluxSigma": "g_totFluxSigma",
        "rTOTFluxMean": "r_totFluxMean",
        "rTOTFluxMeanErr": "r_totFluxMeanErr",
        "rTOTFluxSigma": "r_totFluxSigma",
        "iTOTFluxMean": "i_totFluxMean",
        "iTOTFluxMeanErr": "i_totFluxMeanErr",
        "iTOTFluxSigma": "i_totFluxSigma",
        "zTOTFluxMean": "z_totFluxMean",
        "zTOTFluxMeanErr": "z_totFluxMeanErr",
        "zTOTFluxSigma": "z_totFluxSigma",
        "yTOTFluxMean": "y_totFluxMean",
        "yTOTFluxMeanErr": "y_totFluxMeanErr",
        "yTOTFluxSigma": "y_totFluxSigma",
    }

    _rename_columns(ctx, "DiaObject", column_renames)


def _rename_filter(ctx: Context) -> None:
    """Rename 'filterName' columns to 'band'."""
    table = ctx.get_table("DiaSource", reload=True)
    if "filterName" not in table.columns:
        _LOG.info("filterName columns renaming is not needed")
        return

    column_renames = {
        "filterName": "band",
    }

    _rename_columns(ctx, "DiaSource", column_renames)
    _rename_columns(ctx, "DiaForcedSource", column_renames)


def _rename_band_underscore_ssobject(ctx: Context) -> None:
    """Separate band name with underscore."""
    table = ctx.get_table("SSObject", reload=True)
    if "uH" not in table.columns:
        _LOG.info("Band columns renaming is not needed")
        return

    column_renames = {
        "uH": "u_H",
        "uG12": "u_G12",
        "uHErr": "u_HErr",
        "uG12Err": "u_G12Err",
        "uH_uG12_Cov": "u_H_u_G12_Cov",
        "uChi2": "u_Chi2",
        "uNdata": "u_Ndata",
        "gH": "g_H",
        "gG12": "g_G12",
        "gHErr": "g_HErr",
        "gG12Err": "g_G12Err",
        "gH_gG12_Cov": "g_H_g_G12_Cov",
        "gChi2": "g_Chi2",
        "gNdata": "g_Ndata",
        "rH": "r_H",
        "rG12": "r_G12",
        "rHErr": "r_HErr",
        "rG12Err": "r_G12Err",
        "rH_rG12_Cov": "r_H_r_G12_Cov",
        "rChi2": "r_Chi2",
        "rNdata": "r_Ndata",
        "iH": "i_H",
        "iG12": "i_G12",
        "iHErr": "i_HErr",
        "iG12Err": "i_G12Err",
        "iH_iG12_Cov": "i_H_i_G12_Cov",
        "iChi2": "i_Chi2",
        "iNdata": "i_Ndata",
        "zH": "z_H",
        "zG12": "z_G12",
        "zHErr": "z_HErr",
        "zG12Err": "z_G12Err",
        "zH_zG12_Cov": "z_H_z_G12_Cov",
        "zChi2": "z_Chi2",
        "zNdata": "z_Ndata",
        "yH": "y_H",
        "yG12": "y_G12",
        "yHErr": "y_HErr",
        "yG12Err": "y_G12Err",
        "yH_yG12_Cov": "y_H_y_G12_Cov",
        "yChi2": "y_Chi2",
        "yNdata": "y_Ndata",
    }

    _rename_columns(ctx, "SSObject", column_renames)


def _add_time_processed(ctx: Context) -> None:
    """Add time_processed/time_withdrawn columns."""
    table = ctx.get_table("DiaSource", reload=True)
    if "time_processed" in table.columns:
        _LOG.info("Adding time_processed columns is not needed")
        return

    for table_name in ("DiaSource", "DiaForcedSource"):
        _LOG.info("Adding time_processed/time_withdrawn columns to table %s", table_name)
        with ctx.batch_alter_table(table_name) as batch_op:
            # Time processed as defined as NOT NULL, need to fill it with some
            # values, but the is no way to guess actual time, instead I'll make
            # it NULL.
            batch_op.add_column(Column("time_processed", sqlalchemy.types.TIMESTAMP, nullable=True))
            batch_op.add_column(Column("time_withdrawn", sqlalchemy.types.TIMESTAMP, nullable=True))


def _rename_radectai(ctx: Context, has_dia_object_last: bool) -> None:
    """Rename 'radecTai' columns to 'radecEpoch'."""
    table = ctx.get_table("DiaObject", reload=True)
    if "radecTai" not in table.columns:
        _LOG.info("radecTai columns renaming is not needed")
        return

    column_renames = {
        "radecTai": "radecEpoch",
    }

    _rename_columns(ctx, "DiaObject", column_renames)
    if has_dia_object_last:
        _rename_columns(ctx, "DiaObjectLast", column_renames)


def _rename_totflux(ctx: Context) -> None:
    """Rename 'totFlux' columns to 'scienceFlux'."""
    table = ctx.get_table("DiaSource", reload=True)
    if "totFlux" not in table.columns:
        _LOG.info("totFlux columns renaming is not needed")
        return

    renames = {
        "DiaObject": {
            "u_totFluxMean": "u_scienceFluxMean",
            "u_totFluxMeanErr": "u_scienceFluxMeanErr",
            "u_totFluxSigma": "u_scienceFluxSigma",
            "g_totFluxMean": "g_scienceFluxMean",
            "g_totFluxMeanErr": "g_scienceFluxMeanErr",
            "g_totFluxSigma": "g_scienceFluxSigma",
            "r_totFluxMean": "r_scienceFluxMean",
            "r_totFluxMeanErr": "r_scienceFluxMeanErr",
            "r_totFluxSigma": "r_scienceFluxSigma",
            "i_totFluxMean": "i_scienceFluxMean",
            "i_totFluxMeanErr": "i_scienceFluxMeanErr",
            "i_totFluxSigma": "i_scienceFluxSigma",
            "z_totFluxMean": "z_scienceFluxMean",
            "z_totFluxMeanErr": "z_scienceFluxMeanErr",
            "z_totFluxSigma": "z_scienceFluxSigma",
            "y_totFluxMean": "y_scienceFluxMean",
            "y_totFluxMeanErr": "y_scienceFluxMeanErr",
            "y_totFluxSigma": "y_scienceFluxSigma",
        },
        "DiaSource": {
            "totFlux": "scienceFlux",
            "totFluxErr": "scienceFluxErr",
        },
        "DiaForcedSource": {
            "totFlux": "scienceFlux",
            "totFluxErr": "scienceFluxErr",
        },
    }

    for table_name, column_renames in renames.items():
        _rename_columns(ctx, table_name, column_renames)


def _rename_diffflux(ctx: Context) -> None:
    """Rename 'diffFlux' columns to 'snapDiffFlux'."""
    table = ctx.get_table("DiaSource", reload=True)
    if "diffFlux" not in table.columns:
        _LOG.info("diffFlux columns renaming is not needed")
        return

    column_renames = {
        "diffFlux": "snapDiffFlux",
        "diffFluxErr": "snapDiffFluxErr",
    }

    _rename_columns(ctx, "DiaSource", column_renames)


def _rename_midpointtai(ctx: Context) -> None:
    """Rename 'midPointTai' columns to 'midPointMjd'."""
    table = ctx.get_table("DiaSource", reload=True)
    if "midPointTai" not in table.columns:
        _LOG.info("midPointTai columns renaming is not needed")
        return

    column_renames = {
        "midPointTai": "midPointMjd",
    }

    _rename_columns(ctx, "DiaSource", column_renames)
    _rename_columns(ctx, "DiaForcedSource", column_renames)


def _drop_prv_procOrder(ctx: Context) -> None:
    """Drop DiaSource.prv_procOrder column."""
    table = ctx.get_table("DiaSource", reload=True)
    if "prv_procOrder" not in table.columns:
        _LOG.info("prv_procOrder column dropping is not needed")
        return

    _LOG.info("Dropping prv_procOrder column from table DiaSource")
    with ctx.batch_alter_table("DiaSource") as batch_op:
        batch_op.drop_column("prv_procOrder")


def _rename_mjdtai(ctx: Context, has_dia_object_last: bool) -> None:
    """Rename MJD columns to have 'MjdTai' suffix."""
    table = ctx.get_table("DiaSource", reload=True)
    if "midPointMjd" not in table.columns:
        _LOG.info("totFlux columns renaming is not needed")
        return

    renames = {
        "DiaObject": {
            "radecEpoch": "radecMjdTai",
        },
        "DiaSource": {
            "midPointMjd": "midpointMjdTai",
        },
        "DiaForcedSource": {
            "midPointMjd": "midpointMjdTai",
        },
        "DiaObjectLast": {
            "radecEpoch": "radecMjdTai",
        },
    }

    for table_name, column_renames in renames.items():
        if table_name == "DiaObjectLast" and not has_dia_object_last:
            continue
        _rename_columns(ctx, table_name, column_renames)


def _rename_spuriousness(ctx: Context) -> None:
    """Rename 'spuriousness' column to 'reliability'."""
    table = ctx.get_table("DiaSource", reload=True)
    if "spuriousness" not in table.columns:
        _LOG.info("spuriousness columns renaming is not needed")
        return

    column_renames = {
        "spuriousness": "reliability",
    }

    _rename_columns(ctx, "DiaSource", column_renames)


def _make_nullable(ctx: Context) -> None:
    """Make a bunch of columns nullable."""
    table_columns = {
        "DiaObject": (
            "pmParallaxNdata",
            "u_psfFluxNdata",
            "g_psfFluxNdata",
            "r_psfFluxNdata",
            "i_psfFluxNdata",
            "z_psfFluxNdata",
            "y_psfFluxNdata",
            "nearbyObj1",
            "nearbyObj2",
            "nearbyObj3",
        ),
        "DiaSource": ("dipoleNdata", "trailNdata"),
        "DiaForcedSource": ("flags",),
    }

    for table_name, columns in table_columns.items():
        _LOG.info("Change %d columns in table %s to nullable.", len(columns), table_name)
        with ctx.batch_alter_table(table_name) as batch_op:
            for column in columns:
                batch_op.alter_column(column, nullable=True)
