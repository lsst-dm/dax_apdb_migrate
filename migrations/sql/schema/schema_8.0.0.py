"""Migration script for schema 8.0.0.

Revision ID: schema_8.0.0
Revises: schema_7.0.1
Create Date: 2025-08-13 20:14:44.281174
"""

import logging

import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context

# revision identifiers, used by Alembic.
revision = "schema_8.0.0"
down_revision = "schema_7.0.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

# Names of dropped columns by table.
_dropped_columns = {
    "DiaObject": {
        "radecMjdTai": sqlalchemy.types.Double,
        "pmRa": sqlalchemy.types.REAL,
        "pmRaErr": sqlalchemy.types.REAL,
        "pmDec": sqlalchemy.types.REAL,
        "pmDecErr": sqlalchemy.types.REAL,
        "parallax": sqlalchemy.types.REAL,
        "parallaxErr": sqlalchemy.types.REAL,
        "pmRa_pmDec_Cov": sqlalchemy.types.REAL,
        "pmRa_parallax_Cov": sqlalchemy.types.REAL,
        "pmDec_parallax_Cov": sqlalchemy.types.REAL,
        "pmParallaxLnL": sqlalchemy.types.REAL,
        "pmParallaxChi2": sqlalchemy.types.REAL,
        "pmParallaxNdata": sqlalchemy.types.Integer,
        "u_psfFluxChi2": sqlalchemy.types.REAL,
        "u_fpFluxSigma": sqlalchemy.types.REAL,
        "g_psfFluxChi2": sqlalchemy.types.REAL,
        "g_fpFluxSigma": sqlalchemy.types.REAL,
        "r_psfFluxChi2": sqlalchemy.types.REAL,
        "r_fpFluxSigma": sqlalchemy.types.REAL,
        "i_psfFluxChi2": sqlalchemy.types.REAL,
        "i_fpFluxSigma": sqlalchemy.types.REAL,
        "z_psfFluxChi2": sqlalchemy.types.REAL,
        "z_fpFluxSigma": sqlalchemy.types.REAL,
        "y_psfFluxChi2": sqlalchemy.types.REAL,
        "y_fpFluxSigma": sqlalchemy.types.REAL,
        "nearbyObj1": sqlalchemy.types.BigInteger,
        "nearbyObj1Dist": sqlalchemy.types.REAL,
        "nearbyObj1LnP": sqlalchemy.types.REAL,
        "nearbyObj2": sqlalchemy.types.BigInteger,
        "nearbyObj2Dist": sqlalchemy.types.REAL,
        "nearbyObj2LnP": sqlalchemy.types.REAL,
        "nearbyObj3": sqlalchemy.types.BigInteger,
        "nearbyObj3Dist": sqlalchemy.types.REAL,
        "nearbyObj3LnP": sqlalchemy.types.REAL,
        "nearbyExtObj1": sqlalchemy.types.BigInteger,
        "nearbyExtObj1Sep": sqlalchemy.types.REAL,
        "nearbyExtObj2": sqlalchemy.types.BigInteger,
        "nearbyExtObj2Sep": sqlalchemy.types.REAL,
        "nearbyExtObj3": sqlalchemy.types.BigInteger,
        "nearbyExtObj3Sep": sqlalchemy.types.REAL,
        "nearbyLowzGal": sqlalchemy.types.VARCHAR,
        "nearbyLowzGalSep": sqlalchemy.types.REAL,
        "u_scienceFluxSigma": sqlalchemy.types.REAL,
        "g_scienceFluxSigma": sqlalchemy.types.REAL,
        "r_scienceFluxSigma": sqlalchemy.types.REAL,
        "i_scienceFluxSigma": sqlalchemy.types.REAL,
        "z_scienceFluxSigma": sqlalchemy.types.REAL,
        "y_scienceFluxSigma": sqlalchemy.types.REAL,
        "u_psfFluxMAD": sqlalchemy.types.REAL,
        "u_psfFluxSkew": sqlalchemy.types.REAL,
        "u_psfFluxPercentile05": sqlalchemy.types.REAL,
        "u_psfFluxPercentile25": sqlalchemy.types.REAL,
        "u_psfFluxPercentile50": sqlalchemy.types.REAL,
        "u_psfFluxPercentile75": sqlalchemy.types.REAL,
        "u_psfFluxPercentile95": sqlalchemy.types.REAL,
        "u_psfFluxStetsonJ": sqlalchemy.types.REAL,
        "u_psfFluxLinearSlope": sqlalchemy.types.REAL,
        "u_psfFluxLinearIntercept": sqlalchemy.types.REAL,
        "g_psfFluxMAD": sqlalchemy.types.REAL,
        "g_psfFluxSkew": sqlalchemy.types.REAL,
        "g_psfFluxPercentile05": sqlalchemy.types.REAL,
        "g_psfFluxPercentile25": sqlalchemy.types.REAL,
        "g_psfFluxPercentile50": sqlalchemy.types.REAL,
        "g_psfFluxPercentile75": sqlalchemy.types.REAL,
        "g_psfFluxPercentile95": sqlalchemy.types.REAL,
        "g_psfFluxStetsonJ": sqlalchemy.types.REAL,
        "g_psfFluxLinearSlope": sqlalchemy.types.REAL,
        "g_psfFluxLinearIntercept": sqlalchemy.types.REAL,
        "r_psfFluxMAD": sqlalchemy.types.REAL,
        "r_psfFluxSkew": sqlalchemy.types.REAL,
        "r_psfFluxPercentile05": sqlalchemy.types.REAL,
        "r_psfFluxPercentile25": sqlalchemy.types.REAL,
        "r_psfFluxPercentile50": sqlalchemy.types.REAL,
        "r_psfFluxPercentile75": sqlalchemy.types.REAL,
        "r_psfFluxPercentile95": sqlalchemy.types.REAL,
        "r_psfFluxStetsonJ": sqlalchemy.types.REAL,
        "r_psfFluxLinearSlope": sqlalchemy.types.REAL,
        "r_psfFluxLinearIntercept": sqlalchemy.types.REAL,
        "i_psfFluxMAD": sqlalchemy.types.REAL,
        "i_psfFluxSkew": sqlalchemy.types.REAL,
        "i_psfFluxPercentile05": sqlalchemy.types.REAL,
        "i_psfFluxPercentile25": sqlalchemy.types.REAL,
        "i_psfFluxPercentile50": sqlalchemy.types.REAL,
        "i_psfFluxPercentile75": sqlalchemy.types.REAL,
        "i_psfFluxPercentile95": sqlalchemy.types.REAL,
        "i_psfFluxStetsonJ": sqlalchemy.types.REAL,
        "i_psfFluxLinearSlope": sqlalchemy.types.REAL,
        "i_psfFluxLinearIntercept": sqlalchemy.types.REAL,
        "z_psfFluxMAD": sqlalchemy.types.REAL,
        "z_psfFluxSkew": sqlalchemy.types.REAL,
        "z_psfFluxPercentile05": sqlalchemy.types.REAL,
        "z_psfFluxPercentile25": sqlalchemy.types.REAL,
        "z_psfFluxPercentile50": sqlalchemy.types.REAL,
        "z_psfFluxPercentile75": sqlalchemy.types.REAL,
        "z_psfFluxPercentile95": sqlalchemy.types.REAL,
        "z_psfFluxStetsonJ": sqlalchemy.types.REAL,
        "z_psfFluxLinearSlope": sqlalchemy.types.REAL,
        "z_psfFluxLinearIntercept": sqlalchemy.types.REAL,
        "y_psfFluxMAD": sqlalchemy.types.REAL,
        "y_psfFluxSkew": sqlalchemy.types.REAL,
        "y_psfFluxPercentile05": sqlalchemy.types.REAL,
        "y_psfFluxPercentile25": sqlalchemy.types.REAL,
        "y_psfFluxPercentile50": sqlalchemy.types.REAL,
        "y_psfFluxPercentile75": sqlalchemy.types.REAL,
        "y_psfFluxPercentile95": sqlalchemy.types.REAL,
        "y_psfFluxStetsonJ": sqlalchemy.types.REAL,
        "y_psfFluxLinearSlope": sqlalchemy.types.REAL,
        "y_psfFluxLinearIntercept": sqlalchemy.types.REAL,
        "lastNonForcedSource": sqlalchemy.types.TIMESTAMP,
    },
    "DiaSource": {
        "x_y_Cov": sqlalchemy.types.REAL,
        "psfRa": sqlalchemy.types.Double,
        "psfRaErr": sqlalchemy.types.REAL,
        "psfDec": sqlalchemy.types.Double,
        "psfDecErr": sqlalchemy.types.REAL,
        "psfFlux_psfRa_Cov": sqlalchemy.types.REAL,
        "psfFlux_psfDec_Cov": sqlalchemy.types.REAL,
        "psfRa_psfDec_Cov": sqlalchemy.types.REAL,
        "trailFlux_trailRa_Cov": sqlalchemy.types.REAL,
        "trailFlux_trailDec_Cov": sqlalchemy.types.REAL,
        "trailFlux_trailLength_Cov": sqlalchemy.types.REAL,
        "trailFlux_trailAngle_Cov": sqlalchemy.types.REAL,
        "trailRa_trailDec_Cov": sqlalchemy.types.REAL,
        "trailRa_trailLength_Cov": sqlalchemy.types.REAL,
        "trailRa_trailAngle_Cov": sqlalchemy.types.REAL,
        "trailDec_trailLength_Cov": sqlalchemy.types.REAL,
        "trailDec_trailAngle_Cov": sqlalchemy.types.REAL,
        "trailLength_trailAngle_Cov": sqlalchemy.types.REAL,
        "trailLnL": sqlalchemy.types.REAL,
        "dipoleRa": sqlalchemy.types.Double,
        "dipoleRaErr": sqlalchemy.types.REAL,
        "dipoleDec": sqlalchemy.types.Double,
        "dipoleDecErr": sqlalchemy.types.REAL,
        "dipoleLengthErr": sqlalchemy.types.REAL,
        "dipoleAngleErr": sqlalchemy.types.REAL,
        "dipoleMeanFlux_dipoleFluxDiff_Cov": sqlalchemy.types.REAL,
        "dipoleMeanFlux_dipoleRa_Cov": sqlalchemy.types.REAL,
        "dipoleMeanFlux_dipoleDec_Cov": sqlalchemy.types.REAL,
        "dipoleMeanFlux_dipoleLength_Cov": sqlalchemy.types.REAL,
        "dipoleMeanFlux_dipoleAngle_Cov": sqlalchemy.types.REAL,
        "dipoleFluxDiff_dipoleRa_Cov": sqlalchemy.types.REAL,
        "dipoleFluxDiff_dipoleDec_Cov": sqlalchemy.types.REAL,
        "dipoleFluxDiff_dipoleLength_Cov": sqlalchemy.types.REAL,
        "dipoleFluxDiff_dipoleAngle_Cov": sqlalchemy.types.REAL,
        "dipoleRa_dipoleDec_Cov": sqlalchemy.types.REAL,
        "dipoleRa_dipoleLength_Cov": sqlalchemy.types.REAL,
        "dipoleRa_dipoleAngle_Cov": sqlalchemy.types.REAL,
        "dipoleDec_dipoleLength_Cov": sqlalchemy.types.REAL,
        "dipoleDec_dipoleAngle_Cov": sqlalchemy.types.REAL,
        "dipoleLength_dipoleAngle_Cov": sqlalchemy.types.REAL,
        "dipoleLnL": sqlalchemy.types.REAL,
        "snapDiffFlux": sqlalchemy.types.REAL,
        "snapDiffFluxErr": sqlalchemy.types.REAL,
        "fpBkgd": sqlalchemy.types.REAL,
        "fpBkgdErr": sqlalchemy.types.REAL,
        "ixxErr": sqlalchemy.types.REAL,
        "iyyErr": sqlalchemy.types.REAL,
        "ixyErr": sqlalchemy.types.REAL,
        "ixx_iyy_Cov": sqlalchemy.types.REAL,
        "ixx_ixy_Cov": sqlalchemy.types.REAL,
        "iyy_ixy_Cov": sqlalchemy.types.REAL,
    },
    "DiaObjectLast": {
        "lastNonForcedSource": sqlalchemy.types.TIMESTAMP,
        "radecMjdTai": sqlalchemy.types.Double,
        "pmRa": sqlalchemy.types.REAL,
        "pmRaErr": sqlalchemy.types.REAL,
        "pmDec": sqlalchemy.types.REAL,
        "pmDecErr": sqlalchemy.types.REAL,
        "parallax": sqlalchemy.types.REAL,
        "parallaxErr": sqlalchemy.types.REAL,
        "pmRa_pmDec_Cov": sqlalchemy.types.REAL,
        "pmRa_parallax_Cov": sqlalchemy.types.REAL,
        "pmDec_parallax_Cov": sqlalchemy.types.REAL,
    },
    "SSSource": {
        "mpcUniqueId": sqlalchemy.types.BigInteger,
        "nearbyObj1": sqlalchemy.types.BigInteger,
        "nearbyObj2": sqlalchemy.types.BigInteger,
        "nearbyObj3": sqlalchemy.types.BigInteger,
        "nearbyObj4": sqlalchemy.types.BigInteger,
        "nearbyObj5": sqlalchemy.types.BigInteger,
        "nearbyObj6": sqlalchemy.types.BigInteger,
        "nearbyObjDist1": sqlalchemy.types.REAL,
        "nearbyObjDist2": sqlalchemy.types.REAL,
        "nearbyObjDist3": sqlalchemy.types.REAL,
        "nearbyObjDist4": sqlalchemy.types.REAL,
        "nearbyObjDist5": sqlalchemy.types.REAL,
        "nearbyObjDist6": sqlalchemy.types.REAL,
        "nearbyObjLnP1": sqlalchemy.types.REAL,
        "nearbyObjLnP2": sqlalchemy.types.REAL,
        "nearbyObjLnP3": sqlalchemy.types.REAL,
        "nearbyObjLnP4": sqlalchemy.types.REAL,
        "nearbyObjLnP5": sqlalchemy.types.REAL,
        "nearbyObjLnP6": sqlalchemy.types.REAL,
        "predictedMagnitude": sqlalchemy.types.REAL,
        "predictedMagnitudeSigma": sqlalchemy.types.REAL,
        "predictedRaSigma": sqlalchemy.types.REAL,
        "predictedDecSigma": sqlalchemy.types.REAL,
        "predictedRaDecCov": sqlalchemy.types.REAL,
    },
}

# Renamed columns by table.
_renamed_columns = {"DiaSource": {"is_negative": "isNegative"}}

# Added columns by table.
_added_columns: dict[str, dict] = {
    "DiaObject": {
        "firstDiaSourceMjdTai": sqlalchemy.types.Double,
        "lastDiaSourceMjdTai": sqlalchemy.types.Double,
    },
    "DiaSource": {
        "templateFlux": sqlalchemy.types.REAL,
        "templateFluxErr": sqlalchemy.types.REAL,
    },
    "DiaObjectLast": {
        "firstDiaSourceMjdTai": sqlalchemy.types.Double,
        "lastDiaSourceMjdTai": sqlalchemy.types.Double,
    },
    "SSSource": {
        "predictedVMagnitude": sqlalchemy.types.REAL,
    },
}

# Changed column types.
_alter_column_type = {"DiaSource": {"bboxSize": (sqlalchemy.types.BigInteger, sqlalchemy.types.Integer)}}

_tables_to_migrate = (
    set(_dropped_columns) | set(_renamed_columns) | set(_added_columns) | set(_alter_column_type)
)


def upgrade() -> None:
    """Upgrade 'schema' tree from 7.0.1 to 8.0.0 (ticket DM-52186).

    Summary of changes:
      - Many columns dropped from DiaObject, DiaSource, and SSSource tables.
      - Few columns added to DiaObject and DiaSource tables.
    """
    with Context(revision) as ctx:
        for table_name in _tables_to_migrate:
            try:
                table = ctx.get_table(table_name)
            except:
                # SSSource and DiaObjectLast may be missing
                if table_name in ("SSSource", "DiaObjectLast"):
                    continue
                raise

            with ctx.batch_alter_table(table_name, copy_from=table) as batch_op:
                if columns_to_add := _added_columns.get(table_name):
                    _LOG.info("Adding %d columns to table %s", len(columns_to_add), table_name)
                    for column_name, column_type in columns_to_add.items():
                        batch_op.add_column(sqlalchemy.Column(column_name, column_type, nullable=True))

                if columns_to_rename := _renamed_columns.get(table_name):
                    _LOG.info("Renaming %d columns in table %s", len(columns_to_rename), table_name)
                    for old_column_name, new_column_name in columns_to_rename.items():
                        batch_op.alter_column(old_column_name, new_column_name=new_column_name)

                if columns_to_drop := _dropped_columns.get(table_name):
                    _LOG.info("Dropping %d columns from table %s", len(columns_to_drop), table_name)
                    for column_name in columns_to_drop:
                        batch_op.drop_column(column_name)

                if changed_types := _alter_column_type.get(table_name):
                    _LOG.info("Changing type of %d columns in table %s", len(changed_types), table_name)
                    for column_name, (_, new_type) in changed_types.items():
                        batch_op.alter_column(column_name, type_=new_type)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        for table_name in _tables_to_migrate:
            try:
                table = ctx.get_table(table_name)
            except:
                # SSSource and DiaObjectLast may be missing
                if table_name in ("SSSource", "DiaObjectLast"):
                    continue
                raise

            with ctx.batch_alter_table(table_name, copy_from=table) as batch_op:
                if columns_to_add := _dropped_columns.get(table_name):
                    _LOG.info("Adding %d columns to table %s", len(columns_to_add), table_name)
                    for column_name, column_type in columns_to_add.items():
                        batch_op.add_column(sqlalchemy.Column(column_name, column_type, nullable=True))

                if columns_to_rename := _renamed_columns.get(table_name):
                    _LOG.info("Renaming %d columns in table %s", len(columns_to_rename), table_name)
                    for old_column_name, new_column_name in columns_to_rename.items():
                        batch_op.alter_column(new_column_name, new_column_name=old_column_name)

                if columns_to_drop := _added_columns.get(table_name):
                    _LOG.info("Dropping %d columns from table %s", len(columns_to_drop), table_name)
                    for column_name in columns_to_drop:
                        batch_op.drop_column(column_name)

                if changed_types := _alter_column_type.get(table_name):
                    _LOG.info("Changing type of %d columns in table %s", len(changed_types), table_name)
                    for column_name, (old_type, _) in changed_types.items():
                        batch_op.alter_column(column_name, type_=old_type)
