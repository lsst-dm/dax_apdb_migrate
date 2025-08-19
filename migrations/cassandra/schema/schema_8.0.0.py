"""Migration script for schema 8.0.0.

Revision ID: schema_8.0.0
Revises: schema_7.0.1
Create Date: 2025-08-13 20:08:34.303005
"""

import logging

from lsst.dax.apdb_migrate.cassandra.context import Context

# revision identifiers, used by Alembic.
revision = "schema_8.0.0"
down_revision = "schema_7.0.1"
branch_labels = None
depends_on = None

_LOG = logging.getLogger(__name__)

# Names of dropped columns by table.
_dropped_columns = {
    "DiaObject": {
        "radecMjdTai": "DOUBLE",
        "pmRa": "FLOAT",
        "pmRaErr": "FLOAT",
        "pmDec": "FLOAT",
        "pmDecErr": "FLOAT",
        "parallax": "FLOAT",
        "parallaxErr": "FLOAT",
        "pmRa_pmDec_Cov": "FLOAT",
        "pmRa_parallax_Cov": "FLOAT",
        "pmDec_parallax_Cov": "FLOAT",
        "pmParallaxLnL": "FLOAT",
        "pmParallaxChi2": "FLOAT",
        "pmParallaxNdata": "INT",
        "u_psfFluxChi2": "FLOAT",
        "u_fpFluxSigma": "FLOAT",
        "g_psfFluxChi2": "FLOAT",
        "g_fpFluxSigma": "FLOAT",
        "r_psfFluxChi2": "FLOAT",
        "r_fpFluxSigma": "FLOAT",
        "i_psfFluxChi2": "FLOAT",
        "i_fpFluxSigma": "FLOAT",
        "z_psfFluxChi2": "FLOAT",
        "z_fpFluxSigma": "FLOAT",
        "y_psfFluxChi2": "FLOAT",
        "y_fpFluxSigma": "FLOAT",
        "nearbyObj1": "BIGINT",
        "nearbyObj1Dist": "FLOAT",
        "nearbyObj1LnP": "FLOAT",
        "nearbyObj2": "BIGINT",
        "nearbyObj2Dist": "FLOAT",
        "nearbyObj2LnP": "FLOAT",
        "nearbyObj3": "BIGINT",
        "nearbyObj3Dist": "FLOAT",
        "nearbyObj3LnP": "FLOAT",
        "nearbyExtObj1": "BIGINT",
        "nearbyExtObj1Sep": "FLOAT",
        "nearbyExtObj2": "BIGINT",
        "nearbyExtObj2Sep": "FLOAT",
        "nearbyExtObj3": "BIGINT",
        "nearbyExtObj3Sep": "FLOAT",
        "nearbyLowzGal": "TEXT",
        "nearbyLowzGalSep": "FLOAT",
        "u_scienceFluxSigma": "FLOAT",
        "g_scienceFluxSigma": "FLOAT",
        "r_scienceFluxSigma": "FLOAT",
        "i_scienceFluxSigma": "FLOAT",
        "z_scienceFluxSigma": "FLOAT",
        "y_scienceFluxSigma": "FLOAT",
        "u_psfFluxMAD": "FLOAT",
        "u_psfFluxSkew": "FLOAT",
        "u_psfFluxPercentile05": "FLOAT",
        "u_psfFluxPercentile25": "FLOAT",
        "u_psfFluxPercentile50": "FLOAT",
        "u_psfFluxPercentile75": "FLOAT",
        "u_psfFluxPercentile95": "FLOAT",
        "u_psfFluxStetsonJ": "FLOAT",
        "u_psfFluxLinearSlope": "FLOAT",
        "u_psfFluxLinearIntercept": "FLOAT",
        "g_psfFluxMAD": "FLOAT",
        "g_psfFluxSkew": "FLOAT",
        "g_psfFluxPercentile05": "FLOAT",
        "g_psfFluxPercentile25": "FLOAT",
        "g_psfFluxPercentile50": "FLOAT",
        "g_psfFluxPercentile75": "FLOAT",
        "g_psfFluxPercentile95": "FLOAT",
        "g_psfFluxStetsonJ": "FLOAT",
        "g_psfFluxLinearSlope": "FLOAT",
        "g_psfFluxLinearIntercept": "FLOAT",
        "r_psfFluxMAD": "FLOAT",
        "r_psfFluxSkew": "FLOAT",
        "r_psfFluxPercentile05": "FLOAT",
        "r_psfFluxPercentile25": "FLOAT",
        "r_psfFluxPercentile50": "FLOAT",
        "r_psfFluxPercentile75": "FLOAT",
        "r_psfFluxPercentile95": "FLOAT",
        "r_psfFluxStetsonJ": "FLOAT",
        "r_psfFluxLinearSlope": "FLOAT",
        "r_psfFluxLinearIntercept": "FLOAT",
        "i_psfFluxMAD": "FLOAT",
        "i_psfFluxSkew": "FLOAT",
        "i_psfFluxPercentile05": "FLOAT",
        "i_psfFluxPercentile25": "FLOAT",
        "i_psfFluxPercentile50": "FLOAT",
        "i_psfFluxPercentile75": "FLOAT",
        "i_psfFluxPercentile95": "FLOAT",
        "i_psfFluxStetsonJ": "FLOAT",
        "i_psfFluxLinearSlope": "FLOAT",
        "i_psfFluxLinearIntercept": "FLOAT",
        "z_psfFluxMAD": "FLOAT",
        "z_psfFluxSkew": "FLOAT",
        "z_psfFluxPercentile05": "FLOAT",
        "z_psfFluxPercentile25": "FLOAT",
        "z_psfFluxPercentile50": "FLOAT",
        "z_psfFluxPercentile75": "FLOAT",
        "z_psfFluxPercentile95": "FLOAT",
        "z_psfFluxStetsonJ": "FLOAT",
        "z_psfFluxLinearSlope": "FLOAT",
        "z_psfFluxLinearIntercept": "FLOAT",
        "y_psfFluxMAD": "FLOAT",
        "y_psfFluxSkew": "FLOAT",
        "y_psfFluxPercentile05": "FLOAT",
        "y_psfFluxPercentile25": "FLOAT",
        "y_psfFluxPercentile50": "FLOAT",
        "y_psfFluxPercentile75": "FLOAT",
        "y_psfFluxPercentile95": "FLOAT",
        "y_psfFluxStetsonJ": "FLOAT",
        "y_psfFluxLinearSlope": "FLOAT",
        "y_psfFluxLinearIntercept": "FLOAT",
        "lastNonForcedSource": "TIMESTAMP",
    },
    "DiaSource": {
        "x_y_Cov": "FLOAT",
        "psfRa": "DOUBLE",
        "psfRaErr": "FLOAT",
        "psfDec": "DOUBLE",
        "psfDecErr": "FLOAT",
        "psfFlux_psfRa_Cov": "FLOAT",
        "psfFlux_psfDec_Cov": "FLOAT",
        "psfRa_psfDec_Cov": "FLOAT",
        "trailFlux_trailRa_Cov": "FLOAT",
        "trailFlux_trailDec_Cov": "FLOAT",
        "trailFlux_trailLength_Cov": "FLOAT",
        "trailFlux_trailAngle_Cov": "FLOAT",
        "trailRa_trailDec_Cov": "FLOAT",
        "trailRa_trailLength_Cov": "FLOAT",
        "trailRa_trailAngle_Cov": "FLOAT",
        "trailDec_trailLength_Cov": "FLOAT",
        "trailDec_trailAngle_Cov": "FLOAT",
        "trailLength_trailAngle_Cov": "FLOAT",
        "trailLnL": "FLOAT",
        "dipoleRa": "DOUBLE",
        "dipoleRaErr": "FLOAT",
        "dipoleDec": "DOUBLE",
        "dipoleDecErr": "FLOAT",
        "dipoleLengthErr": "FLOAT",
        "dipoleAngleErr": "FLOAT",
        "dipoleMeanFlux_dipoleFluxDiff_Cov": "FLOAT",
        "dipoleMeanFlux_dipoleRa_Cov": "FLOAT",
        "dipoleMeanFlux_dipoleDec_Cov": "FLOAT",
        "dipoleMeanFlux_dipoleLength_Cov": "FLOAT",
        "dipoleMeanFlux_dipoleAngle_Cov": "FLOAT",
        "dipoleFluxDiff_dipoleRa_Cov": "FLOAT",
        "dipoleFluxDiff_dipoleDec_Cov": "FLOAT",
        "dipoleFluxDiff_dipoleLength_Cov": "FLOAT",
        "dipoleFluxDiff_dipoleAngle_Cov": "FLOAT",
        "dipoleRa_dipoleDec_Cov": "FLOAT",
        "dipoleRa_dipoleLength_Cov": "FLOAT",
        "dipoleRa_dipoleAngle_Cov": "FLOAT",
        "dipoleDec_dipoleLength_Cov": "FLOAT",
        "dipoleDec_dipoleAngle_Cov": "FLOAT",
        "dipoleLength_dipoleAngle_Cov": "FLOAT",
        "dipoleLnL": "FLOAT",
        "snapDiffFlux": "FLOAT",
        "snapDiffFluxErr": "FLOAT",
        "fpBkgd": "FLOAT",
        "fpBkgdErr": "FLOAT",
        "ixxErr": "FLOAT",
        "iyyErr": "FLOAT",
        "ixyErr": "FLOAT",
        "ixx_iyy_Cov": "FLOAT",
        "ixx_ixy_Cov": "FLOAT",
        "iyy_ixy_Cov": "FLOAT",
    },
    "DiaObjectLast": {
        "lastNonForcedSource": "TIMESTAMP",
        "radecMjdTai": "DOUBLE",
        "pmRa": "FLOAT",
        "pmRaErr": "FLOAT",
        "pmDec": "FLOAT",
        "pmDecErr": "FLOAT",
        "parallax": "FLOAT",
        "parallaxErr": "FLOAT",
        "pmRa_pmDec_Cov": "FLOAT",
        "pmRa_parallax_Cov": "FLOAT",
        "pmDec_parallax_Cov": "FLOAT",
    },
    "SSSource": {
        "mpcUniqueId": "BIGINT",
        "nearbyObj1": "BIGINT",
        "nearbyObj2": "BIGINT",
        "nearbyObj3": "BIGINT",
        "nearbyObj4": "BIGINT",
        "nearbyObj5": "BIGINT",
        "nearbyObj6": "BIGINT",
        "nearbyObjDist1": "FLOAT",
        "nearbyObjDist2": "FLOAT",
        "nearbyObjDist3": "FLOAT",
        "nearbyObjDist4": "FLOAT",
        "nearbyObjDist5": "FLOAT",
        "nearbyObjDist6": "FLOAT",
        "nearbyObjLnP1": "FLOAT",
        "nearbyObjLnP2": "FLOAT",
        "nearbyObjLnP3": "FLOAT",
        "nearbyObjLnP4": "FLOAT",
        "nearbyObjLnP5": "FLOAT",
        "nearbyObjLnP6": "FLOAT",
        "predictedMagnitude": "FLOAT",
        "predictedMagnitudeSigma": "FLOAT",
        "predictedRaSigma": "FLOAT",
        "predictedDecSigma": "FLOAT",
        "predictedRaDecCov": "FLOAT",
    },
}

# Renamed columns by table.
_renamed_columns = {"DiaSource": {"is_negative": ("isNegative", "BOOLEAN")}}

# Added columns by table.
_added_columns = {
    "DiaObject": {
        "firstDiaSourceMjdTai": "DOUBLE",
        "lastDiaSourceMjdTai": "DOUBLE",
    },
    "DiaSource": {
        "templateFlux": "FLOAT",
        "templateFluxErr": "FLOAT",
    },
    "DiaObjectLast": {
        "firstDiaSourceMjdTai": "DOUBLE",
        "lastDiaSourceMjdTai": "DOUBLE",
    },
    "SSSource": {
        "predictedVMagnitude": "FLOAT",
    },
}

# Changed column types. See notes below.
_alter_column_type = {"DiaSource": {"bboxSize": ("BIGINT", "INT")}}

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
        # ApdbCassandra_0.1.1 depends on a column which is renamed here, check
        # that that migration is already done.
        try:
            ctx.require_version("ApdbCassandra_0.1.1")
        except ValueError as exc:
            raise ValueError(
                "ApdbCassandra version needs to be upgraded to 0.1.1 before this migration can be applied"
            ) from exc

        for table_name in _tables_to_migrate:
            tables = ctx.schema.tables_for_schema(table_name)
            if not tables:
                continue

            for table in tables:
                additions = []
                drops = []

                if columns_to_add := _added_columns.get(table_name):
                    additions += [
                        f'"{column_name}" {column_type}'
                        for column_name, column_type in columns_to_add.items()
                    ]

                if columns_to_rename := _renamed_columns.get(table_name):
                    # Cassandra does not allow renaming, instead drop and add.
                    drops += [f'"{old_column_name}"' for old_column_name, _ in columns_to_rename.items()]
                    additions += [
                        f'"{new_column_name}" {column_type}'
                        for _, (new_column_name, column_type) in columns_to_rename.items()
                    ]

                if columns_to_drop := _dropped_columns.get(table_name):
                    drops += [f'"{column_name}"' for column_name in columns_to_drop]

                # NOTE: Changing column types is not supported in Cassandra, we
                # could drop and re-add the column with a different type, but
                # it will destroy existing data. In this particular case it is
                # possible to keep the existing column type (BIGINT).

                if additions:
                    _LOG.info("Adding %d columns to table %s", len(additions), table)
                    query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD ({", ".join(additions)})'
                    ctx.update(query)

                if drops:
                    _LOG.info("Dropping %d columns from table %s", len(drops), table)
                    query = f'ALTER TABLE "{ctx.keyspace}"."{table}" DROP ({", ".join(drops)})'
                    ctx.update(query)


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        for table_name in _tables_to_migrate:
            tables = ctx.schema.tables_for_schema(table_name)
            if not tables:
                continue

            for table in tables:
                additions = []
                drops = []

                if columns_to_add := _added_columns.get(table_name):
                    drops += [f'"{column_name}"' for column_name in columns_to_add]

                if columns_to_rename := _renamed_columns.get(table_name):
                    # Cassandra does not allow renaming, instead drop and add.
                    additions += [
                        f'"{old_column_name}" {column_type}'
                        for old_column_name, (_, column_type) in columns_to_rename.items()
                    ]
                    drops += [f'"{new_column_name}"' for new_column_name, _ in columns_to_rename.values()]

                if columns_to_drop := _dropped_columns.get(table_name):
                    additions += [
                        f'"{column_name}" {column_type}'
                        for column_name, column_type in columns_to_drop.items()
                    ]

                if additions:
                    _LOG.info("Adding %d columns to table %s", len(additions), table)
                    query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD ({", ".join(additions)})'
                    ctx.update(query)

                if drops:
                    _LOG.info("Dropping %d columns from table %s", len(drops), table)
                    query = f'ALTER TABLE "{ctx.keyspace}"."{table}" DROP ({", ".join(drops)})'
                    ctx.update(query)
