##############################
Migrations for ``schema`` tree
##############################

Upgrade from 0.1.0 to 0.1.1
===========================

Migration script: `schema_0.1.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_0.1.1.py>`_

This migration creates ``metadata`` table to the schema which was added in version ``0.1.1`` of ``apdb.yaml``.
It also populates metadata with version numbers for both ``schema`` and ``ApdbSql`` trees (usually migrations only update their own tree version).
The ``ApdbSql`` schema version stored in ``metadata`` will be ``0.1.0`` by default, but it can be changed via the command line option.

An example of specifying different version number for ApdbSql::

    $ apdb-migrate-sql upgrade --options apdb_sql_version=0.2.0 -s SCHEMA_NAME $APDB_URL schema_0.1.1


Upgrade from 0.1.1 to 1.0.0
===========================

Migration script: `schema_1.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_1.0.0.py>`_

This migration implements two unrelated schema changes:

  - replacing ``ccdVisitId`` column in source tables with ``visit`` and ``detector`` columns (`DM-42435 <https://rubinobs.atlassian.net/browse/DM-42435>`_)
  - replacing ``DiaSource.flags`` bitmask column with individual boolean columns for each flag (`DM-41530 <https://rubinobs.atlassian.net/browse/DM-41530>`_)

Unpacking ``ccdVisitId`` value into ``visit`` and ``detector`` is implemented by specific instrument code and uses Butler registry to locate packer parameters.
This requires passing additional parameters to the migration script specifying Butler repository and instrument name.
The same Butler repository that was used for processing of APDB data should be specified to avoid potential mismatch in configuration.
Instrument code needs corresponding packages to be setup, e.g. LATISS needs ``obs_lsst`` package.
If script cannot import corresponding instrument classes, it will raise an exception and migration will be aborted.

An example of migrating APDB populated from LATISS data::

    $ setup -k obs_lsst
    $ apdb-migrate-sql upgrade -s SCHEMA_NAME --options butler-repo=/repo/main --options instrument=LATISS $APDB_URL schema_1.0.0


Upgrade from 1.0.0 to 1.1.0
===========================

Migration script: `schema_1.1.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_1.1.0.py>`_

This migration removes multiple BLOB columns from ``DiaObject`` table (and ``DiaObjectChunks`` if that is present).
Column names are ``{b}_lcPeriodic`` and ``{b}_lcNonPeriodic`` where ``{b}`` is one of six band names (``ugrizy``).
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_1.1.0


Upgrade from 1.1.0 to 2.0.0
===========================

Migration script: `schema_2.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_2.0.0.py>`_

This migration drops ``x`` and ``y`` columns from ``DiaForcedSource`` table and adds ``ra`` and ``dec`` columns.
New columns are populated from the same columns of the matching ``DiaObject`` records.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_2.0.0

Upgrade from 2.0.0 to 2.0.1
===========================

Migration script: `schema_2.0.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_2.0.1.py>`_

This migration adds ``pixelScale`` column to ``DetectorVisitProcessingSummary`` table.
At this point the table is not supported by ``dax_apdb``, so this change is a patch-level change.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_2.0.1

Upgrade from 2.0.1 to 3.0.0
===========================

Migration script: `schema_3.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_3.0.0.py>`_

This migration adds ``dipoleFitAttempted`` column to ``DiaSource`` table, initially set to ``NULL``.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_3.0.0

Upgrade from 3.0.0 to 4.0.0
===========================

Migration script: `schema_4.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_4.0.0.py>`_

This migration adds ``nDiaSources`` column to ``DiaObjectLast`` table.
The column is filled with the actual count of matching sources in ``DiaSource`` table.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_4.0.0

Upgrade from 4.0.0 to 5.0.0
===========================

Migration script: `schema_5.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_5.0.0.py>`_

This migration adds ``is_negative`` column to ``DiaSource`` table, initially set to ``NULL``.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_5.0.0

Upgrade from 5.0.0 to 6.0.0
===========================

Migration script: `schema_6.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_6.0.0.py>`_

This migration adds ``pixelFlags_nodata`` and ``pixelFlags_nodataCenter`` columns to ``DiaSource`` table, initially set to ``NULL``.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_6.0.0

Upgrade from 6.0.0 to 7.0.0
===========================

Migration script: `schema_7.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_7.0.0.py>`_

This migration adds ``glint_trail`` column to ``DiaSource`` table, initially set to ``NULL``.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_7.0.0

Upgrade from 7.0.0 to 7.0.1
===========================

Migration script: `schema_7.0.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_7.0.1.py>`_

Version 7.0.1 updates schema of ``MPCORB`` table, dropping and adding few columns.
None of the existing APDB instances has ``MPCORB`` table yet, this migration is a no-op.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_7.0.1

Upgrade from 7.0.1 to 8.0.0
===========================

Migration script: `schema_8.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_8.0.0.py>`_

Version 8.0.0 drops multiple columns from DiaSource and DiaObject tables, adding or renaming a small number of columns.
Details of changes to the schema can be seen on `DM-50837 <https://rubinobs.atlassian.net/browse/DM-50837>`_.
No additional parameters or packages are needed for this script.
Migration to ``ApdbSql_1.0.0`` should be performed after migration to ``schema_8.0.0``.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_8.0.0

Upgrade from 8.0.0 to 9.0.0
===========================

Migration script: `schema_9.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_9.0.0.py>`_

Version 9.0.0 replaces native timestamp columns with MJD TAI (double precisiton).
Columns are also renamed to have "MjdTai" suffix.
No additional parameters or packages are needed for this script.
Migration to ``ApdbSql_1.1.0`` should be performed together and after migration to ``schema_9.0.0``.

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_9.0.0
