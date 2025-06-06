###############################
Migrations for ``ApdbSql`` tree
###############################

Upgrade from 0.1.0 to 0.1.1
===========================

Migration script: `ApdbSql_0.1.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/ApdbSql/ApdbSql_0.1.1.py>`_

This schema upgrade changes all TIMESTAMP columns to TIMESTAMP WITH TIMEZONE.
This is relevant for Postgres only, SQLite does not know about timezones, so the script is no-op for SQLite.

An example command for applying the schema upgrade::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL ApdbSql_0.1.1


Upgrade from 0.1.1 to 0.1.2
===========================

Migration script: `ApdbSql_0.1.2.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/ApdbSql/ApdbSql_0.1.2.py>`_

This migration is only relevant for PostgreSQL backend, and it does nothing of SQLite.
This schema upgrade changes type of some columns from ``double precision`` to ``real``.
The columns declared as ``float`` in Felis schema were originally created with ``sqlalchemy.Float`` type which translated to ``double precision`` in Postgres.
This script requires access to Felis schema (``apdb.yaml``) which needs to be passed as an argument to the script.
Schema version in ``apdb.yaml`` must match exactly schema version in database.
Additionaly ``felis`` package must be setup before running this script.

An example command for applying the schema upgrade::

    $ setup -k felis
    $ apdb-migrate-sql upgrade -s SCHEMA_NAME --options schema-file=/path/to/apdb.yaml $APDB_URL ApdbSql_0.1.2
