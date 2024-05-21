###############################
Migrations for ``ApdbSql`` tree
###############################

Upgrade from 0.1.0 to 0.1.1
===========================

Migration script: `ApdbSql_0.1.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/ApdbSql/ApdbSql_0.1.1.py>`_

This migration updates timestamp columns to have TIMESTAMP WITH TIMEZONE type.
Only Postgres supports TIMEZONE types, thhis upgrade does nothing for SQLite (but updates version in metadata).

An example of migration::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL ApdbSql_0.1.1
