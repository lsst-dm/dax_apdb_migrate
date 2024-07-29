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
