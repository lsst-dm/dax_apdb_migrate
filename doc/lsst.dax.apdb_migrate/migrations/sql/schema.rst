##############################
Migrations for ``schema`` tree
##############################

Upgrade from 0.1.0 to 0.1.1
===========================

Migration script: `schema_0.1.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/sql/schema/schema_0.1.1.py>`_

This migration creates ``metadata`` table to the schema which was added in version ``0.1.1`` of ``apdb.yaml``.
It also populates metadata with version numbers for both ``schema`` and ``ApdbSql`` trees (usually migreations only update their own tree version).
The ``ApdbSql`` schema version store in ``metadata`` will be ``0.1.0`` by default, but it can be changed via the command line option.

An example of specifying different version number for ApdbSql::

    $ apdb-migrate-sql upgrade --options apdb_sql_version=0.2.0 $APDB_URL schema_0.1.1
