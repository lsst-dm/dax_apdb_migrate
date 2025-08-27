##############################
Migrations for ``schema`` tree
##############################

Migrations to schema versions earlier than 3.0.0 were not implemented, as we do not have known instances with schema earlier than 2.0.1.

Upgrade from 2.0.1 to 3.0.0
===========================

Migration script: `schema_3.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/schema/schema_3.0.0.py>`_

This migration adds ``dipoleFitAttempted`` column to ``DiaSource`` table, initially set to ``NULL``.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> schema_3.0.0

Upgrade from 3.0.0 to 4.0.0
===========================

Migration script: `schema_4.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/schema/schema_4.0.0.py>`_

This migration adds ``nDiaSources`` column to ``DiaObjectLast`` table.
The column is filled with the actual count of matching sources in ``DiaSource`` table.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> schema_4.0.0

Upgrade from 4.0.0 to 5.0.0
===========================

Migration script: `schema_5.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/schema/schema_5.0.0.py>`_

This migration adds ``is_negative`` column to ``DiaSource`` table (and ``DiaSourceChunks`` if it exists), initially set to ``NULL``.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> schema_5.0.0

Upgrade from 5.0.0 to 6.0.0
===========================

Migration script: `schema_6.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/schema/schema_6.0.0.py>`_

This migration adds ``pixelFlags_nodata`` and ``pixelFlags_nodataCenter`` columns to ``DiaSource`` table (and ``DiaSourceChunks`` if it exists), initially set to ``NULL``.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> schema_6.0.0

Upgrade from 6.0.0 to 7.0.0
===========================

Migration script: `schema_7.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/schema/schema_7.0.0.py>`_

This migration adds ``glint_trail`` column to ``DiaSource`` table(s) (and ``DiaSourceChunks`` if it exists), initially set to ``NULL``.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> schema_7.0.0

Upgrade from 7.0.0 to 7.0.1
===========================

Migration script: `schema_7.0.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/schema/schema_7.0.1.py>`_

Version 7.0.1 updates schema of ``MPCORB`` table, dropping and adding few columns.
None of the existing APDB instances has ``MPCORB`` table yet, this migration is a no-op.
No additional parameters or packages are needed for this script.

An example of migration::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> schema_7.0.1

Upgrade from 7.0.1 to 8.0.0
===========================

Migration script: `schema_8.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/schema/schema_8.0.0.py>`_

Version 8.0.0 drops multiple columns from DiaSource and DiaObject tables, adding or renaming a small number of columns.
Details of changes to the schema can be seen on `DM-50837 <https://rubinobs.atlassian.net/browse/DM-50837>`_.
The column ``DiaSource.bboxSize`` changed type from ``long`` to ``int`` in ``sdm_schemas``, but because Cassandra does not support changed to column types, its type will remain unchanged after migration.

No additional parameters or packages are needed for this script.

Dependencies:

- Migration to ``schema_8.0.0`` has to be performed after ``ApdbCassandra_0.1.1``.
- Migration to ``ApdbCassandra_1.0.0`` should be performed after migration to ``schema_8.0.0``.

An example of migration::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> schema_8.0.0

Upgrade from 8.0.0 to 9.0.0
===========================

Migration script: `schema_9.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/schema/schema_9.0.0.py>`_

Version 9.0.0 replaces native timestamp columns with MJD TAI (double precisiton).
Columns are also renamed to have "MjdTai" suffix.

.. note::
    This migration exist as a placeholder, but it is not implemented.
    Implementing it would require a lot of work and using external tools (dsbulk), and it would be very slow for reasonably-sized data.
    The only way to use schema 9.0.0 with Cassandra is to create an empty database with that schema version.
