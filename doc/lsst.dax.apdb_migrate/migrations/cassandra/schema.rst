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
