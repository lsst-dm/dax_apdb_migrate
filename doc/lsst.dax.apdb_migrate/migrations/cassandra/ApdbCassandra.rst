#####################################
Migrations for ``ApdbCassandra`` tree
#####################################

Upgrade from 0.1.0 to 0.1.1
===========================

Migration script: `ApdbCassandra_0.1.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandra/ApdbCassandra_0.1.1.py>`_

This schema upgrade adds an additional table ``DiaObjectLastToPartition`` used to avoid duplicate records in ``DiaObjectLast`` table.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandra_0.1.1


Upgrade from 0.1.1 to 0.1.2
===========================

Migration script: `ApdbCassandra_0.1.2.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandra/ApdbCassandra_0.1.2.py>`_

This schema upgrade adds an additional table ``ApdbVisitDetector`` to store visit/detectors processed by AP pipeline.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandra_0.1.2


Upgrade from 0.1.2 to 0.1.3
===========================

Migration script: `ApdbCassandra_0.1.3.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandra/ApdbCassandra_0.1.3.py>`_

This schema upgrade adds metadata entry with key "config:time-partition-range.json" which contains the range of time partitions.
This new key only exists when APDB is configured with per-partiton tables.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandra_0.1.3


Upgrade from 0.1.3 to 1.0.0
===========================

Migration script: `ApdbCassandra_1.0.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandra/ApdbCassandra_1.0.0.py>`_

``ApdbCassandra`` code was updated to not fill ``lastNonForcedSource`` field, which makes it incompatible with the previous releases.
The actual change to the schema (dropping of ``lastNonForcedSource`` and many other columns) is performed by migration to ``schema_8.0.0``.
This migration only changes version of ``ApdbSql`` tree in the metadata.
This migration has to be applied after migration to ``schema_8.0.0``, it will fail if ``schema`` version is older than ``8.0.0``.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandra_1.0.0


Upgrade from 1.0.0 to 1.1.0
===========================

Migration script: `ApdbCassandra_1.1.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandra/ApdbCassandra_1.1.0.py>`_

``ApdbCassandra`` code was updated to use MJD TAI values for timestamp columns, and it is compatible with the schema that uses native timestamp types.
MJD TAI timestamp columns were introduced with ``schema_9.0.0``.
This migration only changes version of ``ApdCassandra`` tree in the metadata.

Dependencies:

- This migration requires revision ``schema_9.0.0`` in the database.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandra_1.1.0

.. note::
    Cassandra database cannot be migrated to schema 9.0.0 from earlier versions.
    The only way to use schema 9.0.0 with Cassandra is to create an empty database with that schema version, and should also produce ApdbCassandra 1.1.0 or later.


Upgrade from 1.1.0 to 1.2.0
===========================

Migration script: `ApdbCassandra_1.2.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandra/ApdbCassandra_1.2.0.py>`_

``ApdbCassandra`` code was updated to support filling of the ``DiaObjectLast.validityStartMjdTai`` column.
The code is compatible with the previous revision of schema which lacks that column.
The actual change to the schema is performed by migration to ``schema_9.1.0``.
This migration only changes version of ``ApdCassandra`` tree in the metadata.

Dependencies:

- This migration requires revision ``schema_9.1.0`` in the database.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandra_1.2.0
