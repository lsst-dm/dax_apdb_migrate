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
