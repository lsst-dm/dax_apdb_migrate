#####################################
Migrations for ``ApdbCassandra`` tree
#####################################

Upgrade from 0.1.0 to 0.1.1
===========================

Migration script: `ApdbCassandra_0.1.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandra/ApdbCassandra_0.1.1.py>`_

This schema upgrade adds an additional table ``DiaObjectLastToPartition`` used to avoid duplicate records in ``DiaObjectLast`` table.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandra_0.1.1
