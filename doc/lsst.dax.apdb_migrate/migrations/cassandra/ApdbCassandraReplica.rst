############################################
Migrations for ``ApdbCassandraReplica`` tree
############################################

Initial version for this tree is 1.0.0.

Upgrade from 1.0.0 to 1.1.0
===========================

Migration script: `ApdbCassandraReplica_1.1.0.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandraReplica/ApdbCassandraReplica_1.1.0.py>`_

This schema upgrade adds support for replica tables sub-partitioning, which improves performance of writing to replica chunk tables.
Summary of changes to the schema:

- ``ApdbReplicaChunks`` table adds new boolean column ``has_subchunks``.
- ``DiaSourceToPartition`` table adds new column ``apdb_replica_subchunk``.
- Three new replica chunk tables are added - ``DiaObjectChunks2``, ``DiaSourceChunks2``, and ``DiaForcedSourceChunks2``.

The old replica chunks tables (e.g. ``DiaObjectChunks``) are not removed as they may contain data from existing chunks.
These tables will need to be removed manually once all their chunks are removed.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandraReplica_1.1.0


Upgrade from 1.1.0 to 1.1.1
===========================

Migration script: `ApdbCassandraReplica_1.1.1.py <https://github.com/lsst-dm/dax_apdb_migrate/blob/main/migrations/cassandra/ApdbCassandraReplica/ApdbCassandraReplica_1.1.1.py>`_

This schema upgrade adds ``ApdbUpdateRecordChunks`` table used for replicating changes to the existing table records.

An example command for applying the schema upgrade::

    $ apdb-migrate-cassandra upgrade <host> <keyspace> ApdbCassandraReplica_1.1.1
