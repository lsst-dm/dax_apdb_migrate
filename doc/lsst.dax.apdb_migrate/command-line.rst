##################
Command line tools
##################


SQL Backend
===========

Command line interface for SQL migration tools is implemented as a ``apdb-migrate-sql`` script which has multiple sub-commands.
The commands implemented by ``apdb-migrate-sql`` are mostly wrappers for corresponding Alembic commands/scripts, which hide complexity of the Alembic configuration and APDB-specific knowledge.


There are two classes of ``apdb-migrate-sql`` commands.
Commands from the first class work with migration scripts and revision trees, and do not need access to the database.
These commands are:

- ``show-trees``
- ``show-history``
- ``add-tree``
- ``add-revision``

Commands from other class work with database and require a database URL:

- ``show-current``
- ``stamp``
- ``upgrade``
- ``downgrade``

Sections below describe individual commands and their options.

.. note::

  Please note that documentation of the default values for some options may refer to a file path at a time of documentation generation, actual default paths will be different when CLI command are executed.


.. click:: lsst.dax.apdb_migrate.sql.cli.apdb_migrate_sql:main
   :prog: apdb-migrate-sql
   :show-nested:



Cassandra Backend
=================

Command line interface for Cassandra migration tools is implemented as a ``apdb-migrate-cassandra`` script which has multiple sub-commands.
The commands implemented by ``apdb-migrate-cassandra`` are mostly wrappers for corresponding Alembic commands/scripts, which hide complexity of the Alembic configuration and APDB-specific knowledge.

There are two classes of ``apdb-migrate-cassandra`` commands.
Commands from the first class work with migration scripts and revision trees, and do not need access to the database.
They work very similarly to the ones that exist for SQL backend.
These commands are:

- ``show-trees``
- ``show-history``
- ``add-tree``
- ``add-revision``

Commands from other class work with database and require a database caonnection specified via a host name and a keyspace name:

- ``show-current``
- ``upgrade``
- ``downgrade``

Sections below describe individual commands and their options.

.. note::

  Please note that documentation of the default values for some options may refer to a file path at a time of documentation generation, actual default paths will be different when CLI command are executed.


.. click:: lsst.dax.apdb_migrate.cassandra.cli.apdb_migrate_cassandra:main
   :prog: apdb-migrate-cassandra
   :show-nested:
