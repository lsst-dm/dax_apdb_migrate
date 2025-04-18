
#######################
Typical migration tasks
#######################

Ths page collects some examples of typical migration tasks with some explanation.
As ``dax_apdb_migrate`` is not a part of the regular releases it has to be checked out from Github::

    $ git clone git@github.com:lsst-dm/dax_apdb_migrate
    $ cd dax_apdb_migrate
    $ setup -k -r .
    $ scons

After this the CLI commands should be available for use.


SQL Backend
===========

Examples below assume that the ``APDB_URL`` environment variable is set to a location of a APDB::

    APDB_URL=postgresql://database-server.example.com/apdb

Below a re examples of commands that are typically used with SQL backend.

Note that our usual setup uses shared PostgreSQL database where each APDB instance is located in a separate schema.
It is important to always use correct ``--schema/-s`` option providing correct schema name.
SQLite databases do not use schemas and ``--schema/-s`` should not be given in that case.
Examples below include ``-s SCHEMA_NAME`` for the commands that have that option.


Revision stamping
-----------------

Alembic depends on the ``alembic_version`` table to know the current revision(s) of the database schema.
The database created by APDB initially does not include this table, but APDB stores current versions of its managers in ``metadata`` table (if that table does not exist then versions are assumed to be ``0.1.0``).
Before executing any other migration commands the ``alembic_version`` table needs to be created with the ``apdb-migrate-sql stamp`` command.

To check the existence of the ``alembic_version`` table one can run the ``show-current`` command::

    $ apdb-migrate-sql show-current -s SCHEMA_NAME $APDB_URL

If its output is empty then ``alembic_version`` does not exist and has to be created and current revisions stamped into it with this command::

    $ apdb-migrate-sql stamp -s SCHEMA_NAME $APDB_URL

Once the revisions are stamped the ``show-current`` command should produce non-empty output::

    $ apdb-migrate-sql show-current -s SCHEMA_NAME $APDB_URL
    ApdbSql_0.1.0 (head)
    schema_0.1.0

Note that (head) tag above means that this revision correspond to the latest know revision in that tree.


Checking schema revisions
-------------------------

Current revisions of the schema exist in two places in the database.
Alembic revisions are stored in the ``alembic_version`` table created by the ``stamp`` command as described above.
The APDB stores version numbers of its components in the ``metadata`` table.
The ``show-current`` command displays versions/revisions from both these tables.

Without options it shows revisions from ``alembic_version`` table::

    $ apdb-migrate-sql show-current -s SCHEMA_NAME $APDB_URL
    ApdbSql_0.1.0 (head)
    schema_0.1.0

Passing the ``-v`` option to this command will show more verbose output that also includes the location of the migration script for that revision.
The ``(head)`` tag above means that this revision is the latest one in the revision history.
If ``(head)`` is missing then newer revisions exist in the revision history, and migrations can be performed on that tree.

With the ``--metadata`` (or ``-m``) option this command displays information from the ``metadata`` table::

    $ apdb-migrate-sql show-current --metadata -s SCHEMA_NAME $APDB_URL
    ApdbSql: 0.1.0 -> ApdbSql_0.1.0 (head)
    schema: 0.1.0 -> schema_0.1.0

The output includes tree name and its version number.
The revision number appearing here is calculated from those two items and it must match one of the revisions in ``alembic_version`` table.


Running schema upgrades
-----------------------

If database has an obsolete schema revision for one of the managers one can upgrade it by running a migration script with the ``upgrade`` command.
The first step is to decide which migrations need to be performed.
Suppose you look at the ``show-current`` output and noticed that it shows revision ``schema_0.1.0`` without ``(head)``.

To check which new revisions exist run the ``show-history`` command for that manager::

    $ apdb-migrate-sql show-history schema
    schema_0.1.1 -> schema_1.0.0 (schema) (head), Migration script for schema 1.0.0.
    schema_0.1.0 -> schema_0.1.1 (schema), Migration script for schema 0.1.1.
    schema_root -> schema_0.1.0 (schema), Migration script for schema 0.1.0.
    <base> -> schema_root (schema), An initial pseudo-revision of the 'schema' tree.

You can tell that revision ``schema_0.1.0`` can be upgraded to ``schema_0.1.1``, and the latter can be also upgraded to ``schema_1.0.0``.

Migration can be performed as separate steps, providing an explicit revision number for each step, or as one command specifying final revision.
An example of migration with two separate steps that perform the upgrade to the latest version 1.0.0 are::

    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_0.1.1
    $ apdb-migrate-sql upgrade -s SCHEMA_NAME $APDB_URL schema_1.0.0

After that ``show-current`` should show ``schema_1.0.0 (head)`` in its output.

Usually migration scripts are running in a single transaction, if migration fails for some reason, the state of the schema should remain unchanged.

Some migrations may require additional command line arguments which are passed via ``--options KEY=VALUE`` option.
Individual scripts detect when such options are necessary and will produce a message when options are missing.


Downgrading schema
------------------

It is possible to also switch the schema to a previous revision via ``downgrade`` command.
The command takes a revision number which should be a revision preceding the current one.
For example, to downgrade ``schema`` tree from current ``schema_1.0.0`` to previous ``schema_0.1.1`` run this command::

    $ apdb-migrate-sql downgrade -s SCHEMA_NAME $APDB_URL schema_0.1.1

Of course, the migration script has to implement the ``downgrade()`` method for this, which may not always be true.


Cassandra Backend
=================

Many aspects of Cassandra revision management are similar to SQL case.
One obvious distinction is that Cassandra connection does not use URL, but a host name and keyspace name instead.
Any host in a Cassandra cluster can be specified when executing migration scripts.

Cassandra does not store Alembic revisions in a database, instead we generate them in a temporary SQLite database on each command.
As a result, a separate step of stamping of the revisions is not needed for Cassandra.

Checking schema revisions
-------------------------

The ``show-current`` command reports version numbers stored in APDB ``metadata`` table and their corresponding Alembic revisions:

    $ apdb-migrate-cassandra show-current <host> <keyspace>
    ApdbCassandra: 0.1.1 -> ApdbCassandra_0.1.1 (head)
    ApdbCassandraReplica: 1.0.0 -> ApdbCassandraReplica_1.0.0 (head)
    schema: 5.0.0 -> schema_5.0.0

Updating schema
---------------

Upgrading and downgrading APDB schema works similarly to SQL, except that ``apdb-migrate-cassandra`` is used, e.g.:

    $ apdb-migrate-cassandra upgrade <host> <keyspace> schema_6.0.0

Some migration scripts may not implement ``downgrade()`` method, in that case ``downgrade`` command will raise ``NotImplementedError`` exception.
