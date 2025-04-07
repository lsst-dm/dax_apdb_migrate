
###########################
Common migration operations
###########################

This page collects recipes for common migration tasks happening in the migration scripts.

SQL Backend
===========

Similar page from `daf_butler_migration documentation`_ may provide additional hints for writing migration scripts.

The most important thing to remember is that due to the very limited support of ALTER TABLE syntax in SQLite backend, many migration operations have to use Alembic `batch migrations`_.


Updating metadata table
-----------------------

Each migration script is responsible for updating the version of its corresponding tree in the ``metadata`` table.
Easiest way to do that is by using ``ApdbMetadata`` class, here is a standard way to do it::

    from alembic import op, context
    from lsst.dax.apdb_migrate.sql.apdb_metadata import ApdbMetadata

    def upgrade():
        # Do actual schema upgrade then update metadata
        mig_context = context.get_context()
        schema = mig_context.version_table_schema
        metadata = ApdbMetadata(op.get_bind(), schema)
        metadata.update_tree_version("schema", "0.1.1", insert=True)

The ``update_tree_version`` method will raise an exception if tree is not defined in ``metadata`` table.


Cassandra Backend
=================

Cassandra migration scripts can use a special context manager instance for operations on database.
A context manager is normally instanciated for the duration of the whole upgrade/downgrade method, e.g.::

    from lsst.dax.apdb_migrate.cassandra.context import Context

    def upgrade() -> None:
        with Context(revision) as ctx:
            # Use `ctx` methods to run queries in database
            ...

Most operations involve writing CQL qeries and executing them.
An example of common operation would be adding a column to a table, which may look like this::

    with Context(final_revision) as ctx:
        table = "DiaTable"
        column = "very_new_column"
        query = f'ALTER TABLE "{ctx.keyspace}"."{table}" ADD "{column}" BOOLEAN'
        ctx.update(query)

Few important points to remember:

- Use ``ctx.query()`` method to execute ``SELECT`` queries and ``ctx.update()`` for queries that change anything.
- Always remember to prefix table name with keyspace name like in example above.
- Cassandra support for `schema changes <https://cassandra.apache.org/doc/latest/cassandra/developing/cql/ddl.html>`_ is extremly limited, some updates may not be possible or be very non-trivial.


.. _batch migrations: https://alembic.sqlalchemy.org/en/latest/batch.html
.. _daf_butler_migration documentation: https://daf-butler-migrate.lsst.io/lsst.daf.butler_migrate/index.html
