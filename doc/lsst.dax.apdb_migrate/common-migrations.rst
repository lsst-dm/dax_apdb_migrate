
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


.. _batch migrations: https://alembic.sqlalchemy.org/en/latest/batch.html
.. _daf_butler_migration documentation: https://daf-butler-migrate.lsst.io/lsst.daf.butler_migrate/index.html
