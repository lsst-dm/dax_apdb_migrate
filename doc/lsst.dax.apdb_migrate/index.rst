.. py:currentmodule:: lsst.dax.apdb_migrate

.. _lsst.dax.apdb_migrate:

#####################
lsst.dax.apdb_migrate
#####################

.. _lsst.dax.apdb_migrate-using:

.. toctree::
    :maxdepth: 1

Using migration tools
=====================

This package provides tools for performing database schema migration on APDB instances, both relational and Cassandra.
Database migrations are defined as scripts executed by these tools, this package also serves as a repository for migration scripts.

.. toctree::
   :maxdepth: 1

   concepts.rst
   command-line.rst
   migration-scripts.rst
   typical-tasks.rst
   common-migrations.rst

Migrations catalog
==================

Links below lead to the description of existing migration scripts for each of the migration trees.

Migrations that exist for SQL backend:

.. toctree::
   :maxdepth: 1

   migrations/sql/schema.rst
   migrations/sql/ApdbSql.rst

Migrations that exist for Cassandra backend:

.. toctree::
   :maxdepth: 1

   migrations/cassandra/schema.rst
   migrations/cassandra/ApdbCassandra.rst


Implementation details
======================

``dax_apdb_migrate`` does not provide public API.
This package is also very special because it is supposed to work with database schemas created by different (even incompatible) releases.
Due to that it cannot depend directly on many features of ``dax_apdb``, dependencies, if any, will be limited to the most stable parts of its API.
A small subset of ``dax_apdb`` API was re-implemented in this package to avoid dependency issues.

Even with the very limited dependencies it is not guaranteed that ``dax_apdb_migrate`` will be completely backward compatible.
Migrating older registries may require use of older releases and older version of this package.
