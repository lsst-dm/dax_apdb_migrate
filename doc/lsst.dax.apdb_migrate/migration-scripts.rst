
#################
Migration scripts
#################

``dax_apdb_migrate`` is also a repository of migration scripts located in ``migrations`` folder.

SQL backend
===========


Migration scripts for Alembic-based migrations are located in ``migrations/sql`` folder.
Each migration script represents a single edge in revision trees, Alembic uses these scripts to build a complete revision tree every time it runs.
Alembic itself does not know anything about the migration process, it simply calls these script to perform actual migration steps (upgrades or downgrades).
The only thing that Alembic manages in the database itself is the contents of ``alembic_version`` table, which contains revision(s) of the current database schema.

Here is an example of contents of the ``migrations`` folder::

    $ ls -1 migrations/sql
    _alembic
    ApdbSql
    schema

Many sub-folders there contain regular migration scripts for a corresponding tree (manager type), but there is one special sub-folders in it:

- ``_alembic`` which contains ``env.py`` file used by Alembic and migration script template.

Regular sub-folders contain a bunch of migration scripts, e.g.::

    $ ls -1 migrations/sql/schema
    schema_0.1.0.py
    schema_0.1.1.py
    schema_root.py

``schema_root.py`` is a special placeholder script, without migration code.
Some scripts may not have actual migration code implemented (with empty methods), this usually reflect some past versions for which migrations were not needed.

``apdb-migrate-sql`` has a number of commands to visualize revision trees and create new migration scripts.
Below are some simple examples of the usage for these commands.


Display list of tree names
--------------------------

The command for that is ``apdb-migrate-sql show-trees``, it just list the trees (manager type names) in the migrations folder, e.g.::

    $ apdb-migrate-sql show-trees
    ApdbSql
    schema


Display revision trees
----------------------

The ``apdb-migrate-sql show-history`` will show complete revision history for all trees, which could be long.
Passing an optional tree name will limit output to just that tree::

    $ apdb-migrate-sql show-history schema
    schema_0.1.0 -> schema_0.1.1 (schema) (head), Migration script for schema 0.1.1.
    schema_root -> schema_0.1.0 (schema), Migration script for schema 0.1.0.
    <base> -> schema_root (schema), An initial pseudo-revision of the 'schema' tree.

Option ``-v`` can be used with this and other commands to produce more detailed Alembic output.

Above output includes parent and target revisions for each script, name of the tree, special tags (``head``), and a comment string as defined in a script.


Add new revision tree
---------------------

Before new revisions are added to a tree the tree itself has to be created with ``apdb-migrate-sql add-tree`` command, e.g.::

    $ apdb-migrate-sql add-tree schema
      Creating directory .../migrations/sql/schema ...  done
      Generating .../migrations/sql/schema/schema_root.py ...  done

The ``add-tree`` command creates the corresponding folder inside the ``migrations/sql`` folder and adds a placeholder migration script to it.
This script is not used for actual migration, it defines the tree root with a branch name corresponding to the manager name.


Add new revision
----------------

Adding a new revision to an existing tree is done with the command ``apdb-migrate-sql add-revision`` which takes tree name and a version::

    $ apdb-migrate-sql add-revision schema 0.1.0
      Generating /home/salnikov/lsst/apdb/dax_apdb_migrate/migrations/sql/schema/schema_0.1.0.py ...  done

Usually the initial version of the schema is added to database when APDB is created.
In that case there is no need to populate the first migration script with actual migration code, e.g. the ``schema_0.1.0.py`` script just created can be a placeholder and a starting point for defining further migrations.

Note that this tool does not care about ordering of semantic version numbers and it always uses latest ``(head)`` revision as a parent for each new revision.


Editing migration script
------------------------

Migration scripts contain some metadata used by Alembic and two methods -- ``upgrade()`` and ``downgrade()``.
Generated scripts have these two methods empty and at least the ``upgrade`` method needs to be implemented.
The ``downgrade`` method can be used for reverting migrations and it is also a good idea to implement it, if it can be reasonably done.
If the ``downgrade`` method is not implemented then reverting migration will not be possible using ``apdb-migrate-sql downgrade``.

Implementation of the two methods is not always trivial.
A good starting point for this is the `Alembic`_ documentation and examples in existing migration scripts.


.. _Alembic: https://alembic.sqlalchemy.org/
