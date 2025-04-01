"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""

from lsst.dax.apdb_migrate.cassandra.context import Context
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade():
    """Upgrade database schema to a new version."""
    with Context(revision) as ctx:
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError()

def downgrade():
    """Downgrade database schema to a previous version."""
    with Context(down_revision) as ctx:
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError()
