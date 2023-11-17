"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
import sqlalchemy as sa
from alembic import op
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade():
    """Upgrade database schema to a new version."""
    ${upgrades if upgrades else "raise NotImplementedError()"}


def downgrade():
    """Downgrade database schema to a previous version."""
    ${downgrades if downgrades else "raise NotImplementedError()"}
