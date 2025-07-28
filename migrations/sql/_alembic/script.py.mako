"""${message}

Revision ID: ${up_revision}
Revises:${" " if down_revision else ""}${down_revision | comma,n}
Create Date: ${create_date}
"""
<%!
    def dq(item):
        if isinstance(item, str):
            return f'"{item}"'
        elif isinstance(item, tuple):
            r = ", ".join(dq(i) for i in item)
            if len(item) == 1:
                r += ","
            return f"({r})"
        elif isinstance(item, (list, tuple)):
            r = ", ".join(dq(i) for i in item)
            return f"[{r}]"
        else:
            return item
%>
import logging

import alembic
import sqlalchemy
from lsst.dax.apdb_migrate.sql.context import Context
${imports if imports else ""}
# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade '...' tree from ... to ... (ticket ...).

    Summary of changes:
      - <Add summary of changes>.
    """
    with Context(revision) as ctx:
        ${upgrades if upgrades else "raise NotImplementedError()"}


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:
        ${downgrades if downgrades else "raise NotImplementedError()"}
