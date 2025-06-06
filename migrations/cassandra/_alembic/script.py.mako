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

from lsst.dax.apdb_migrate.cassandra.context import Context
${imports if imports else ""}
# revision identifiers, used by Alembic.
revision = ${dq(up_revision)}
down_revision = ${dq(down_revision)}
branch_labels = ${dq(branch_labels)}
depends_on = ${dq(depends_on)}

_LOG = logging.getLogger(__name__)


def upgrade() -> None:
    """Upgrade '...' tree from ... to ... (ticket ...).

    Summary of changes:
      - <Add summary of changes>.
    """
    with Context(revision) as ctx:  # noqa: F841
        # Add code to upgrade the schema using ctx.execute() method.
        raise NotImplementedError()


def downgrade() -> None:
    """Undo changes applied in `upgrade`."""
    with Context(down_revision) as ctx:  # noqa: F841
        # Add code to downgrade the schema using ctx.execute() method.
        raise NotImplementedError()
