"""Add durable bash log snapshot columns to automation_runs.

Revision ID: 023
Revises: 021
Create Date: 2026-08-29

Agent-server bash_events are ephemeral (retention prune / DELETE). Snapshot
the outer command's final exit_code and bounded stdout/stderr onto the run
row so verification and the run-logs UI survive after events disappear.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "023"
down_revision: str = "021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _is_sqlite() -> bool:
    return op.get_bind().dialect.name == "sqlite"


def upgrade() -> None:
    op.add_column(
        "automation_runs",
        sa.Column("exit_code", sa.Integer(), nullable=True),
    )
    op.add_column(
        "automation_runs",
        sa.Column("stdout", sa.Text(), nullable=True),
    )
    op.add_column(
        "automation_runs",
        sa.Column("stderr", sa.Text(), nullable=True),
    )
    op.add_column(
        "automation_runs",
        sa.Column("logs_truncated", sa.Boolean(), nullable=True),
    )

    if _is_sqlite():
        return

    op.execute(
        "COMMENT ON COLUMN automation_runs.exit_code IS "
        "'Final outer-bash exit code snapshotted at first non-null result. "
        "NULL means no snapshot yet (live bash_events are still the source).'"
    )
    op.execute(
        "COMMENT ON COLUMN automation_runs.stdout IS "
        "'Bounded stdout snapshot of the outer bash command (tail-capped).'"
    )
    op.execute(
        "COMMENT ON COLUMN automation_runs.stderr IS "
        "'Bounded stderr snapshot of the outer bash command (tail-capped).'"
    )
    op.execute(
        "COMMENT ON COLUMN automation_runs.logs_truncated IS "
        "'True if stdout or stderr was truncated to the per-stream cap.'"
    )


def downgrade() -> None:
    op.drop_column("automation_runs", "logs_truncated")
    op.drop_column("automation_runs", "stderr")
    op.drop_column("automation_runs", "stdout")
    op.drop_column("automation_runs", "exit_code")
