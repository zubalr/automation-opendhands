"""Tests for durable AutomationRun bash log snapshots."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from openhands.automation.utils.run_logs import (
    SNAPSHOT_STREAM_CAP_BYTES,
    RunLogSnapshot,
    apply_run_log_snapshot,
    collect_bash_output_snapshot,
    snapshot_from_bash_outputs,
    snapshot_values_for_run,
    snapshot_with_fallback_exit_code,
)


def test_concat_multi_chunk_bash_output_in_timestamp_order():
    """Multi-chunk BashOutput is concatenated in timestamp order, not last-only."""
    events = [
        {
            "id": "2",
            "timestamp": "2026-08-29T12:00:02Z",
            "stdout": "two",
            "stderr": "e2",
            "exit_code": 0,
        },
        {
            "id": "1",
            "timestamp": "2026-08-29T12:00:01Z",
            "stdout": "one",
            "stderr": "e1",
            "exit_code": None,
        },
    ]

    snapshot = snapshot_from_bash_outputs(events)

    assert snapshot is not None
    assert snapshot.exit_code == 0
    assert snapshot.stdout == "onetwo"
    assert snapshot.stderr == "e1e2"
    assert snapshot.logs_truncated is False


def test_snapshot_none_until_exit_code_present():
    """Streaming chunks without an exit_code are not snapshotted."""
    events = [
        {
            "timestamp": "2026-08-29T12:00:01Z",
            "stdout": "still running",
            "stderr": "",
            "exit_code": None,
        }
    ]
    assert snapshot_from_bash_outputs(events) is None


def test_snapshot_keeps_streams_when_exit_code_not_required():
    """Keep streams when require_exit_code is False."""
    events = [
        {
            "timestamp": "2026-08-29T12:00:01Z",
            "stdout": "still running",
            "stderr": "warn",
            "exit_code": None,
        }
    ]
    snapshot = snapshot_from_bash_outputs(events, require_exit_code=False)
    assert snapshot is not None
    assert snapshot.exit_code is None
    assert snapshot.stdout == "still running"
    assert snapshot.stderr == "warn"


def test_fallback_exit_code_uses_callback_when_bash_still_running():
    """Callback 0/1 fills a missing bash exit_code."""
    partial = RunLogSnapshot(
        exit_code=None, stdout="partial", stderr="e", logs_truncated=False
    )
    filled = snapshot_with_fallback_exit_code(partial, 0)
    assert filled.exit_code == 0
    assert filled.stdout == "partial"
    assert filled.stderr == "e"

    empty = snapshot_with_fallback_exit_code(None, 1)
    assert empty.exit_code == 1
    assert empty.stdout == ""
    assert empty.stderr == ""


def test_truncation_flag_and_tail_prefer_stderr():
    """Streams over the cap keep the tail; logs_truncated is set if either is cut."""
    cap = 16
    stdout = "HEAD" + ("x" * 40) + "TAIL"
    stderr = "old-error\n" + ("y" * 40) + "FINAL_ERR"

    snapshot = snapshot_from_bash_outputs(
        [
            {
                "timestamp": "2026-08-29T12:00:01Z",
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": 1,
            }
        ],
        cap_bytes=cap,
    )

    assert snapshot is not None
    assert snapshot.logs_truncated is True
    assert snapshot.stdout.encode("utf-8").endswith(b"TAIL")
    assert len(snapshot.stdout.encode("utf-8")) <= cap
    assert snapshot.stderr.endswith("FINAL_ERR")
    assert len(snapshot.stderr.encode("utf-8")) <= cap


def test_default_cap_is_256_kib():
    assert SNAPSHOT_STREAM_CAP_BYTES == 256 * 1024


def test_string_exit_code_is_coerced_to_int():
    snapshot = snapshot_from_bash_outputs(
        [{"timestamp": "2026-08-29T12:00:01Z", "stdout": "x", "exit_code": "0"}]
    )
    assert snapshot is not None
    assert snapshot.exit_code == 0
    assert isinstance(snapshot.exit_code, int)


def test_apply_snapshot_is_idempotent():
    """A second write must not overwrite a non-null snapshot."""
    run = SimpleNamespace(exit_code=None, stdout=None, stderr=None, logs_truncated=None)
    first = RunLogSnapshot(exit_code=0, stdout="first", stderr="", logs_truncated=False)
    second = RunLogSnapshot(
        exit_code=1, stdout="second", stderr="boom", logs_truncated=True
    )

    assert apply_run_log_snapshot(run, first) is True
    assert apply_run_log_snapshot(run, second) is False
    assert run.exit_code == 0
    assert run.stdout == "first"
    assert run.stderr == ""
    assert run.logs_truncated is False
    assert snapshot_values_for_run(run, second) == {}


@pytest.mark.asyncio
async def test_collect_paginates_and_concats_chunks():
    """collect_bash_output_snapshot follows next_page_id and concats bodies."""
    pages = [
        {
            "items": [
                {
                    "id": "1",
                    "timestamp": "2026-08-29T12:00:01Z",
                    "stdout": "aaa",
                    "stderr": "",
                    "exit_code": None,
                }
            ],
            "next_page_id": "page-2",
        },
        {
            "items": [
                {
                    "id": "2",
                    "timestamp": "2026-08-29T12:00:02Z",
                    "stdout": "bbb",
                    "stderr": "err",
                    "exit_code": 0,
                }
            ],
            "next_page_id": None,
        },
    ]
    mock_client = MagicMock(spec=httpx.AsyncClient)
    responses = []
    for page in pages:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = page
        responses.append(resp)
    mock_client.get = AsyncMock(side_effect=responses)

    snapshot = await collect_bash_output_snapshot(
        mock_client, "http://agent", "session-key", "cmd-1"
    )

    assert snapshot is not None
    assert snapshot.exit_code == 0
    assert snapshot.stdout == "aaabbb"
    assert snapshot.stderr == "err"
    assert mock_client.get.await_count == 2
    first_params = mock_client.get.await_args_list[0].kwargs["params"]
    assert first_params["command_id__eq"] == "cmd-1"
    assert first_params["sort_order"] == "TIMESTAMP"
    assert first_params["limit"] == 100
    assert "page_id" not in first_params
    second_params = mock_client.get.await_args_list[1].kwargs["params"]
    assert second_params["page_id"] == "page-2"


@pytest.mark.asyncio
async def test_get_run_logs_returns_stored_snapshot(async_client, async_session):
    """GET /v1/runs/{id}/logs returns the durable snapshot columns."""
    import uuid

    from openhands.automation.models import (
        Automation,
        AutomationRun,
        AutomationRunStatus,
    )

    user_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    org_id = uuid.UUID("87654321-4321-8765-4321-876543218765")
    automation = Automation(
        user_id=user_id,
        org_id=org_id,
        name="Logs Automation",
        trigger={"type": "cron", "schedule": "0 9 * * *", "timezone": "UTC"},
        tarball_path="s3://bucket/code.tar.gz",
        entrypoint="uv run script.py",
    )
    async_session.add(automation)
    await async_session.commit()

    run = AutomationRun(
        automation_id=automation.id,
        status=AutomationRunStatus.COMPLETED,
        bash_command_id="abc123",
        exit_code=0,
        stdout="hello",
        stderr="warn",
        logs_truncated=False,
    )
    async_session.add(run)
    await async_session.commit()

    response = await async_client.get(f"/api/automation/v1/runs/{run.id}/logs")
    assert response.status_code == 200
    assert response.json() == {
        "exit_code": 0,
        "stdout": "hello",
        "stderr": "warn",
        "logs_truncated": False,
    }


@pytest.mark.asyncio
async def test_complete_run_persists_logs_before_bash_exits(
    async_client, async_session
):
    """/complete persists streams when bash exit_code is still null."""
    import uuid

    from openhands.automation.models import (
        Automation,
        AutomationRun,
        AutomationRunStatus,
    )

    user_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    org_id = uuid.UUID("87654321-4321-8765-4321-876543218765")
    automation = Automation(
        user_id=user_id,
        org_id=org_id,
        name="Callback Before Exit",
        trigger={"type": "cron", "schedule": "0 9 * * *", "timezone": "UTC"},
        tarball_path="s3://bucket/code.tar.gz",
        entrypoint="uv run script.py",
    )
    async_session.add(automation)
    await async_session.commit()

    run = AutomationRun(
        automation_id=automation.id,
        status=AutomationRunStatus.RUNNING,
        bash_command_id="cmd-still-running",
    )
    async_session.add(run)
    await async_session.commit()

    snapshot = RunLogSnapshot(
        exit_code=None,
        stdout="partial-out",
        stderr="partial-err",
        logs_truncated=False,
    )
    with patch(
        "openhands.automation.router.fetch_run_log_snapshot_for_run",
        new_callable=AsyncMock,
        return_value=snapshot,
    ) as mock_fetch:
        response = await async_client.post(
            f"/api/automation/v1/runs/{run.id}/complete",
            json={"status": "COMPLETED"},
        )
        mock_fetch.assert_awaited()
        assert mock_fetch.await_args.kwargs.get("require_exit_code") is False

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "COMPLETED"
    assert data["exit_code"] == 0
    assert data["stdout"] == "partial-out"
    assert data["stderr"] == "partial-err"

    await async_session.refresh(run)
    assert run.exit_code == 0
    assert run.stdout == "partial-out"
    assert run.stderr == "partial-err"


@pytest.mark.asyncio
async def test_complete_run_failed_fallback_exit_code_when_events_gone(
    async_client, async_session
):
    """Callback FAILED still stores exit_code=1 when events are gone."""
    import uuid

    from openhands.automation.models import (
        Automation,
        AutomationRun,
        AutomationRunStatus,
    )

    user_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    org_id = uuid.UUID("87654321-4321-8765-4321-876543218765")
    automation = Automation(
        user_id=user_id,
        org_id=org_id,
        name="Callback No Events",
        trigger={"type": "cron", "schedule": "0 9 * * *", "timezone": "UTC"},
        tarball_path="s3://bucket/code.tar.gz",
        entrypoint="uv run script.py",
    )
    async_session.add(automation)
    await async_session.commit()

    run = AutomationRun(
        automation_id=automation.id,
        status=AutomationRunStatus.RUNNING,
        bash_command_id="cmd-pruned",
    )
    async_session.add(run)
    await async_session.commit()

    with patch(
        "openhands.automation.router.fetch_run_log_snapshot_for_run",
        new_callable=AsyncMock,
        return_value=None,
    ):
        response = await async_client.post(
            f"/api/automation/v1/runs/{run.id}/complete",
            json={"status": "FAILED", "error": "entrypoint crashed"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "FAILED"
    assert data["exit_code"] == 1

    await async_session.refresh(run)
    assert run.exit_code == 1
    assert run.stdout == ""
    assert run.stderr == ""
