"""Bounded bash stdout/stderr snapshot onto AutomationRun.

Copy only. Never delete agent-server files. Never log snapshot bodies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx

from openhands.automation.utils.agent_server import (
    VerificationOutcome,
    VerificationResult,
)


if TYPE_CHECKING:
    from openhands.automation.models import AutomationRun


logger = logging.getLogger(__name__)

# Hard cap per stream. Tail is preferred so the final error is kept.
SNAPSHOT_STREAM_CAP_BYTES = 256 * 1024
_PAGE_LIMIT = 100
_MAX_PAGES = 200


@dataclass(frozen=True)
class RunLogSnapshot:
    """Bounded copy of outer-bash streams and exit_code."""

    exit_code: int | None
    stdout: str
    stderr: str
    logs_truncated: bool


def _utf8_tail(text: str, cap_bytes: int) -> tuple[str, bool]:
    """Keep the last ``cap_bytes`` of UTF-8, without splitting a character."""
    encoded = text.encode("utf-8")
    if len(encoded) <= cap_bytes:
        return text, False
    tail = encoded[-cap_bytes:]
    return tail.decode("utf-8", errors="ignore"), True


def bound_streams(
    stdout: str,
    stderr: str,
    cap_bytes: int = SNAPSHOT_STREAM_CAP_BYTES,
) -> tuple[str, str, bool]:
    """Cap each stream independently, tail-preferring."""
    stdout, out_cut = _utf8_tail(stdout, cap_bytes)
    stderr, err_cut = _utf8_tail(stderr, cap_bytes)
    return stdout, stderr, out_cut or err_cut


def _event_sort_key(event: dict[str, Any]) -> tuple[str, str]:
    return (
        str(event.get("timestamp") or ""),
        str(event.get("id") or ""),
    )


def _concat_events(
    events: list[dict[str, Any]],
    *,
    cap_bytes: int,
    truncated_by_pagination: bool,
) -> tuple[str, str, bool, int | None]:
    ordered = sorted(events, key=_event_sort_key)
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    exit_code: int | None = None
    for event in ordered:
        stdout_parts.append(event.get("stdout") or "")
        stderr_parts.append(event.get("stderr") or "")
        raw = event.get("exit_code")
        if raw is None:
            continue
        try:
            exit_code = int(raw)
        except (TypeError, ValueError):
            continue
    stdout, stderr, cut = bound_streams(
        "".join(stdout_parts),
        "".join(stderr_parts),
        cap_bytes=cap_bytes,
    )
    return stdout, stderr, cut or truncated_by_pagination, exit_code


def snapshot_from_bash_outputs(
    events: list[dict[str, Any]],
    *,
    truncated_by_pagination: bool = False,
    cap_bytes: int = SNAPSHOT_STREAM_CAP_BYTES,
    require_exit_code: bool = True,
) -> RunLogSnapshot | None:
    """Concat BashOutput chunks in timestamp order and bound each stream.

    Default: None until an exit_code exists. ``require_exit_code=False``
    keeps only still-running (null-exit) chunks.
    """
    if not events:
        return None

    kwargs = dict(cap_bytes=cap_bytes, truncated_by_pagination=truncated_by_pagination)
    if not require_exit_code:
        running = [event for event in events if event.get("exit_code") is None]
        if running:
            stdout, stderr, truncated, _ = _concat_events(running, **kwargs)
            if not stdout and not stderr:
                return None
            return RunLogSnapshot(
                exit_code=None,
                stdout=stdout,
                stderr=stderr,
                logs_truncated=truncated,
            )

    stdout, stderr, truncated, exit_code = _concat_events(events, **kwargs)
    if exit_code is None:
        return None
    return RunLogSnapshot(
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        logs_truncated=truncated,
    )


async def collect_bash_output_snapshot(
    client: httpx.AsyncClient,
    agent_url: str,
    session_key: str,
    command_id: str,
    *,
    require_exit_code: bool = True,
) -> RunLogSnapshot | None:
    """Paginate bash_events/search for command_id and build a snapshot."""
    items: list[dict[str, Any]] = []
    page_id: str | None = None
    truncated_by_pagination = False
    agent_url = agent_url.rstrip("/")

    for page_num in range(_MAX_PAGES):
        params: dict[str, str | int] = {
            "kind__eq": "BashOutput",
            "command_id__eq": command_id,
            "sort_order": "TIMESTAMP",
            "limit": _PAGE_LIMIT,
        }
        if page_id:
            params["page_id"] = page_id
        resp = await client.get(
            f"{agent_url}/api/bash/bash_events/search",
            params=params,
            headers={"X-Session-API-Key": session_key},
            timeout=30.0,
        )
        resp.raise_for_status()
        page = resp.json()
        batch = page.get("items") if isinstance(page, dict) else None
        if not isinstance(batch, list) or not batch:
            break
        items.extend(item for item in batch if isinstance(item, dict))
        page_id = page.get("next_page_id") if isinstance(page, dict) else None
        if not page_id:
            break
        if page_num == _MAX_PAGES - 1:
            truncated_by_pagination = True

    return snapshot_from_bash_outputs(
        items,
        truncated_by_pagination=truncated_by_pagination,
        require_exit_code=require_exit_code,
    )


def snapshot_already_written(run: AutomationRun) -> bool:
    """True when a non-null exit_code snapshot is already on the run row."""
    return run.exit_code is not None


def apply_run_log_snapshot(run: AutomationRun, snapshot: RunLogSnapshot) -> bool:
    """Copy snapshot onto run unless a non-null exit_code is already stored."""
    if snapshot_already_written(run):
        return False
    if snapshot.exit_code is not None:
        run.exit_code = snapshot.exit_code
    run.stdout = snapshot.stdout
    run.stderr = snapshot.stderr
    run.logs_truncated = snapshot.logs_truncated
    return True


def snapshot_with_fallback_exit_code(
    snapshot: RunLogSnapshot | None,
    fallback_exit_code: int,
) -> RunLogSnapshot | None:
    """Fill a missing bash exit_code from the callback. None stays None."""
    if snapshot is None:
        return None
    if snapshot.exit_code is None:
        return RunLogSnapshot(
            exit_code=fallback_exit_code,
            stdout=snapshot.stdout,
            stderr=snapshot.stderr,
            logs_truncated=snapshot.logs_truncated,
        )
    return snapshot


def snapshot_column_values(snapshot: RunLogSnapshot) -> dict[str, Any]:
    """ORM/UPDATE column mapping for a snapshot. Does not include secrets in logs."""
    return {
        "exit_code": snapshot.exit_code,
        "stdout": snapshot.stdout,
        "stderr": snapshot.stderr,
        "logs_truncated": snapshot.logs_truncated,
    }


def snapshot_values_for_run(
    run: AutomationRun, snapshot: RunLogSnapshot | None
) -> dict[str, Any]:
    """Columns to persist, empty if missing or a snapshot is already stored."""
    if snapshot is None or snapshot_already_written(run):
        return {}
    return snapshot_column_values(snapshot)


def verification_from_snapshot(run: AutomationRun) -> VerificationResult | None:
    """Build a verification result from a stored snapshot, if present."""
    if run.exit_code is None:
        return None
    success = run.exit_code == 0
    return VerificationResult(
        outcome=VerificationOutcome.COMPLETED
        if success
        else VerificationOutcome.FAILED,
        exit_code=run.exit_code,
        stdout=run.stdout or "",
        stderr=run.stderr or "",
        logs_truncated=bool(run.logs_truncated),
    )


async def fetch_run_log_snapshot_for_run(
    run: AutomationRun,
    *,
    require_exit_code: bool = True,
) -> RunLogSnapshot | None:
    """Best-effort snapshot from the run's agent-server. Failures are swallowed."""
    if not run.bash_command_id or snapshot_already_written(run):
        return None

    try:
        from openhands.automation.backends import get_backend
        from openhands.automation.config import get_config
        from openhands.automation.utils.sandbox import get_sandbox_agent_url

        backend = get_backend(run)
        async with httpx.AsyncClient(timeout=60.0) as client:
            if backend.is_local_mode:
                ctx = await backend.get_execution_context(client)
                return await collect_bash_output_snapshot(
                    client,
                    ctx.agent_url,
                    ctx.session_key,
                    run.bash_command_id,
                    require_exit_code=require_exit_code,
                )

            if not run.sandbox_id:
                return None

            api_key = await backend.get_api_key()
            result = await get_sandbox_agent_url(
                client,
                get_config().service.openhands_api_base_url,
                api_key,
                run.sandbox_id,
            )
            if result is None:
                return None
            agent_url, session_key = result
            return await collect_bash_output_snapshot(
                client,
                agent_url,
                session_key,
                run.bash_command_id,
                require_exit_code=require_exit_code,
            )
    except Exception as exc:
        logger.warning(
            "Could not snapshot run logs for run %s: %s",
            run.id,
            exc,
        )
        return None
