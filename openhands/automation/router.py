"""FastAPI router for the automations CRUD API."""

import asyncio
import logging
import re
import uuid
from typing import Any

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.responses import RedirectResponse
from sqlalchemy import func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from openhands.automation.auth import AuthenticatedUser, require_permission
from openhands.automation.db import get_session
from openhands.automation.git_sync import mark_git_sync_dirty
from openhands.automation.models import (
    Automation,
    AutomationDisableEvent,
    AutomationRun,
    AutomationRunStatus,
    TarballUpload,
)
from openhands.automation.preset_router import regenerate_preset_prompt_tarball
from openhands.automation.schemas import (
    AutomationListResponse,
    AutomationResponse,
    AutomationRunListResponse,
    AutomationRunLogsResponse,
    AutomationRunResponse,
    CreateAutomationRequest,
    RunCompleteRequest,
    RunPhaseRequest,
    UpdateAutomationRequest,
)
from openhands.automation.storage import FileStore, get_file_store
from openhands.automation.telemetry import (
    capture_automation_event,
    get_request_telemetry_context,
)
from openhands.automation.utils import utcnow
from openhands.automation.utils.api_key import (
    APIKeyError,
    get_api_key_for_automation_run,
)
from openhands.automation.utils.callback_error import format_callback_error
from openhands.automation.utils.conversation_outcome import (
    fetch_latest_finish_tool_response_for_run,
)
from openhands.automation.utils.model_profiles import resolve_model_profile_for_user
from openhands.automation.utils.run import (
    create_pending_run,
    record_first_run_outcome,
    skip_pending_runs_for_disabled_automation,
)
from openhands.automation.utils.run_logs import (
    fetch_run_log_snapshot_for_run,
    snapshot_values_for_run,
    snapshot_with_fallback_exit_code,
)
from openhands.automation.utils.run_status_detail import (
    run_status_detail_from_callback_error,
)
from openhands.automation.utils.sandbox import cleanup_sandbox
from openhands.automation.utils.tarball_validation import (
    is_http_url,
    parse_internal_upload_id,
    validate_tarball_path,
)
from openhands.automation.utils.templates import (
    TEMPLATE_EXISTS_RESPONSE,
    find_existing_template_automation,
)
from openhands.automation.utils.timeout import default_automation_timeout
from openhands.automation.utils.unhealthy import maybe_disable_unhealthy_automation


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["Automations"])

_require_manage_automations = require_permission("manage_automations")


# --- CRUD ---


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=TEMPLATE_EXISTS_RESPONSE
)
async def create_automation(
    body: CreateAutomationRequest,
    request: Request,
    response: Response,
    user: AuthenticatedUser = Depends(_require_manage_automations),
    session: AsyncSession = Depends(get_session),
) -> AutomationResponse:
    """Create a new automation.

    The tarball_path can be either:
    - Internal upload: oh-internal://uploads/{uuid} (from /v1/uploads)
    - External public URL: https://, s3://, or gs:// URLs

    An entry shipping its own tarball creates here rather than through a
    preset, so it may carry the same ``template`` provenance those accept.
    """
    # Enabling the same template twice returns the existing automation rather
    # than a duplicate. Before tarball validation, so a repeat enable costs one
    # query and leaves the new upload unreferenced rather than adopting it.
    if body.template is not None:
        existing = await find_existing_template_automation(
            session, user.org_id, body.template.id
        )
        if existing is not None:
            response.status_code = status.HTTP_200_OK
            return AutomationResponse.model_validate(existing)

    # Validate tarball_path (checks ownership for internal uploads)
    await validate_tarball_path(
        tarball_path=body.tarball_path,
        user_id=user.user_id,
        org_id=user.org_id,
        session=session,
    )
    model = resolve_model_profile_for_user(body.model, user)

    preset_metadata: dict[str, Any] | None = None
    if body.template is not None:
        preset_metadata = {"template": body.template.model_dump(exclude_none=True)}

    auto = Automation(
        user_id=user.user_id,
        org_id=user.org_id,
        name=body.name,
        model=model,
        preset_metadata=preset_metadata,
        trigger=body.trigger.model_dump(),
        tarball_path=body.tarball_path,
        setup_script_path=body.setup_script_path,
        entrypoint=body.entrypoint,
        timeout=default_automation_timeout(body.timeout),
        keep_alive=body.keep_alive,
        telemetry_distinct_id=get_request_telemetry_context(
            request
        ).frontend_distinct_id,
    )
    session.add(auto)
    await session.flush()
    await session.refresh(auto)
    await mark_git_sync_dirty(session, auto)
    creation_properties: dict[str, Any] = {"creation_path": "raw"}
    if body.template is not None:
        creation_properties["template_id"] = body.template.id
        creation_properties["template_version"] = body.template.version
    await capture_automation_event(
        "automation_created",
        request=request,
        user=user,
        automation=auto,
        properties=creation_properties,
    )
    return AutomationResponse.model_validate(auto)
