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
