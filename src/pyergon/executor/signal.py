"""External signal handling for flow coordination.

Provides:
- Wait/resume mechanism for external events
- Global state management for waiting flows
- Support for pausing flows until external events occur

Design: Information Hiding (Parnas)
Encapsulates decisions about signal coordination across flow
executions. Uses asyncio.Event for async notification and
global dictionaries to track waiting flows and resume parameters.
"""

import asyncio
import pickle
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from pyergon.core import CallType, get_current_context
from pyergon.models import InvocationStatus

# Global state for signal coordination
# Module-level to ensure visibility across all flow executions
_wait_notifiers: dict[str, asyncio.Event] = {}
_resume_params: dict[str, bytes] = {}

# Lock for thread-safe access to dictionaries
_lock = asyncio.Lock()

R = TypeVar("R")


async def await_external_signal(signal_name: str) -> bytes:
    """Await an external signal by name.

    Suspends the flow until a signal with the given name arrives.
    Returns the raw signal data as bytes (caller must deserialize).

    Args:
        signal_name: Name of the signal to wait for

    Returns:
        Signal data bytes

    Raises:
        RuntimeError: If called outside execution context

    Example:
        ```python
        data_bytes = await await_external_signal("user_approval")
        approval: ApprovalData = pickle.loads(data_bytes)
        ```
    """
    ctx = get_current_context()
    if ctx is None:
        raise RuntimeError("await_external_signal called outside execution context")

    # Get current step number
    current_step = ctx.get_enclosing_step()
    if current_step is None:
        raise RuntimeError("await_external_signal called but no enclosing step set")

    # Check if signal already arrived (we're resuming)
    existing_inv = await ctx.storage.get_invocation(ctx.flow_id, current_step)

    if existing_inv and existing_inv.status == InvocationStatus.COMPLETE:
        # Signal already received - return cached result
        if existing_inv.return_value:
            return existing_inv.return_value
        raise RuntimeError("Signal step completed but no return value found")

    if existing_inv and existing_inv.status == InvocationStatus.WAITING_FOR_SIGNAL:
        # Check if signal has arrived while we were waiting
        params = await ctx.storage.get_suspension_result(ctx.flow_id, current_step, signal_name)
        if params:
            # Signal arrived! Complete the step and return
            await ctx.storage.log_invocation_completion(ctx.flow_id, current_step, params)
            await ctx.storage.remove_suspension_result(ctx.flow_id, current_step, signal_name)
            return params

    # First time - mark as waiting for signal
    await ctx.storage.log_signal(ctx.flow_id, current_step, signal_name)

    # Suspend execution
    from pyergon.executor.outcome import SuspendReason, _SuspendExecution

    reason = SuspendReason(flow_id=ctx.flow_id, step=current_step, signal_name=signal_name)
    ctx.set_suspend_reason(reason)
    raise _SuspendExecution()


async def await_external_signal_callable(step_callable: Callable[[], Awaitable[R | None]]) -> R:
    """Await an external signal before continuing flow execution.

    Used when a step needs to wait for an external event (email confirmation,
    payment webhook, manual approval) before continuing.

    Process:
    1. Logs the step as WAITING_FOR_SIGNAL in the database
    2. Pauses execution until signal_resume() is called
    3. Returns the value provided in the resume signal

    Args:
        step_callable: Callable that returns Optional[R]
            - Returns None in Await mode (first execution)
            - Returns R in Resume mode (after signal_resume)

    Returns:
        Value provided by signal_resume()

    Raises:
        RuntimeError: If called outside execution context
        RuntimeError: If no resume parameters found
        RuntimeError: If step returns None in Resume mode

    Example:
        ```python
        @flow
        class SignupFlow:
            @step
            async def confirm_email(self, timestamp: datetime) -> Optional[datetime]:
                call_type = get_current_call_type()
                if call_type == CallType.AWAIT:
                    return None
                elif call_type == CallType.RESUME:
                    return datetime.now()
                else:
                    return timestamp

            async def run(self) -> str:
                await self.send_confirmation_email()

                # Flow pauses here until user clicks confirmation link
                confirmed_at = await await_external_signal(
                    lambda: self.confirm_email(datetime.now())
                )

                await self.activate_account()
                return f"Account activated at {confirmed_at}"
        ```
    """
    ctx = get_current_context()
    if ctx is None:
        raise RuntimeError(
            "BUG: await_external_signal called outside execution context - "
            "this is a framework error"
        )

    # Get the current step number (peek without incrementing)
    current_step = ctx.current_step()

    # Check if this step is already waiting for a signal
    existing_inv = await ctx.storage.get_invocation(flow_id=ctx.flow_id, step=current_step)

    if existing_inv and existing_inv.status == InvocationStatus.WAITING_FOR_SIGNAL:
        # We're resuming - execute the step in Resume mode
        from pyergon.core.context import CALL_TYPE

        token = CALL_TYPE.set(CallType.RESUME)
        try:
            result = await step_callable()
            if result is None:
                raise RuntimeError(
                    "BUG: Step returned None in Resume mode - step implementation error"
                )
            return result
        finally:
            CALL_TYPE.reset(token)

    # First time calling this await - set up waiting state
    from pyergon.core.context import CALL_TYPE

    token = CALL_TYPE.set(CallType.AWAIT)
    try:
        # Execute the step - it should return None in Await mode
        result = await step_callable()

        if result is not None:
            # Step completed immediately (no wait needed)
            return result

        # Step is awaiting - wait for external signal
        return await _await_signal(ctx.flow_id)
    finally:
        CALL_TYPE.reset(token)


async def _await_signal(flow_id: str) -> Any:
    """Wait for external signal to resume flow execution.

    Internal function that blocks until a signal is received
    via signal_resume.

    Args:
        flow_id: The flow ID to wait for

    Returns:
        Deserialized parameter value from signal_resume

    Raises:
        RuntimeError: If no resume parameters found
    """
    # Get or create notifier
    async with _lock:
        if flow_id not in _wait_notifiers:
            _wait_notifiers[flow_id] = asyncio.Event()
        notifier = _wait_notifiers[flow_id]

    # Wait for notification
    await notifier.wait()

    # Retrieve resume parameters
    async with _lock:
        params_bytes = _resume_params.pop(flow_id, None)
        # Clean up notifier
        _wait_notifiers.pop(flow_id, None)

    if params_bytes is None:
        raise RuntimeError("No resume parameters found")

    # Deserialize and return
    result = pickle.loads(params_bytes)
    return result


def signal_resume(flow_id: str, params: Any) -> None:
    """Resume a flow that is waiting for an external signal.

    Called from outside the flow (webhook handler, background task,
    or manual admin action) to provide the awaited value and resume
    execution.

    Args:
        flow_id: The flow ID to resume
        params: The value to return from await_external_signal

    Example:
        ```python
        @app.post("/confirm-email")
        async def confirm_email_webhook(flow_id: str):
            confirmed_at = datetime.now()

            # Resume the waiting flow
            signal_resume(flow_id, confirmed_at)

            # Re-execute the flow (resumes from waiting step)
            storage = get_storage()
            scheduler = Scheduler(storage)
            await scheduler.schedule_resume(flow_id)

            return {"status": "confirmed"}
        ```
    """
    # Serialize parameters
    params_bytes = pickle.dumps(params)

    # Store params and notify (synchronously schedule on event loop)
    loop = asyncio.get_event_loop()
    loop.create_task(_signal_resume_async(flow_id, params_bytes))


async def _signal_resume_async(flow_id: str, params_bytes: bytes) -> None:
    """Internal async helper for signal_resume.

    Allows signal_resume to be called from sync contexts while
    still properly coordinating with async locks.

    Args:
        flow_id: The flow ID to resume
        params_bytes: Serialized parameters
    """
    async with _lock:
        # Store resume parameters
        _resume_params[flow_id] = params_bytes

        # Get or create notifier and wake it up
        if flow_id not in _wait_notifiers:
            _wait_notifiers[flow_id] = asyncio.Event()
        notifier = _wait_notifiers[flow_id]
        notifier.set()


# Public exports
__all__ = [
    "await_external_signal",
    "signal_resume",
]
