"""
Child flow completion signaling.

**Rust Reference**:
`/home/richinex/Documents/devs/rust_projects/ergon/ergon/src/executor/execution.rs`
lines 34-194

This module provides automatic parent signaling when child flows complete.

**Python Documentation**:
- Logging: https://docs.python.org/3/library/logging.html
- pickle serialization: https://docs.python.org/3/library/pickle.html

From Dave Cheney: "Errors are values"
We don't raise exceptions on signaling failures - we log and return gracefully,
allowing the system to continue processing other flows.
"""

import logging
import pickle

from pyergon.core import InvocationStatus
from pyergon.executor.suspension_payload import SuspensionPayload
from pyergon.storage.base import ExecutionLog

logger = logging.getLogger(__name__)

__all__ = ["complete_child_flow"]


async def complete_child_flow(
    storage: ExecutionLog,
    flow_id: str,
    parent_metadata: tuple[str, str] | None,
    success: bool,
    error_msg: str | None = None,
) -> None:
    """
    Signal parent flow when child flow completes.

    **Rust Reference**: `src/executor/execution.rs` lines 34-194

    This is called by the worker when a child flow scheduled via invoke()
    finishes execution. It handles:
    - Reading the child's result from storage
    - Wrapping it in a SuspensionPayload (success/error)
    - Finding the parent's waiting step via signal token
    - Storing signal parameters
    - Resuming the parent flow

    This is an internal method used by Level 3 API automatic parent-child
    coordination. Users don't call this directly - they use invoke().result().

    **From Rust**:
    ```rust
    pub(super) async fn complete_child_flow<S: ExecutionLog>(
        storage: &Arc<S>,
        flow_id: Uuid,
        parent_metadata: Option<(Uuid, String)>,
        success: bool,
        error_msg: Option<&str>,
    )
    ```

    Args:
        storage: Storage backend
        flow_id: Child flow identifier
        parent_metadata: Optional (parent_id, signal_token) tuple
        success: Whether child succeeded
        error_msg: Error message if child failed

    Example:
        ```python
        # Called by Worker after child flow completes
        await complete_child_flow(
            storage=storage,
            flow_id=child_flow_id,
            parent_metadata=(parent_id, signal_token),
            success=True,
            error_msg=None
        )
        ```

    Note:
        This function logs errors but doesn't raise exceptions.
        Failures are logged and the function returns gracefully.
    """
    # Early return if no parent
    if parent_metadata is None:
        return

    parent_id, signal_token = parent_metadata

    # Prepare signal payload
    # From Rust lines 45-115
    if success:
        # Read the flow's result from storage (invocation step 0)
        try:
            invocation = await storage.get_invocation(flow_id, 0)
            if invocation is None:
                logger.error(f"No invocation found for successful flow {flow_id}")
                return

            result_bytes = invocation.return_value
            if result_bytes is None:
                logger.error(f"No result bytes for successful flow {flow_id}")
                return

            # result_bytes contains the actual result value already (pickled)
            # No need to unwrap Result like in Rust - Python doesn't have Result type
            payload = SuspensionPayload(
                success=True,
                data=result_bytes,
                is_retryable=None,  # Not applicable for success
            )

        except Exception as e:
            logger.error(f"Failed to get invocation for flow {flow_id}: {e}")
            return

    else:
        # Error case - use the error_msg which contains formatted error
        # Format: "type_name: message"
        error_msg = error_msg or "Unknown error"
        error_bytes = pickle.dumps(error_msg)

        # Read child's step 0 retryability flag
        is_retryable = None
        try:
            invocation = await storage.get_invocation(flow_id, 0)
            if invocation is not None:
                is_retryable = invocation.is_retryable
        except Exception as e:
            logger.warning(f"Failed to get retryability for flow {flow_id}: {e}")

        payload = SuspensionPayload(success=False, data=error_bytes, is_retryable=is_retryable)

    # Serialize signal payload
    # From Rust lines 117-127
    try:
        signal_bytes = pickle.dumps(payload)
    except Exception as e:
        logger.error(f"Failed to serialize signal payload for parent {parent_id}: {e}")
        return

    # Find parent's waiting step
    # From Rust lines 128-148
    # Now using get_invocations_for_flow() that we implemented
    waiting_step = None
    waiting_step_num = None

    try:
        invocations = await storage.get_invocations_for_flow(parent_id)

        for inv in invocations:
            if inv.status == InvocationStatus.WAITING_FOR_SIGNAL and inv.timer_name == signal_token:
                waiting_step = inv
                waiting_step_num = inv.step
                break

        if waiting_step is None:
            logger.debug(
                f"No waiting step found for parent flow {parent_id} with token {signal_token}"
            )
            return

    except Exception as e:
        logger.error(f"Failed to get invocations for parent flow {parent_id}: {e}")
        return

    # CRITICAL: Store suspension result - must succeed for parent to resume
    # From Rust lines 152-166
    try:
        await storage.store_suspension_result(
            parent_id, waiting_step_num, signal_token, signal_bytes
        )
    except Exception as e:
        logger.error(
            f"CRITICAL: Failed to store suspension result for parent flow {parent_id}: {e}"
        )
        return

    # Resume parent flow
    # From Rust lines 168-191
    # Now using resume_flow() that we implemented
    try:
        resumed = await storage.resume_flow(parent_id)

        if resumed:
            status = "success" if success else "error"
            error_suffix = f": {error_msg}" if error_msg else ""
            logger.debug(
                f"Auto-signaled and resumed parent flow {parent_id} with token {signal_token} "
                f"({status}{error_suffix})"
            )
        else:
            logger.debug(f"Parent flow {parent_id} not resumed (not in SUSPENDED state)")

    except Exception as e:
        logger.error(f"Failed to resume parent flow {parent_id}: {e}")
