"""Child flow completion signaling.

Provides automatic parent notification when child flows complete.
When a child flow finishes, this module signals the parent flow
to resume execution and receive the result.

Design: Error Handling
Signaling failures are logged but don't raise exceptions, allowing
the system to continue processing other flows gracefully.
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
    """Signal parent flow when child flow completes.

    Called by the worker when a child flow scheduled via invoke()
    finishes execution. Handles reading the child's result, packaging
    it as a signal payload, and resuming the parent flow.

    Internal method for automatic parent-child coordination. Users
    interact with this indirectly via invoke().result().

    Args:
        storage: Storage backend for persistence
        flow_id: Child flow identifier
        parent_metadata: Parent flow ID and signal token for resumption
        success: Whether child completed successfully
        error_msg: Error message if child failed

    Example:
        ```python
        await complete_child_flow(
            storage, child_flow_id,
            parent_metadata=(parent_id, signal_token),
            success=True
        )
        ```

    Note:
        Errors are logged but not raised, allowing graceful degradation.
    """
    # Early return if no parent
    if parent_metadata is None:
        return

    parent_id, signal_token = parent_metadata

    # Prepare signal payload based on child flow outcome
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

            # Result bytes contain pickled return value
            payload = SuspensionPayload(
                success=True,
                data=result_bytes,
                is_retryable=None,  # Not applicable for success
            )

        except Exception as e:
            logger.error(f"Failed to get invocation for flow {flow_id}: {e}")
            return

    else:
        # Error case - format: "type_name: message"
        error_msg = error_msg or "Unknown error"
        error_bytes = pickle.dumps(error_msg)

        # Read child's retryability flag from invocation
        is_retryable = None
        try:
            invocation = await storage.get_invocation(flow_id, 0)
            if invocation is not None:
                is_retryable = invocation.is_retryable
        except Exception as e:
            logger.warning(f"Failed to get retryability for flow {flow_id}: {e}")

        payload = SuspensionPayload(success=False, data=error_bytes, is_retryable=is_retryable)

    # Serialize signal payload for storage
    try:
        signal_bytes = pickle.dumps(payload)
    except Exception as e:
        logger.error(f"Failed to serialize signal payload for parent {parent_id}: {e}")
        return

    # Find parent's waiting step by matching signal token
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

    # Store suspension result - required for parent flow resumption
    try:
        await storage.store_suspension_result(
            parent_id, waiting_step_num, signal_token, signal_bytes
        )
    except Exception as e:
        logger.error(
            f"CRITICAL: Failed to store suspension result for parent flow {parent_id}: {e}"
        )
        return

    # Resume parent flow execution
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
