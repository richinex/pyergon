"""Flow execution outcome handling.

Handles three flow completion paths:
- Successful completion: signal parent, mark complete
- Retryable error: schedule retry with exponential backoff
- Non-retryable error: signal parent, mark failed

Design: Information Hiding (Parnas)
Retry and signaling logic is isolated here, allowing worker
loop to remain simple and these policies to evolve independently.
"""

import logging
from datetime import timedelta

from pyergon.core import InvocationStatus, ScheduledFlow, TaskStatus
from pyergon.executor.child_completion import complete_child_flow
from pyergon.executor.outcome import SuspendReason
from pyergon.storage.base import ExecutionLog

logger = logging.getLogger(__name__)

__all__ = [
    "handle_flow_completion",
    "handle_flow_error",
    "handle_suspended_flow",
    "check_should_retry",
]


async def handle_flow_completion(
    storage: ExecutionLog,
    worker_id: str,
    flow_task_id: str,
    flow_id: str,
    parent_metadata: tuple[str, str] | None,
) -> None:
    """Handle successful flow completion.

    Signals parent flow if this is a child, then marks task
    as complete in the work queue.

    Args:
        storage: Storage backend for persistence
        worker_id: Identifier of worker completing the flow
        flow_task_id: Task identifier in work queue
        flow_id: Flow execution identifier
        parent_metadata: Parent flow ID and signal token for child flows

    Example:
        ```python
        await handle_flow_completion(
            storage, "worker-1", task_id, flow_id,
            parent_metadata=(parent_id, signal_token)
        )
        ```
    """
    logger.info(f"Worker {worker_id} completed flow: task_id={flow_task_id}")

    # Signal parent flow if this is a child flow
    await complete_child_flow(
        storage=storage,
        flow_id=flow_id,
        parent_metadata=parent_metadata,
        success=True,
        error_msg=None,
    )

    # Mark task as complete in work queue
    try:
        await storage.complete_flow(flow_task_id, TaskStatus.COMPLETE, None)
    except Exception as e:
        logger.error(f"Worker {worker_id} failed to mark flow complete: {e}")


async def handle_flow_error(
    storage: ExecutionLog,
    worker_id: str,
    flow: ScheduledFlow,
    flow_task_id: str,
    error: Exception,
    parent_metadata: tuple[str, str] | None,
) -> None:
    """Handle failed flow with retry policy and parent signaling.

    Marks non-retryable errors, checks retry policy, and either
    schedules a retry with exponential backoff or signals parent
    and marks the flow as failed.

    Args:
        storage: Storage backend for persistence
        worker_id: Identifier of worker handling the error
        flow: Scheduled flow details
        flow_task_id: Task identifier in work queue
        error: The exception that occurred
        parent_metadata: Parent flow ID and signal token for child flows

    Example:
        ```python
        await handle_flow_error(
            storage, "worker-1", scheduled_flow, task_id,
            ValueError("Processing failed"),
            parent_metadata=None
        )
        ```
    """
    error_msg = str(error)

    logger.error(f"Worker {worker_id} flow failed: task_id={flow_task_id}, error={error_msg}")

    # Mark non-retryable errors in storage
    if hasattr(error, "is_retryable") and not error.is_retryable():
        try:
            await storage.update_is_retryable(flow.flow_id, 0, False)
        except Exception as e:
            logger.warning(f"Worker {worker_id} failed to mark error as non-retryable: {e}")

    # Check retry policy and decide whether to retry
    retry_decision = await check_should_retry(storage, flow)

    if retry_decision is not None:
        # Should retry
        delay = retry_decision
        logger.info(
            f"Worker {worker_id} retrying flow: task_id={flow_task_id}, "
            f"attempt={flow.retry_count + 1}, delay={delay}"
        )
        try:
            await storage.retry_flow(flow_task_id, error_msg, delay)
        except Exception as e:
            logger.error(f"Worker {worker_id} failed to schedule retry: {e}")

    else:
        # Should not retry - signal parent and mark as failed
        logger.debug(
            f"Worker {worker_id} not retrying flow: task_id={flow_task_id} "
            f"(not retryable or max attempts reached)"
        )

        # Signal parent flow after retries exhausted
        await complete_child_flow(
            storage=storage,
            flow_id=flow.flow_id,
            parent_metadata=parent_metadata,
            success=False,
            error_msg=error_msg,
        )

        # Mark task as failed in work queue
        try:
            await storage.complete_flow(flow_task_id, TaskStatus.FAILED, error_msg)
        except Exception as e:
            logger.error(f"Worker {worker_id} failed to mark flow failed: {e}")


async def handle_suspended_flow(
    storage: ExecutionLog, worker_id: str, flow_task_id: str, flow_id: str, reason: SuspendReason
) -> None:
    """Handle flow suspension for timer or signal.

    Marks task as suspended in queue, then checks for race condition
    where signal/timer arrived while suspending. Resumes flow
    immediately if result is already available.

    Args:
        storage: Storage backend for persistence
        worker_id: Identifier of worker handling suspension
        flow_task_id: Task identifier in work queue
        flow_id: Flow execution identifier
        reason: Suspension reason (timer or signal)

    Example:
        ```python
        await handle_suspended_flow(
            storage, "worker-1", task_id, flow_id,
            reason=SuspendReason(flow_id=flow_id, step=5, signal_name="payment")
        )
        ```
    """
    logger.info(f"Worker {worker_id} flow suspended: task_id={flow_task_id}, reason={reason}")

    # Mark flow as SUSPENDED so resume_flow() can re-enqueue it
    try:
        await storage.complete_flow(flow_task_id, TaskStatus.SUSPENDED, None)
    except Exception as e:
        logger.error(f"Worker {worker_id} failed to mark flow suspended: {e}")

    # Check for race condition: signal/timer arrived while suspending
    # Race scenario:
    # - Worker A: Parent suspends (RUNNING)
    # - Worker B: Child completes/timer fires, calls resume_flow() returns False (still RUNNING)
    # - Worker A: Marks parent SUSPENDED
    # - Need to check for pending signals/timers and resume immediately
    try:
        invocations = await storage.get_invocations_for_flow(flow_id)

        for inv in invocations:
            should_resume = False

            if inv.status == InvocationStatus.WAITING_FOR_SIGNAL:
                # Check if signal arrived
                signal_result = await storage.get_suspension_result(
                    flow_id, inv.step, inv.timer_name or ""
                )
                should_resume = signal_result is not None

            elif inv.status == InvocationStatus.WAITING_FOR_TIMER:
                # Check if timer fired and result is cached
                timer_result = await storage.get_suspension_result(
                    flow_id, inv.step, inv.timer_name or ""
                )
                should_resume = timer_result is not None

            if should_resume:
                logger.debug(
                    f"Worker {worker_id} resuming flow {flow_id} - result arrived during suspension"
                )
                await storage.resume_flow(flow_id)
                break

    except Exception as e:
        logger.warning(f"Worker {worker_id} failed to check for pending results: {e}")


async def check_should_retry(storage: ExecutionLog, flow: ScheduledFlow) -> timedelta | None:
    """Check if a flow should be retried based on retry policy.

    Gets retry policy from step 0's invocation and calculates
    exponential backoff delay for the next retry attempt.

    Returns:
        Retry delay timedelta if should retry, None if should not retry

    Example:
        ```python
        delay = await check_should_retry(storage, flow)
        if delay is not None:
            await storage.retry_flow(task_id, error_msg, delay)
        else:
            await storage.complete_flow(task_id, TaskStatus.FAILED, error_msg)
        ```
    """
    # Check if any step has a non-retryable error
    try:
        has_non_retryable = await storage.has_non_retryable_error(flow.flow_id)
        if has_non_retryable:
            logger.info(f"Flow {flow.flow_id} has a non-retryable error - will not retry")
            return None
        else:
            logger.debug(
                f"Flow {flow.flow_id} has no non-retryable errors - "
                "continuing to retry policy check"
            )
    except Exception as e:
        logger.warning(f"Failed to check for non-retryable errors for flow {flow.flow_id}: {e}")
        # Continue anyway

    # Get flow invocation to check retry policy
    try:
        invocation = await storage.get_invocation(flow.flow_id, 0)
    except Exception as e:
        logger.error(f"Failed to get invocation: {e}")
        return None

    if invocation is None:
        logger.warning(f"No invocation found for flow {flow.flow_id}")
        return None

    # Get retry policy from invocation (explicit or default)
    policy = invocation.retry_policy
    if policy is None:
        # No explicit policy, but error is retryable (we passed has_non_retryable_error check)
        # Use a default policy for retryable errors
        logger.debug(
            f"Flow {flow.flow_id} has no explicit retry policy but error is retryable, "
            f"using default: RetryPolicy.STANDARD (3 attempts)"
        )
        from pyergon.core import RetryPolicy

        policy = RetryPolicy.STANDARD

    next_attempt = flow.retry_count + 1

    # Check if we've exceeded max attempts
    if next_attempt >= policy.max_attempts:
        logger.debug(
            f"Flow {flow.flow_id} exceeded max retry attempts "
            f"({next_attempt}/{policy.max_attempts})"
        )
        return None

    # Calculate exponential backoff delay
    base_delay = policy.initial_delay_ms / 1000.0  # Convert to seconds
    multiplier = policy.backoff_multiplier
    max_delay = policy.max_delay_ms / 1000.0  # Convert to seconds

    # Exponential backoff: delay = base * multiplier^(attempt-1)
    # attempt is 1-indexed: attempt 1 gets base_delay, attempt 2 gets base*multiplier, etc.
    delay_seconds = base_delay * (multiplier ** (next_attempt - 1))

    # Cap at max_delay
    delay_seconds = min(delay_seconds, max_delay)

    logger.debug(
        f"Flow {flow.flow_id} will retry "
        f"(attempt {next_attempt}/{policy.max_attempts}) "
        f"after {delay_seconds:.2f}s"
    )

    return timedelta(seconds=delay_seconds)
