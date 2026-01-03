"""
Flow execution outcome handling.

**Rust Reference**:
`/home/richinex/Documents/devs/rust_projects/ergon/ergon/src/executor/execution.rs`

This module hides the design decisions around:
- Retry policies and when to retry failed flows
- Parent signaling when child flows complete (Level 3 API)
- Suspension handling for timer/signal workflows
- Error handling and flow completion

**From Parnas's Principle**: These decisions can change independently
of the worker loop implementation.

**Python Documentation**:
- Logging: https://docs.python.org/3/library/logging.html
- Timedelta: https://docs.python.org/3/library/datetime.html
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
    """
    Handle successful flow completion.

    **Rust Reference**: `src/executor/execution.rs` lines 349-370

    This function:
    1. Signals parent flow if this is a child (Level 3 API)
    2. Marks task as COMPLETE in queue

    Args:
        storage: Storage backend
        worker_id: Worker identifier
        flow_task_id: Task queue ID
        flow_id: Flow execution ID
        parent_metadata: Optional (parent_id, signal_token) for child flows

    Example:
        ```python
        await handle_flow_completion(
            storage=storage,
            worker_id="worker-1",
            flow_task_id=task_id,
            flow_id=flow_id,
            parent_metadata=(parent_id, signal_token)
        )
        ```
    """
    logger.info(f"Worker {worker_id} completed flow: task_id={flow_task_id}")

    # Complete child flow (Level 3 API) - signals parent after successful completion
    # From Rust line 362
    await complete_child_flow(
        storage=storage,
        flow_id=flow_id,
        parent_metadata=parent_metadata,
        success=True,
        error_msg=None,
    )

    # Mark task as complete in queue
    # From Rust lines 364-369
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
    """
    Handle failed flow, checking retry policy and signaling parent if needed.

    **Rust Reference**: `src/executor/execution.rs` lines 373-466

    This function:
    1. Marks non-retryable errors in storage
    2. Checks retry policy
    3. If should retry: Schedule retry with backoff
    4. If not: Signal parent (if child) and mark as FAILED

    Args:
        storage: Storage backend
        worker_id: Worker identifier
        flow: Scheduled flow details
        flow_task_id: Task queue ID
        error: The exception that occurred
        parent_metadata: Optional (parent_id, signal_token) for child flows

    Example:
        ```python
        await handle_flow_error(
            storage=storage,
            worker_id="worker-1",
            flow=scheduled_flow,
            flow_task_id=task_id,
            error=ValueError("Processing failed"),
            parent_metadata=None
        )
        ```
    """
    error_msg = str(error)

    logger.error(f"Worker {worker_id} flow failed: task_id={flow_task_id}, error={error_msg}")

    # Mark non-retryable errors in storage
    # From Rust lines 388-397
    # Check if error has is_retryable attribute (RetryableError protocol)
    if hasattr(error, "is_retryable") and not error.is_retryable():
        try:
            await storage.update_is_retryable(flow.flow_id, 0, False)
        except Exception as e:
            logger.warning(f"Worker {worker_id} failed to mark error as non-retryable: {e}")

    # Check retry policy
    # From Rust lines 399-466
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

        # Complete child flow (Level 3 API) - signals parent after retries exhausted
        # From Rust lines 424-432
        await complete_child_flow(
            storage=storage,
            flow_id=flow.flow_id,
            parent_metadata=parent_metadata,
            success=False,
            error_msg=error_msg,
        )

        # Mark task as failed
        # From Rust lines 434-439
        try:
            await storage.complete_flow(flow_task_id, TaskStatus.FAILED, error_msg)
        except Exception as e:
            logger.error(f"Worker {worker_id} failed to mark flow failed: {e}")


async def handle_suspended_flow(
    storage: ExecutionLog, worker_id: str, flow_task_id: str, flow_id: str, reason: SuspendReason
) -> None:
    """
    Handle flow suspension for timer or signal.

    **Rust Reference**: `src/executor/execution.rs` lines 270-347

    This function:
    1. Marks task as SUSPENDED in queue
    2. Checks for race condition where signal/timer arrived while suspending
    3. Resumes flow immediately if result already available

    Args:
        storage: Storage backend
        worker_id: Worker identifier
        flow_task_id: Task queue ID
        flow_id: Flow execution ID
        reason: Why the flow suspended (timer or signal)

    Example:
        ```python
        await handle_suspended_flow(
            storage=storage,
            worker_id="worker-1",
            flow_task_id=task_id,
            flow_id=flow_id,
            reason=SuspendReason(flow_id=flow_id, step=5, signal_name="payment")
        )
        ```
    """
    logger.info(f"Worker {worker_id} flow suspended: task_id={flow_task_id}, reason={reason}")

    # Mark flow as SUSPENDED so resume_flow() can re-enqueue it
    # From Rust lines 282-288
    try:
        await storage.complete_flow(flow_task_id, TaskStatus.SUSPENDED, None)
    except Exception as e:
        logger.error(f"Worker {worker_id} failed to mark flow suspended: {e}")

    # Check if signal or timer result arrived while we were still RUNNING
    # From Rust lines 290-347
    # This handles a race condition with multiple workers:
    # - Worker A: Parent suspends (RUNNING)
    # - Worker B: Child completes/timer fires, calls resume_flow()
    #   → returns False (parent still RUNNING)
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
    """
    Check if a flow should be retried based on retry policy.

    **Rust Reference**: `src/executor/execution.rs` lines 195-268

    Gets retry policy from step 0's invocation (NOT from ScheduledFlow).
    This matches Rust which stores retry_policy in Invocation, not ScheduledFlow.

    Returns:
        timedelta for retry delay if should retry, None if should not retry

    Example:
        ```python
        delay = await check_should_retry(storage, flow)
        if delay is not None:
            await storage.retry_flow(task_id, error_msg, delay)
        else:
            await storage.complete_flow(task_id, TaskStatus.FAILED, error_msg)
        ```
    """
    # First check if any step has a non-retryable error
    # From Rust lines 206-229
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

    # Get the flow's invocation (step 0) to check retry policy
    # From Rust lines 231-267
    try:
        invocation = await storage.get_invocation(flow.flow_id, 0)
    except Exception as e:
        logger.error(f"Failed to get invocation: {e}")
        return None

    if invocation is None:
        logger.warning(f"No invocation found for flow {flow.flow_id}")
        return None

    # Get retry policy (explicit or default)
    # From Rust lines 234-243
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
    # From Rust lines 217-220: if attempt >= max_attempts, no more retries
    if next_attempt >= policy.max_attempts:
        logger.debug(
            f"Flow {flow.flow_id} exceeded max retry attempts "
            f"({next_attempt}/{policy.max_attempts})"
        )
        return None

    # Calculate exponential backoff delay
    # From Rust lines 247-252 (via policy.delay_for_attempt)
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
