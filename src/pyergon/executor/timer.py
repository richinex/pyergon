"""Durable timer scheduling for flows.

Design: Observer Pattern
Timers notify waiting flows when they fire. Timers are durable
and survive process restarts. All state is persisted in storage,
allowing workers to fire timers even if the original process crashed.

Distributed Implementation:
Timers work across multiple processes and machines because:
1. Timer state is persisted in storage (database)
2. Workers poll storage for expired timers
3. Workers atomically claim timers to prevent duplicate firing
4. Timer results are stored in suspension_params table
5. Flows check storage for timer results on resume

No in-memory synchronization needed. All coordination happens
through the database.
"""

from datetime import datetime, timedelta

from pyergon.core import get_current_context as get_execution_context


def get_current_context():
    """Get the current execution context for timer scheduling.

    Raises:
        TimerError: If no context set (called outside flow)
    """
    ctx = get_execution_context()

    if ctx is None:
        raise TimerError(
            "No execution context set. schedule_timer() must be called within a flow step."
        )

    return ctx


async def schedule_timer(duration: float) -> None:
    """Schedule a durable timer within a flow step.

    Worker polls storage for expired timers and fires them automatically.

    Args:
        duration: Timer duration in seconds

    Raises:
        TimerError: If called outside a flow context

    Example:
        ```python
        @step
        async def wait_for_fraud_check(self):
            await schedule_timer(2.0)
            print("Fraud check complete")
        ```
    """
    await schedule_timer_named(duration, name="")


async def schedule_timer_named(duration: float, name: str) -> None:
    """Schedule a named durable timer for debugging.

    Suspends the flow until the timer fires. The timer is persisted
    to storage and will fire even if the worker crashes.

    Args:
        duration: Timer duration in seconds
        name: Timer name for debugging/logging

    Raises:
        TimerError: If called outside a flow context
        _SuspendExecution: Internal exception to signal flow suspension

    Example:
        ```python
        @step
        async def wait_for_warehouse(self):
            await schedule_timer_named(3.0, "warehouse-processing")
            print("Warehouse processing complete")
        ```
    """
    from pyergon.executor.outcome import SuspendReason, _SuspendExecution
    from pyergon.models import InvocationStatus

    # Get current flow context
    ctx = get_current_context()

    # Get the step number from enclosing step
    current_step = ctx.get_enclosing_step()
    if current_step is None:
        raise TimerError("schedule_timer called but no enclosing step set")

    # Check if we're resuming from a timer
    existing_inv = await ctx.storage.get_invocation(ctx.flow_id, current_step)

    if existing_inv is not None:
        if existing_inv.status == InvocationStatus.COMPLETE:
            # Timer already fired and step completed - resuming
            return

        if existing_inv.status == InvocationStatus.WAITING_FOR_TIMER:
            # Check if timer has fired and has a cached result
            timer_key = name if name else ""

            result_bytes = await ctx.storage.get_suspension_result(
                ctx.flow_id, current_step, timer_key
            )

            if result_bytes is not None:
                # Timer fired - clean up suspension result
                await ctx.storage.remove_suspension_result(ctx.flow_id, current_step, timer_key)

                # Deserialize the SuspensionPayload to check success
                import pickle

                try:
                    payload = pickle.loads(result_bytes)
                    if payload.get("success", False):
                        # Timer fired successfully
                        return
                    else:
                        # Timer marked as failed (shouldn't happen, but handle gracefully)
                        raise TimerError("Timer marked as failed")
                except Exception as e:
                    raise TimerError(f"Failed to deserialize timer payload: {e}")

            # Timer not fired yet - suspend the flow
            reason = SuspendReason(
                flow_id=ctx.flow_id,
                step=current_step,
                signal_name=None,
            )
            ctx.set_suspend_reason(reason)

            # Raise exception to signal suspension
            raise _SuspendExecution()

    # First time - calculate fire time and log timer
    fire_at = datetime.now() + timedelta(seconds=duration)

    await ctx.storage.log_timer(ctx.flow_id, current_step, fire_at, name if name else None)

    # Set suspension reason and raise exception to suspend
    reason = SuspendReason(
        flow_id=ctx.flow_id,
        step=current_step,
        signal_name=None,
    )
    ctx.set_suspend_reason(reason)

    # Raise suspension exception for executor to catch
    raise _SuspendExecution()


# Workers handle timer firing by polling storage for expired timers,
# atomically claiming them, storing suspension results, and resuming flows.
# When the flow resumes, schedule_timer_named() checks storage for the
# suspension result and continues execution. No in-memory notification needed.


class TimerError(Exception):
    """Timer operation failed."""

    pass
