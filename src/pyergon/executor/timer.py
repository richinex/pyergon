"""Durable timer scheduling for flows.

Timers pause workflow execution for a specified duration. All state is
persisted to storage, surviving process crashes and enabling distributed
operation across multiple workers.

Implementation:
1. schedule_timer_named() persists timer to storage and suspends flow
2. Workers sleep until timer fires (event-driven) or poll periodically (fallback)
3. On timer expiry: worker atomically claims timer, stores SuspensionPayload
4. Flow resumes and checks storage for timer result

Atomic claiming prevents duplicate firing. Observer pattern: timers notify
waiting flows when they fire.
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

    ctx = get_current_context()

    current_step = ctx.get_enclosing_step()
    if current_step is None:
        raise TimerError("schedule_timer called but no enclosing step set")

    existing_inv = await ctx.storage.get_invocation(ctx.flow_id, current_step)

    if existing_inv is not None:
        if existing_inv.status == InvocationStatus.COMPLETE:
            # Timer already fired and step completed - resuming
            return

        if existing_inv.status == InvocationStatus.WAITING_FOR_TIMER:
            timer_key = name if name else ""

            result_bytes = await ctx.storage.get_suspension_result(
                ctx.flow_id, current_step, timer_key
            )

            if result_bytes is not None:
                # Timer fired - clean up suspension result
                await ctx.storage.remove_suspension_result(ctx.flow_id, current_step, timer_key)

                import pickle

                from pyergon.executor.suspension_payload import SuspensionPayload

                try:
                    payload = pickle.loads(result_bytes)

                    # Handle both dict (backward compat) and dataclass
                    if isinstance(payload, SuspensionPayload):
                        if payload.success:
                            return
                        else:
                            # Timer marked as failed (shouldn't happen, but handle gracefully)
                            raise TimerError("Timer marked as failed")
                    elif isinstance(payload, dict):
                        if payload.get("success", False):
                            return
                        else:
                            raise TimerError("Timer marked as failed")
                    else:
                        raise TimerError(f"Invalid payload type: {type(payload)}")
                except Exception as e:
                    raise TimerError(f"Failed to deserialize timer payload: {e}")

            # Timer not fired yet - suspend the flow
            reason = SuspendReason(
                flow_id=ctx.flow_id,
                step=current_step,
                signal_name=None,
            )
            ctx.set_suspend_reason(reason)

            raise _SuspendExecution()

    # First time - calculate fire time and log timer
    fire_at = datetime.now() + timedelta(seconds=duration)

    await ctx.storage.log_timer(ctx.flow_id, current_step, fire_at, name if name else None)

    reason = SuspendReason(
        flow_id=ctx.flow_id,
        step=current_step,
        signal_name=None,
    )
    ctx.set_suspend_reason(reason)

    raise _SuspendExecution()


class TimerError(Exception):
    """Timer operation failed."""

    pass
