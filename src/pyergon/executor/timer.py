"""
Durable timer scheduling with Observer pattern.

Design Pattern: Observer Pattern (Chapter 11 principles)
Timers are "subjects" that notify "observers" (waiting flows) when they fire.

From Rust ergon timer.rs:
- Uses task-local context to access flow_id and step
- Lazy-static TIMER_NOTIFIERS map: (flow_id, step) → Notify
- Timeline: Calculate fire_at → Log to storage → Await notification → Fire
- Handles race conditions (timer may fire between logging and awaiting)

From Dave Cheney's Practical Go:
"Leave concurrency to the caller" - these functions are async,
letting caller decide whether to await or run concurrently.

Python Implementation:
- Module-level dict instead of lazy_static
- asyncio.Event instead of tokio::sync::Notify
- asyncio.Lock for thread-safety

Key Principle:
Timers are DURABLE - they survive process restarts. The storage
backend persists timer state, and workers can fire timers even if
the original process that scheduled them crashed.

FULLY DISTRIBUTED IMPLEMENTATION:
Unlike the previous implementation which used in-memory events, this version
matches the Rust implementation exactly. Timers work across multiple processes
and machines because:

1. Timer state is persisted in storage (database)
2. Workers poll storage for expired timers
3. Workers atomically claim timers to prevent duplicate firing
4. Timer results are stored in suspension_params table
5. Flows check storage for timer results on resume

No in-memory synchronization primitives are used. All coordination happens
through the database, making this fully distributed.

**Rust Equivalent**: See `src/executor/timer.rs` lines 198-294
"""

from datetime import datetime, timedelta

# ========================================================================
# Context Variables - Track current execution context
# ========================================================================
# Use Context from core module
# From Dave Cheney: "Avoid package level state"
# Context is managed by core/context.py using EXECUTION_CONTEXT ContextVar
from pyergon.core import get_current_context as get_execution_context


def get_current_context():
    """
    Get the current execution context (for timer scheduling).

    Uses core/execution_context.py's get_current_context() internally.

    From Dave Cheney: "Return early rather than nesting deeply"
    Guard clause checks precondition immediately.

    Raises:
        TimerError: If no context set (called outside flow)
    """
    ctx = get_execution_context()

    if ctx is None:
        raise TimerError(
            "No execution context set. schedule_timer() must be called within a flow step."
        )

    return ctx


# ========================================================================
# NO IN-MEMORY STATE - Fully Distributed Implementation
# ========================================================================

# Unlike the previous implementation, we don't use in-memory events.
# All timer coordination happens through the database, making this
# fully distributed across multiple processes and machines.


# ========================================================================
# Public API - Timer Scheduling
# ========================================================================


async def schedule_timer(duration: float) -> None:
    """
    Schedule a durable timer within a flow step.

    Design Pattern: Observer Pattern
    The timer (subject) will notify waiting flow (observer) when it fires.

    From Rust ergon: This is the core timer scheduling function.
    Worker polls storage for expired timers and calls notify_timer().

    From Dave Cheney: "Let functions define the behavior they require"
    Takes duration (simple float), not complex timer configuration object.

    Args:
        duration: Timer duration in seconds

    Raises:
        TimerError: If called outside a flow context

    Example:
        @step
        async def wait_for_fraud_check(self):
            await schedule_timer(2.0)  # Wait 2 seconds
            print("Fraud check complete")
    """
    await schedule_timer_named(duration, name="")


async def schedule_timer_named(duration: float, name: str) -> None:
    """
    Schedule a named durable timer for debugging.

    **Rust Reference**: `src/executor/timer.rs` lines 194-294

    This function suspends the flow until the timer fires. The timer is persisted
    to storage and will fire even if the worker crashes (worker polls expired timers).

    **Distributed Implementation**: Uses suspension pattern (not in-memory events).
    Timers work across multiple processes because all state is in the database.

    Args:
        duration: Timer duration in seconds
        name: Timer name for debugging/logging

    Raises:
        TimerError: If called outside a flow context
        _SuspendExecution: Internal exception to signal flow suspension

    Example:
        @step
        async def wait_for_warehouse(self):
            await schedule_timer_named(3.0, "warehouse-processing")
            print("Warehouse processing complete")
    """
    from pyergon.executor.outcome import SuspendReason, _SuspendExecution
    from pyergon.models import InvocationStatus

    # Get current flow context
    ctx = get_current_context()

    # Get the step number from enclosing step (set by @step decorator)
    current_step = ctx.get_enclosing_step()
    if current_step is None:
        raise TimerError("schedule_timer called but no enclosing step set")

    # Check if we're resuming from a timer
    # **Rust Reference**: lines 211-254
    existing_inv = await ctx.storage.get_invocation(ctx.flow_id, current_step)

    if existing_inv is not None:
        if existing_inv.status == InvocationStatus.COMPLETE:
            # Timer already fired and step completed - we're resuming
            # **Rust Reference**: lines 217-220
            return

        if existing_inv.status == InvocationStatus.WAITING_FOR_TIMER:
            # We're waiting for this timer - check if it's fired and has a cached result
            # **Rust Reference**: lines 223-254
            timer_key = name if name else ""

            result_bytes = await ctx.storage.get_suspension_result(
                ctx.flow_id, current_step, timer_key
            )

            if result_bytes is not None:
                # Timer fired! Clean up suspension result so it isn't re-delivered on retry
                # **Rust Reference**: lines 241-245
                await ctx.storage.remove_suspension_result(ctx.flow_id, current_step, timer_key)

                # Deserialize the SuspensionPayload to check success
                # **Rust Reference**: lines 233-239
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
            # **Rust Reference**: lines 256-267
            reason = SuspendReason(
                flow_id=ctx.flow_id,
                step=current_step,
                signal_name=None,  # None indicates timer (not signal)
            )
            ctx.set_suspend_reason(reason)

            # Raise exception to signal suspension
            # This is caught by Executor which checks ctx.take_suspend_reason()
            raise _SuspendExecution()

    # First time - calculate fire time and log timer
    # **Rust Reference**: lines 271-278
    fire_at = datetime.now() + timedelta(seconds=duration)

    await ctx.storage.log_timer(ctx.flow_id, current_step, fire_at, name if name else None)

    # Set suspension reason and raise exception to suspend
    # **Rust Reference**: lines 280-285
    reason = SuspendReason(
        flow_id=ctx.flow_id,
        step=current_step,
        signal_name=None,  # None indicates timer
    )
    ctx.set_suspend_reason(reason)

    # Suspension is control flow, not an error. Raise _SuspendExecution.
    # Executor will catch this and check ctx.take_suspend_reason()
    # **Rust Reference**: line 267 (std::future::pending().await)
    raise _SuspendExecution()


# ========================================================================
# NOTE: No notify_timer() function needed!
# ========================================================================

# Unlike the previous implementation, we don't need a notify_timer() function.
# Workers handle timer firing by:
# 1. Polling storage.get_expired_timers()
# 2. Atomically claiming timer with storage.claim_timer()
# 3. Storing suspension result with storage.store_suspension_result()
# 4. Resuming flow with storage.resume_flow()
#
# When the flow resumes, schedule_timer_named() checks storage for the
# suspension result and continues execution. No in-memory notification needed!
#
# **Rust Reference**: See `src/executor/timer.rs` lines 64-126 for worker logic


# ========================================================================
# Error Types
# ========================================================================


class TimerError(Exception):
    """
    Timer operation failed.

    From Dave Cheney: "Errors are values"
    Custom exception with context, not generic Exception.
    """

    pass
