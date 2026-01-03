"""
PendingChild - Handle for awaiting child flow completion.

**Rust Reference**:
`/home/richinex/Documents/devs/rust_projects/ergon/ergon/src/executor/child_flow.rs` lines 64-326

This module provides PendingChild, which is returned from flow.invoke() and
provides a .result() method to await the child's completion.

**Python Documentation**:
- Generic types: https://docs.python.org/3/library/typing.html#typing.Generic
- async/await: https://docs.python.org/3/library/asyncio-task.html
- UUID: https://docs.python.org/3/library/uuid.html
- hashlib: https://docs.python.org/3/library/hashlib.html

From Dave Cheney: "Define errors out of existence"
By generating deterministic child UUIDs and using step caching,
we eliminate race conditions and ensure exactly-once child execution.
"""

import hashlib
import pickle
from datetime import UTC, datetime
from typing import Generic, TypeVar
from uuid import UUID, uuid5

from pyergon.core import (
    InvocationStatus,
    ScheduledFlow,
    TaskStatus,
    get_current_context,
)
from pyergon.executor.outcome import SuspendReason
from pyergon.executor.suspension_payload import SuspensionPayload

__all__ = ["PendingChild"]

# Type variable for child flow output
R = TypeVar("R")


class PendingChild(Generic[R]):
    """
    Handle for awaiting a child flow's completion.

    **Rust Reference**: `src/executor/child_flow.rs` lines 64-78

    This is a pure data holder - no side effects until `.result()` is called.
    All logic (UUID generation, scheduling, signaling) happens in `result()`,
    where step caching automatically handles idempotency.

    **Python Best Practice**: Using Generic for type safety
    Reference: BEST_PRACTICES.md section "Generic Types (Type Safety)"

    **From Rust**:
    ```rust
    pub struct PendingChild<R> {
        child_bytes: Vec<u8>,
        child_type: String,
        _result: PhantomData<R>,
    }
    ```

    Attributes:
        child_bytes: Serialized child flow data (pickled)
        child_type: Child flow type name (for handler lookup)

    Example:
        ```python
        # Invoke child flow
        pending = parent_flow.invoke(CheckInventory(product_id="PROD-123"))

        # Await result (type-safe!)
        inventory: InventoryStatus = await pending.result()
        ```

    Note:
        The generic type R is only for type checking. At runtime, Python
        doesn't enforce this, but static type checkers (mypy, pyright) will.
    """

    def __init__(self, child_bytes: bytes, child_type: str):
        """
        Initialize PendingChild handle.

        **Python Best Practice**: Simple constructor with clear types
        Reference: https://docs.python.org/3/library/typing.html

        Args:
            child_bytes: Serialized child flow instance
            child_type: Child flow type name (from FlowType.type_id())

        Note:
            This is typically created by invoke() method, not directly by users.
        """
        self.child_bytes = child_bytes
        self.child_type = child_type

    async def result(self) -> R:
        """
        Wait for the child flow to complete and return its result.

        **Rust Reference**: `src/executor/child_flow.rs` lines 84-325

        This method does all the work:
        1. Generates deterministic child UUID (based on parent + step)
        2. Schedules the child flow (only on first execution)
        3. Suspends waiting for child completion
        4. Returns the child's result

        Step caching automatically handles idempotency - on replay, the cached
        result is returned immediately without re-scheduling the child.

        **Type Safety**:
        The result type R is checked at compile time. If the child returns
        a different type than expected, static type checkers will catch it.

        **Determinism**:
        Child flow IDs are deterministic based on:
        - Parent flow ID
        - Child type
        - Child data hash

        This ensures the same child is invoked on replay (critical for durability).

        **Python Documentation**:
        - hashlib for deterministic hashing: https://docs.python.org/3/library/hashlib.html
        - UUID v5 for namespace-based UUIDs: https://docs.python.org/3/library/uuid.html#uuid.uuid5

        Returns:
            The child flow's result (type R)

        Raises:
            RuntimeError: If called outside an execution context
            Exception: Child flow's exception (if child failed)

        Example:
            ```python
            @flow
            class ParentFlow:
                async def run(self):
                    # Invoke child
                    pending = self.invoke(CheckInventory(product_id="PROD-123"))

                    # Await result
                    inventory = await pending.result()

                    if inventory.in_stock:
                        return "Order confirmed"
                    else:
                        return "Out of stock"
            ```
        """
        # Get current context and storage
        # From Rust: EXECUTION_CONTEXT.try_with(|c| c.clone())
        ctx = get_current_context()
        if ctx is None:
            raise RuntimeError("result() called outside execution context")

        parent_id = UUID(ctx.flow_id)
        storage = ctx.storage

        # Generate a stable step ID for this child invocation based on:
        # 1. Child type
        # 2. Child data hash
        # This ensures each unique child invocation gets a stable, unique step ID
        #
        # From Rust lines 119-124:
        # let step = {
        #     let mut hasher = DefaultHasher::new();
        #     self.child_type.hash(&mut hasher);
        #     self.child_bytes.hash(&mut hasher);
        #     (hasher.finish() & 0x7FFFFFFF) as i32
        # };
        hasher = hashlib.sha256()
        hasher.update(self.child_type.encode("utf-8"))
        hasher.update(self.child_bytes)
        # Generate step as a 31-bit positive integer (matching Rust's i32 behavior)
        # Ensure it fits in SQLite INTEGER range
        step = int(int.from_bytes(hasher.digest()[:4], byteorder="little") & 0x7FFFFFFF)

        # Log invocation start - this creates the invocation in storage
        # Required before await_external_signal can call log_signal
        # From Rust lines 128-138
        await ctx.log_step_start(
            step=step,
            class_name="<child_flow>",
            method_name=f"invoke({self.child_type})",
            parameters=b"",  # No parameters for child invocation
            params_hash=0,  # No hash for empty parameters
            delay=None,
            retry_policy=None,
        )

        # Generate deterministic child UUID based on parent + child_type + child_data_hash
        # This ensures uniqueness regardless of step counter state (important on replay
        # when await_external_signal returns cached results without allocating steps)
        #
        # From Rust lines 143-151:
        # let mut seed = Vec::new();
        # seed.extend_from_slice(self.child_type.as_bytes());
        # seed.extend_from_slice(&child_hash.to_le_bytes());
        # let child_flow_id = Uuid::new_v5(&parent_id, &seed);
        child_hash = hashlib.sha256(self.child_bytes).digest()
        seed = self.child_type.encode("utf-8") + child_hash[:8]
        child_flow_id = uuid5(parent_id, seed.hex())
        signal_name = str(child_flow_id)

        # Check if we already completed this (replay scenario)
        # From Rust lines 154-187
        existing_inv = await storage.get_invocation(str(parent_id), step)
        if existing_inv is not None:
            if existing_inv.status == InvocationStatus.COMPLETE:
                # Return cached result
                if existing_inv.return_value is not None:
                    payload: SuspensionPayload = pickle.loads(existing_inv.return_value)
                    if payload.success:
                        # Child succeeded - return result
                        return pickle.loads(payload.data)
                    else:
                        # Child failed - deserialize and raise error
                        # **Rust Reference**: child_flow.rs lines 179-183 (ExecutionError::User)
                        from pyergon.core import ChildFlowError

                        error_msg: str = pickle.loads(payload.data)

                        # Parse type_name and message from formatted string
                        # Format is "type_name: message"
                        if ": " in error_msg:
                            type_name, message = error_msg.split(": ", 1)
                        else:
                            type_name, message = "unknown", error_msg

                        # Get retryability from payload (default to True if not set)
                        # **Rust Reference**: payload.is_retryable.unwrap_or(true)
                        retryable = (
                            payload.is_retryable if payload.is_retryable is not None else True
                        )

                        # Raise ChildFlowError with retryability information preserved
                        raise ChildFlowError(type_name, message, retryable)

            # Check if we're already waiting and signal has arrived
            # From Rust lines 189-226
            if existing_inv.status == InvocationStatus.WAITING_FOR_SIGNAL:
                params_bytes = await storage.get_suspension_result(
                    str(parent_id), step, signal_name
                )
                if params_bytes is not None:
                    payload: SuspensionPayload = pickle.loads(params_bytes)
                    # Complete invocation and clean up
                    await storage.log_invocation_completion(str(parent_id), step, params_bytes)
                    await storage.remove_suspension_result(str(parent_id), step, signal_name)

                    if payload.success:
                        # Child succeeded - return result
                        return pickle.loads(payload.data)
                    else:
                        # Child failed - deserialize and raise error
                        # **Rust Reference**: child_flow.rs lines 219-223 (ExecutionError::User)
                        from pyergon.core import ChildFlowError

                        error_msg: str = pickle.loads(payload.data)

                        # Parse type_name and message from formatted string
                        if ": " in error_msg:
                            type_name, message = error_msg.split(": ", 1)
                        else:
                            type_name, message = "unknown", error_msg

                        # Get retryability from payload (default to True if not set)
                        retryable = (
                            payload.is_retryable if payload.is_retryable is not None else True
                        )

                        # Raise ChildFlowError with retryability information preserved
                        raise ChildFlowError(type_name, message, retryable)
                # Otherwise fall through to suspend below

        # CRITICAL: Set WaitingForSignal status BEFORE scheduling child!
        # This prevents race condition where child completes before parent is waiting
        # From Rust lines 230-235
        await storage.log_signal(str(parent_id), step, signal_name)

        # Check if this child was already scheduled (replay scenario)
        # We check the child flow's existence in storage rather than step completion
        # From Rust lines 237-244
        scheduled_flow = await storage.get_scheduled_flow(str(child_flow_id))
        child_already_scheduled = scheduled_flow is not None

        # Only schedule child on first execution (not replay)
        # From Rust lines 246-273
        if not child_already_scheduled:
            # Create ScheduledFlow with parent metadata
            # CRITICAL: parent_metadata enables complete_child_flow to signal parent
            now = datetime.now(UTC)
            scheduled = ScheduledFlow(
                task_id=str(child_flow_id),
                flow_id=str(child_flow_id),
                flow_type=self.child_type,
                flow_data=self.child_bytes,
                status=TaskStatus.PENDING,
                locked_by=None,
                created_at=now,
                updated_at=now,
                retry_count=0,
                error_message=None,
                scheduled_for=None,  # Execute immediately
                # Level 3 metadata: parent and token as tuple
                # This is how the worker knows to call complete_child_flow with parent info
                parent_metadata=(str(parent_id), str(child_flow_id)),
                retry_policy=None,
                version=None,  # Child flows inherit parent's deployment, no separate version
            )

            # Enqueue synchronously - must succeed before we wait
            # This ensures child is guaranteed to be enqueued before parent suspends
            # From Rust line 270: storage.enqueue_flow(scheduled).await
            try:
                await storage.enqueue_flow(scheduled)
            except Exception as e:
                raise RuntimeError(f"Failed to enqueue child flow: {e}") from e

        # Check if signal already arrived (child might have been VERY fast)
        # From Rust lines 275-311
        params_bytes = await storage.get_suspension_result(str(parent_id), step, signal_name)
        if params_bytes is not None:
            payload: SuspensionPayload = pickle.loads(params_bytes)
            # Complete invocation and clean up
            await storage.log_invocation_completion(str(parent_id), step, params_bytes)
            await storage.remove_suspension_result(str(parent_id), step, signal_name)

            if payload.success:
                # Child succeeded - return result
                return pickle.loads(payload.data)
            else:
                # Child failed - deserialize and raise error
                # **Rust Reference**: child_flow.rs lines 305-309 (ExecutionError::User)
                from pyergon.core import ChildFlowError

                error_msg: str = pickle.loads(payload.data)

                # Parse type_name and message from formatted string
                if ": " in error_msg:
                    type_name, message = error_msg.split(": ", 1)
                else:
                    type_name, message = "unknown", error_msg

                # Get retryability from payload (default to True if not set)
                retryable = payload.is_retryable if payload.is_retryable is not None else True

                # Raise ChildFlowError with retryability information preserved
                raise ChildFlowError(type_name, message, retryable)

        # Suspend - signal hasn't arrived yet
        # From Rust lines 313-324
        reason = SuspendReason(flow_id=parent_id, step=step, signal_name=signal_name)
        ctx.set_suspend_reason(reason)

        # In Rust, std::future::pending() returns Poll::Pending without completing.
        # Python doesn't have this mechanism - async functions must complete or raise.
        #
        # Solution: Raise a special _SuspendExecution exception that the Executor catches.
        # This is an internal control flow mechanism, not a user-visible error.
        #
        # The executor will catch this exception and check ctx.take_suspend_reason()
        # to get the suspension details, then return Suspended(reason).
        from pyergon.executor.outcome import _SuspendExecution

        raise _SuspendExecution()

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return f"PendingChild(child_type={self.child_type!r})"
