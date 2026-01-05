"""Child flow completion handling.

Provides PendingChild, a handle returned from flow.invoke() that
allows awaiting the child's completion and retrieving its result.

Design: Deterministic Child IDs
Child flow IDs are generated deterministically based on parent ID,
child type, and child data. Combined with step caching, this ensures
exactly-once child execution and eliminates race conditions.
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
    """Handle for awaiting a child flow's completion.

    Pure data holder with no side effects until result() is called.
    All logic (UUID generation, scheduling, signaling) happens in
    result(), where step caching automatically handles idempotency.

    Attributes:
        child_bytes: Serialized child flow data
        child_type: Child flow type name for handler lookup

    Example:
        ```python
        # Invoke child flow
        pending = parent_flow.invoke(CheckInventory(product_id="PROD-123"))

        # Await result (type-safe)
        inventory: InventoryStatus = await pending.result()
        ```
    """

    def __init__(self, child_bytes: bytes, child_type: str):
        """Initialize PendingChild handle.

        Args:
            child_bytes: Serialized child flow instance
            child_type: Child flow type name

        Note:
            Typically created by invoke() method, not directly by users.
        """
        self.child_bytes = child_bytes
        self.child_type = child_type

    async def result(self) -> R:
        """Wait for child flow to complete and return its result.

        Handles:
        1. Deterministic child UUID generation (parent + step)
        2. Child flow scheduling (only on first execution)
        3. Suspension waiting for completion
        4. Result retrieval

        Step caching provides idempotency - on replay, cached result
        is returned immediately without re-scheduling the child.

        Child flow IDs are deterministic based on parent flow ID,
        child type, and child data hash. This ensures the same child
        is invoked on replay, critical for durability.

        Returns:
            Child flow's result

        Raises:
            RuntimeError: If called outside an execution context
            Exception: Child flow's exception if child failed

        Example:
            ```python
            @flow
            class ParentFlow:
                async def run(self):
                    pending = self.invoke(CheckInventory(product_id="PROD-123"))
                    inventory = await pending.result()

                    if inventory.in_stock:
                        return "Order confirmed"
                    else:
                        return "Out of stock"
            ```
        """
        ctx = get_current_context()
        if ctx is None:
            raise RuntimeError("result() called outside execution context")

        parent_id = UUID(ctx.flow_id)
        storage = ctx.storage

        hasher = hashlib.sha256()
        hasher.update(self.child_type.encode("utf-8"))
        hasher.update(self.child_bytes)
        # 31-bit positive integer for SQLite INTEGER range
        step = int(int.from_bytes(hasher.digest()[:4], byteorder="little") & 0x7FFFFFFF)

        # Compute params_hash from child data for non-determinism detection
        import xxhash
        params_hash = xxhash.xxh64(self.child_bytes).intdigest() & 0x7FFFFFFFFFFFFFFF

        await ctx.log_step_start(
            step=step,
            class_name="<child_flow>",
            method_name=f"invoke({self.child_type})",
            parameters=self.child_bytes,
            params_hash=params_hash,
            delay=None,
            retry_policy=None,
        )

        # Deterministic UUID ensures same child on replay
        child_hash = hashlib.sha256(self.child_bytes).digest()
        seed = self.child_type.encode("utf-8") + child_hash[:8]
        child_flow_id = uuid5(parent_id, seed.hex())
        signal_name = str(child_flow_id)

        existing_inv = await storage.get_invocation(str(parent_id), step)
        if existing_inv is not None:
            if existing_inv.status == InvocationStatus.COMPLETE:
                if existing_inv.return_value is not None:
                    payload: SuspensionPayload = pickle.loads(existing_inv.return_value)
                    if payload.success:
                        return pickle.loads(payload.data)
                    else:
                        from pyergon.core import ChildFlowError

                        error_msg: str = pickle.loads(payload.data)

                        if ": " in error_msg:
                            type_name, message = error_msg.split(": ", 1)
                        else:
                            type_name, message = "unknown", error_msg

                        retryable = (
                            payload.is_retryable if payload.is_retryable is not None else True
                        )

                        raise ChildFlowError(type_name, message, retryable)

            if existing_inv.status == InvocationStatus.WAITING_FOR_SIGNAL:
                params_bytes = await storage.get_suspension_result(
                    str(parent_id), step, signal_name
                )
                if params_bytes is not None:
                    payload: SuspensionPayload = pickle.loads(params_bytes)
                    await storage.log_invocation_completion(str(parent_id), step, params_bytes)
                    await storage.remove_suspension_result(str(parent_id), step, signal_name)

                    if payload.success:
                        return pickle.loads(payload.data)
                    else:
                        from pyergon.core import ChildFlowError

                        error_msg: str = pickle.loads(payload.data)

                        if ": " in error_msg:
                            type_name, message = error_msg.split(": ", 1)
                        else:
                            type_name, message = "unknown", error_msg

                        retryable = (
                            payload.is_retryable if payload.is_retryable is not None else True
                        )

                        raise ChildFlowError(type_name, message, retryable)

        # CRITICAL: Set WaitingForSignal status BEFORE scheduling child
        # Prevents race where child completes before parent is waiting
        await storage.log_signal(str(parent_id), step, signal_name)

        scheduled_flow = await storage.get_scheduled_flow(str(child_flow_id))
        child_already_scheduled = scheduled_flow is not None

        if not child_already_scheduled:
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
                scheduled_for=None,
                parent_metadata=(str(parent_id), str(child_flow_id)),
                retry_policy=None,
                version=None,
            )

            try:
                await storage.enqueue_flow(scheduled)
            except Exception as e:
                raise RuntimeError(f"Failed to enqueue child flow: {e}") from e

        params_bytes = await storage.get_suspension_result(str(parent_id), step, signal_name)
        if params_bytes is not None:
            payload: SuspensionPayload = pickle.loads(params_bytes)
            await storage.log_invocation_completion(str(parent_id), step, params_bytes)
            await storage.remove_suspension_result(str(parent_id), step, signal_name)

            if payload.success:
                return pickle.loads(payload.data)
            else:
                from pyergon.core import ChildFlowError

                error_msg: str = pickle.loads(payload.data)

                if ": " in error_msg:
                    type_name, message = error_msg.split(": ", 1)
                else:
                    type_name, message = "unknown", error_msg

                retryable = payload.is_retryable if payload.is_retryable is not None else True

                raise ChildFlowError(type_name, message, retryable)

        reason = SuspendReason(flow_id=parent_id, step=step, signal_name=signal_name)
        ctx.set_suspend_reason(reason)

        # Async functions must complete or raise (no pending state)
        # Executor catches _SuspendExecution and returns Suspended(reason)
        from pyergon.executor.outcome import _SuspendExecution

        raise _SuspendExecution()

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return f"PendingChild(child_type={self.child_type!r})"
