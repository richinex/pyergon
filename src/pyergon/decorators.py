"""
Instrumented decorators for durable workflows.

This module provides @flow and @step decorators with FULL instrumentation
matching Rust macro-generated code.

RUST COMPLIANCE: Matches Rust #[step] and #[flow] macro behavior exactly.

From Rust ergon macros:
- Cache checking with params_hash
- Automatic logging (log_step_start/completion)
- Retry loop with exponential backoff
- Integration with Context
- CallType handling

Design: Decorators generate instrumentation code around user functions,
similar to how Rust macros expand.
"""

import asyncio
import functools
import pickle
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

import xxhash

from pyergon.core import (
    _CACHE_MISS,
    RetryableError,
    RetryPolicy,
    get_current_context,
)

if TYPE_CHECKING:
    from pyergon.core.invokable_flow import InvokableFlow
    from pyergon.executor.pending_child import PendingChild

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

# Sentinel value to detect when depends_on was not provided
_UNSET = object()


def step(
    func: F | None = None,
    *,
    depends_on: str | list[str] | None = _UNSET,  # type: ignore
    inputs: dict[str, str] | None = None,
    cache_errors: bool = False,
    delay: int | None = None,
    retry_policy: RetryPolicy | None = None,
) -> F:
    """
    Mark a method as a durable workflow step with FULL instrumentation.

    RUST COMPLIANCE: Matches Rust #[step] macro behavior exactly.
    Generates code similar to what Rust macro expands to.

    This decorator wraps the function with:
    1. Cache checking (params_hash comparison)
    2. Automatic logging (log_step_start/completion)
    3. Retry loop (exponential backoff)
    4. Error handling (is_retryable check)
    5. Parameter serialization

    Args:
        func: The function to decorate
        depends_on: Dependencies for DAG execution
        inputs: Parameter wiring from other steps
        cache_errors: Cache error results (don't retry)
        delay: Delay in milliseconds before execution
        retry_policy: Retry policy for this step

    Example:
        ```python
        @step
        async def fetch_data(self) -> dict:
            # Step is cached, logged, and retried automatically
            return await api.get("/data")

        @step(retry_policy=RetryPolicy.AGGRESSIVE)
        async def critical_step(self) -> str:
            # Custom retry policy
            return await important_operation()

        @step(delay=1000)
        async def delayed_step(self) -> None:
            # Waits 1 second before executing
            pass
        ```

    From Rust #[step] macro:
        The macro generates ~50 lines of instrumentation code
        around the user's function. This decorator does the same.
    """

    def decorator(f: F) -> F:
        # Add metadata (like before)
        f._is_ergon_step = True  # type: ignore
        f._step_name = f.__name__  # type: ignore
        f._step_cache_errors = cache_errors  # type: ignore
        f._step_delay = delay  # type: ignore
        f._step_retry_policy = retry_policy  # type: ignore

        # Build dependencies
        has_explicit_empty_depends_on = (
            depends_on is not _UNSET and isinstance(depends_on, list) and len(depends_on) == 0
        )

        base_deps: list[str] = []
        if depends_on is not _UNSET and depends_on is not None:
            if isinstance(depends_on, str):
                base_deps = [depends_on]
            elif isinstance(depends_on, list):
                base_deps = list(depends_on)

        input_deps: list[str] = []
        if inputs:
            input_deps = list(inputs.values())

        all_deps = base_deps.copy()
        for input_step in input_deps:
            if input_step not in all_deps:
                all_deps.append(input_step)

        if has_explicit_empty_depends_on and not inputs:
            f._step_depends_on = ["__NO_AUTO_CHAIN__"]  # type: ignore
        elif all_deps:
            f._step_depends_on = all_deps  # type: ignore
        else:
            f._step_depends_on = []  # type: ignore

        f._step_inputs = inputs or {}  # type: ignore
        f._step_has_explicit_empty_depends_on = has_explicit_empty_depends_on  # type: ignore

        # Create instrumented wrapper
        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            """
            Instrumented wrapper matching Rust macro expansion.

            From Rust #[step] macro expansion:
            1. Get Context
            2. Allocate step number
            3. Serialize parameters
            4. Check cache
            5. Log start
            6. Execute (with retry)
            7. Log completion
            """
            # Get Context (task-local)
            # From Rust: EXECUTION_CONTEXT.try_with(|c| c.clone())
            ctx = get_current_context()

            if ctx is None:
                # No Context - execute without instrumentation
                # This happens when step is called outside Executor
                return await f(*args, **kwargs)

            # Extract class and method names
            class_name = args[0].__class__.__name__ if args else "Unknown"
            method_name = f.__name__

            # Serialize parameters (skip 'self')
            # From Rust: let params = serialize_value(&params)?
            params_dict = {"args": args[1:] if len(args) > 1 else [], "kwargs": kwargs}
            try:
                parameters = pickle.dumps(params_dict)
            except Exception:
                # If we can't serialize, we can't cache - just execute
                return await f(*args, **kwargs)

            # Compute step ID using hash (matches Rust step.rs:691-697)
            # Hash-based IDs are stable across replays (unlike counters)
            # IMPORTANT: Hash only method name, NOT parameters
            # This ensures same step gets same ID even if parameters change
            # (parameter changes are detected via params_hash check)
            # OPTIMIZATION: Cache step IDs (they never change for a given method)
            hash_key = f"{class_name}.{method_name}"
            if not hasattr(async_wrapper, "_step_id_cache"):
                async_wrapper._step_id_cache = {}  # type: ignore

            if hash_key not in async_wrapper._step_id_cache:  # type: ignore
                # OPTIMIZATION: Using xxhash (4-6x faster than SHA256)
                step_num = xxhash.xxh64(hash_key.encode("utf-8")).intdigest() & 0x7FFFFFFF
                async_wrapper._step_id_cache[hash_key] = step_num  # type: ignore
            else:
                step_num = async_wrapper._step_id_cache[hash_key]  # type: ignore

            # Compute params hash
            # From Rust: let params_hash = hash(&params)
            params_hash = _compute_params_hash(parameters)

            # Save and set enclosing step (matches Rust step.rs:700-706)
            prev_enclosing_step = ctx.get_enclosing_step()
            ctx.set_enclosing_step(step_num)

            # Check cache
            # From Rust: if let Some(cached) = ctx.get_cached_result(...)
            cached = await ctx.get_cached_result(
                step=step_num,
                class_name=class_name,
                method_name=method_name,
                params_hash=params_hash,
            )

            if cached is not _CACHE_MISS:
                # Cache hit - restore enclosing step before returning (Rust step.rs:734-737)
                if prev_enclosing_step is not None:
                    ctx.set_enclosing_step(prev_enclosing_step)
                # Return cached result (can be None)
                # (get_cached_result raises if cached exception)
                return cached

            # Log step start
            # From Rust: ctx.log_step_start(LogStepStartParams { ... })
            await ctx.log_step_start(
                step=step_num,
                class_name=class_name,
                method_name=method_name,
                parameters=parameters,
                params_hash=params_hash,
                delay=delay,
                retry_policy=retry_policy,
            )

            # Execute with retry loop
            # From Rust: loop { match step_fn().await { ... } }
            attempts = 0
            max_attempts = retry_policy.max_attempts if retry_policy else 1

            while True:
                try:
                    # Execute step
                    result = await f(*args, **kwargs)

                    # Log completion (success)
                    result_bytes = pickle.dumps(result)
                    await ctx.log_step_completion(
                        step=step_num,
                        return_value=result_bytes,
                        is_retryable=None,  # None = not an error
                    )

                    # Restore enclosing step before returning (Rust step.rs:792-793)
                    if prev_enclosing_step is not None:
                        ctx.set_enclosing_step(prev_enclosing_step)

                    return result

                except Exception as e:
                    attempts += 1

                    # Check if error is retryable
                    is_retryable = True  # Default: errors are retryable
                    if isinstance(e, RetryableError):
                        is_retryable = e.is_retryable()
                    elif cache_errors:
                        is_retryable = False  # cache_errors means don't retry

                    # Should we retry?
                    should_retry = (
                        is_retryable and attempts < max_attempts and retry_policy is not None
                    )

                    if should_retry:
                        # Calculate backoff delay
                        delay_ms = retry_policy.delay_for_attempt(attempts)
                        if delay_ms is not None:
                            await asyncio.sleep(delay_ms / 1000.0)
                        # Continue loop (retry)
                        continue
                    else:
                        # No more retries
                        # ALWAYS store is_retryable flag (for has_non_retryable_error check)
                        # This ensures worker can determine if error is retryable at flow level
                        await ctx.update_step_retryability(step_num, is_retryable)

                        # From Rust step macro lines 434-452:
                        # Only cache non-retryable (permanent) errors
                        # Retryable errors cause flow-level retry and should NOT be cached
                        if not is_retryable:
                            # Permanent error: cache it
                            error_bytes = pickle.dumps(e)
                            await ctx.log_step_completion(
                                step=step_num,
                                return_value=error_bytes,
                                is_retryable=None,  # Already stored via update_step_retryability
                            )
                        # Always raise (retryable errors will trigger flow-level retry)
                        raise

        # Return async wrapper
        return async_wrapper  # type: ignore

    # Support both @step and @step(...) syntax
    if func is not None:
        return decorator(func)
    else:
        return decorator


def flow_type(
    cls: type[T] = None,
    *,
    type_id: str | None = None,
    retry_policy: RetryPolicy | None = None,
    invokable: type | None = None,
) -> type[T]:
    """
    Mark a class as a FlowType with optional InvokableFlow support.

    RUST COMPLIANCE: Matches Rust #[derive(FlowType)] and #[invokable] macros.

    **Rust Reference**: `ergon_macros/src/flow_type.rs` lines 5-68

    This decorator:
    1. Implements FlowType protocol (type_id() method)
    2. Optionally implements InvokableFlow protocol
    3. Collects all @step methods in definition order
    4. Attaches retry_policy for flow-level retries

    Args:
        cls: The class to decorate
        type_id: Optional custom type identifier (defaults to class name)
        retry_policy: Optional retry policy for flow-level retries
        invokable: Optional output type for InvokableFlow protocol
                   (matches Rust #[invokable(output = Type)])

    Example:
        ```python
        from pyergon.core import RetryPolicy

        # Basic flow type
        @flow_type
        class OrderProcessor:
            @step
            async def validate(self) -> bool:
                return True

            @flow
            async def process(self) -> str:
                return "done"

        # Invokable child flow (matches Rust #[invokable(output = String)])
        @flow_type(invokable=str)
        class PaymentFlow:
            @flow
            async def process(self) -> str:
                return "payment-id"

        # Flow with retry policy
        @flow_type(retry_policy=RetryPolicy.STANDARD)
        class ResilientFlow:
            @flow
            async def run(self) -> str:
                return "resilient"
        ```

    From Rust:
        ```rust
        #[derive(FlowType)]
        #[invokable(output = String)]
        struct PaymentFlow { ... }
        ```
    """

    def decorator(c: type[T]) -> type[T]:
        # Add metadata
        c._is_ergon_flow = True  # type: ignore

        # Attach retry policy if provided
        if retry_policy is not None:
            c._ergon_retry_policy = retry_policy  # type: ignore

        # Implement FlowType protocol (line 43-50 in flow_type.rs)
        flow_type_id = type_id if type_id is not None else c.__name__

        @staticmethod
        def _type_id() -> str:
            return flow_type_id

        c.type_id = _type_id  # type: ignore

        # Implement InvokableFlow protocol if invokable parameter provided
        # From Rust flow_type.rs lines 53-60
        if invokable is not None:
            c._invokable_output_type = invokable  # type: ignore
            # Mark as implementing InvokableFlow protocol
            c._implements_invokable = True  # type: ignore

        # Add invoke() method for child flow invocation
        # From Rust: InvokeChild trait extension on Arc<T> where T: FlowType
        def invoke(self, child: "InvokableFlow") -> "PendingChild":
            """
            Invoke a child flow and get a handle to await its result.

            **Rust Reference**: `src/executor/child_flow.rs` lines 361-384

            This method:
            1. Serializes the child flow
            2. Returns a PendingChild handle
            3. Parent can later await the result via .result()

            **Type Inference**:
            The result type is automatically inferred from the child's Output type:

            ```python
            # CheckInventory implements InvokableFlow[InventoryStatus]
            pending = self.invoke(CheckInventory(product_id="PROD-123"))
            inventory: InventoryStatus = await pending.result()
            # ^^^^^^^^^^^ Type inferred as InventoryStatus!
            ```

            **No Manual Fields Required**:
            Unlike Level 2, the child flow does NOT need:
            - parent_flow_id field
            - Any knowledge of its parent

            The invocation establishes the relationship automatically.

            Args:
                child: Child flow instance (must implement InvokableFlow)

            Returns:
                PendingChild handle to await result

            Example:
                ```python
                @flow
                class ParentFlow:
                    async def run(self):
                        # Invoke child
                        pending = self.invoke(ChildFlow(data="test"))

                        # Await result
                        result = await pending.result()
                        return result
                ```

            From Rust:
                ```rust
                fn invoke<C>(&self, child: C) -> PendingChild<C::Output>
                where C: InvokableFlow + Serialize + ...
                ```
            """
            # Import here to avoid circular dependency
            from pyergon.executor.pending_child import PendingChild

            # Pure builder - just capture data, no side effects
            # All logic (UUID generation, scheduling) happens in .result()
            # From Rust lines 376-383
            try:
                child_bytes = pickle.dumps(child)
            except Exception as e:
                raise RuntimeError(f"Failed to serialize child flow: {e}") from e

            # Get child type ID (must have type_id() method from FlowType)
            if not hasattr(child, "type_id") or not callable(child.type_id):
                raise TypeError(
                    f"Child flow {child.__class__.__name__} must implement FlowType "
                    "(use @flow decorator or implement type_id() method)"
                )

            child_type = child.type_id()

            return PendingChild(child_bytes=child_bytes, child_type=child_type)

        c.invoke = invoke  # type: ignore

        # Collect @step methods in definition order
        steps = []
        for name in c.__dict__:
            attr = getattr(c, name)
            if callable(attr) and hasattr(attr, "_is_ergon_step"):
                steps.append(name)

        c._ergon_steps = steps  # type: ignore

        return c

    # Support both @flow_type and @flow_type(...) syntax
    if cls is not None:
        return decorator(cls)
    else:
        return decorator


def flow(func: F | None = None, *, retry_policy: RetryPolicy | None = None) -> F:
    """
    Mark an async method as a flow entry point.

    RUST COMPLIANCE: Matches Rust #[flow] macro (method-level decorator).

    **Rust Reference**: `ergon_macros/src/flow.rs` lines 67-258

    This decorator marks which method is the entry point for flow execution.
    In Rust, #[flow] is applied to methods within an impl block.
    In Python, @flow is applied to methods within a class.

    The @flow method gets full instrumentation:
    - Context management
    - Caching with params_hash
    - Logging (start/completion)
    - Suspension handling (child flows, timers, signals)

    Args:
        func: The async method to decorate
        retry_policy: Optional retry policy for this flow method

    Example:
        ```python
        @dataclass
        @flow_type
        class DataPipeline:
            id: str
            value: int

            @step
            async def validate(self) -> int:
                return self.value

            @step
            async def transform(self, input: int) -> int:
                return input * 2

            @flow  # Entry point method
            async def process(self) -> int:
                validated = await self.validate()
                return await self.transform(validated)
        ```

    From Rust:
        ```rust
        impl DataPipeline {
            #[flow]
            async fn process(self: Arc<Self>) -> Result<i32> {
                let validated = self.clone().validate().await?;
                self.transform(validated).await
            }
        }
        ```
    """

    def decorator(f: F) -> F:
        # Mark as flow entry point
        f._is_ergon_flow_method = True  # type: ignore
        f._flow_method_name = f.__name__  # type: ignore
        f._flow_retry_policy = retry_policy  # type: ignore

        # Create instrumented wrapper
        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            """
            Instrumented wrapper matching Rust flow macro expansion.

            From Rust flow.rs lines 170-254:
            1. Get Context
            2. Allocate step number
            3. Serialize parameters
            4. Check cache
            5. Log start
            6. Execute flow
            7. Log completion (if not suspending)
            """
            # Get Context (task-local)
            ctx = get_current_context()

            if ctx is None:
                # No Context - execute without instrumentation
                return await f(*args, **kwargs)

            # Extract class and method names
            class_name = args[0].__class__.__name__ if args else "Unknown"
            method_name = f.__name__

            # Serialize parameters (skip 'self')
            params_dict = {"args": args[1:] if len(args) > 1 else [], "kwargs": kwargs}
            try:
                parameters = pickle.dumps(params_dict)
            except Exception:
                # If we can't serialize, execute without caching
                return await f(*args, **kwargs)

            # Compute step ID (step 0 for flow entry point)
            # From Rust flow.rs line 178: let __step = __ctx.next_step();
            step_num = 0  # Flow entry point is always step 0

            # Compute params hash
            params_hash = _compute_params_hash(parameters)

            # Check cache
            cached = await ctx.get_cached_result(
                step=step_num,
                class_name=class_name,
                method_name=method_name,
                params_hash=params_hash,
            )

            if cached is not _CACHE_MISS:
                # Cache hit - return cached result
                return cached

            # Log flow start
            # From Rust flow.rs lines 220-231
            await ctx.log_step_start(
                step=step_num,
                class_name=class_name,
                method_name=method_name,
                parameters=parameters,
                params_hash=params_hash,
                delay=None,
                retry_policy=retry_policy,
            )

            # Set enclosing step (line 235 in flow.rs)
            # This allows child flows/timers/signals to know they're called from this flow
            prev_enclosing_step = ctx.get_enclosing_step()
            ctx.set_enclosing_step(step_num)

            try:
                # Execute the flow method
                result = await f(*args, **kwargs)

                # Check if flow is suspending (line 242 in flow.rs)
                # Suspension happens when waiting for child flow, timer, or signal
                if not ctx.has_suspend_reason():
                    # Normal completion - log it
                    result_bytes = pickle.dumps(result)
                    await ctx.log_step_completion(
                        step=step_num, return_value=result_bytes, is_retryable=None
                    )

                # Restore enclosing step
                if prev_enclosing_step is not None:
                    ctx.set_enclosing_step(prev_enclosing_step)

                return result

            except Exception:
                # Restore enclosing step before raising
                if prev_enclosing_step is not None:
                    ctx.set_enclosing_step(prev_enclosing_step)
                raise

        # Return async wrapper
        return async_wrapper  # type: ignore

    # Support both @flow and @flow(...) syntax
    if func is not None:
        return decorator(func)
    else:
        return decorator


def _compute_params_hash(parameters: bytes) -> int:
    """
    Compute hash using xxHash (4-6x faster than SHA256).

    Performance: xxHash is 4-6x faster than SHA256 for non-cryptographic use
    Deterministic: Yes, xxHash is deterministic across platforms

    From Rust: hash(&params) using seahash
    Note: xxHash is comparable to seahash in speed, deterministic, and widely used

    Args:
        parameters: Serialized parameters

    Returns:
        Hash value as integer (masked to 63 bits for SQLite INTEGER)
    """
    # xxHash provides intdigest() for direct integer output (no struct.unpack needed)
    # Mask to 63 bits for SQLite INTEGER (signed 64-bit, max 2^63-1)
    # Matching Rust's "params_hash as i64" cast behavior
    return xxhash.xxh64(parameters).intdigest() & 0x7FFFFFFFFFFFFFFF
