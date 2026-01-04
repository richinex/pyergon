"""
Worker - Distributed worker that polls and executes flows.

Design Pattern: Template Method Pattern (Chapter 8)
The main loop (_run) defines the algorithm skeleton:
1. Process timers (if enabled) - HOOK
2. Process flows - HOOK
3. Sleep before next poll - FIXED

Subclasses or configuration can customize the hooks without changing the loop structure.

Design Pattern: Strategy Pattern (Chapter 8)
Flow handlers are strategies - different execution strategies for different flow types.
Registered via register() method, executed polymorphically.

From Rust ergon worker.rs:
- Polls storage queue for pending flows
- Spawns flow execution in background (non-blocking)
- Processes expired timers (if enabled)
- Implements retry logic with exponential backoff
- Supports graceful shutdown

From Dave Cheney's Practical Go:
"Never start a goroutine without knowing when it will stop" - worker has explicit
shutdown mechanism via WorkerHandle.

From Software Design Patterns:
Like GameReport (Chapter 8), worker defines fixed algorithm with variation points.
"""

import asyncio
import logging
import pickle
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, Generic, TypeVar
from uuid import UUID

from pyergon.core import ScheduledFlow
from pyergon.executor.execution import (
    handle_flow_completion,
    handle_flow_error,
    handle_suspended_flow,
)
from pyergon.executor.instance import Executor
from pyergon.executor.outcome import Completed, Suspended
from pyergon.storage import TimerNotificationSource, WorkNotificationSource
from pyergon.storage.base import ExecutionLog

logger = logging.getLogger(__name__)

# Type variable for storage backend
S = TypeVar("S", bound=ExecutionLog)


class Registry(Generic[S]):
    """
    Registry that maps flow type names to their executors.

    **Rust Reference**: `src/executor/worker.rs` lines 188-376

    The Registry stores executor functions that can deserialize and execute
    flows based on their type name. This allows the worker to handle different
    flow types dynamically.

    From Rust ergon:
    - Registry<S: ExecutionLog> stores HashMap<String, BoxedExecutor<S>>
    - BoxedExecutor is Arc-wrapped closure that takes serialized flow data
    - register() wraps user's closure in deserialization + execution logic
    - get_executor() returns registered executor or None

    Example:
        registry = Registry()

        # Register with explicit method reference
        registry.register(HolidaySaga, lambda saga: saga.run_saga())

        # Or register with default 'run' method
        registry.register(OrderFlow, lambda flow: flow.run())
    """

    def __init__(self):
        """
        Creates a new empty flow registry.

        **Rust Reference**: lines 222-227
        """
        # Maps flow_type_id -> executor function
        # Executor signature: (flow_data: bytes, flow_id: UUID, storage: S,
        #                      parent_metadata: Optional[tuple]) -> FlowOutcome
        self._executors: dict[str, Callable] = {}

    def register(self, flow_class: type, executor: Callable[[Any], Awaitable[Any]]) -> None:
        """
        Registers a flow type with its executor function.

        **Rust Reference**: lines 261-351

        The executor function receives the deserialized flow instance
        and should return a future that produces the flow's result.

        In Rust:
        ```rust
        pub fn register<T, F, Fut, R, E>(&mut self, executor: F)
        where
            T: DeserializeOwned + FlowType + Send + Sync + Clone + 'static,
            F: Fn(Arc<T>) -> Fut + Send + Sync + Clone + 'static,
        ```

        Args:
            flow_class: The flow type class (must have FlowType protocol)
            executor: Function that takes flow instance and returns awaitable result

        Example:
            registry.register(HolidaySaga, lambda saga: saga.run_saga())
            registry.register(OrderFlow, lambda flow: flow.process())
        """
        # Get stable type ID (Rust uses T::type_id())
        # Python @flow decorator adds type_id() method
        if hasattr(flow_class, "type_id"):
            type_name = flow_class.type_id()
        else:
            # Fallback to class name only (for non-@flow classes)
            type_name = flow_class.__name__

        async def boxed_executor(
            flow_data: bytes, flow_id: UUID, storage: S, parent_metadata: tuple | None = None
        ):
            """
            Boxed executor that mirrors Rust's BoxedExecutor behavior.

            **Rust Reference**: lines 273-346

            This function:
            1. Deserializes the flow from bytes
            2. Creates an Executor instance
            3. Executes the user's closure
            4. Maps result to FlowOutcome<Result<(), Exception>>
            """
            try:
                # Deserialize flow instance (Rust line 285)
                flow_instance = pickle.loads(flow_data)
            except Exception as e:
                # Rust lines 287-292
                return Completed(result=Exception(f"failed to deserialize flow: {e}"))

            # Create executor (Rust line 296)
            exec = Executor(flow=flow_instance, storage=storage, flow_id=flow_id)

            # Execute the flow with user's closure (Rust lines 298-302)
            # The executor expects a lambda that calls the user's method
            outcome = await exec.run(lambda _: executor(flow_instance))

            # Map FlowOutcome (Rust lines 305-344)
            # The outcome is already a FlowOutcome (Suspended or Completed)
            # Just return it directly
            return outcome

        logger.debug(f"Registered flow type: {type_name}")
        self._executors[type_name] = boxed_executor

    def get_executor(self, flow_type: str) -> Callable | None:
        """
        Gets an executor for a flow type.

        **Rust Reference**: lines 353-359

        Returns:
            Executor function if registered, None otherwise
        """
        return self._executors.get(flow_type)

    def __len__(self) -> int:
        """Returns the number of registered flow types."""
        return len(self._executors)

    def is_empty(self) -> bool:
        """Returns True if no flow types are registered."""
        return len(self._executors) == 0


class Worker:
    """
    Worker that polls and executes flows from distributed queue.

    Design Patterns:
    - Template Method: _run() defines fixed algorithm skeleton
    - Strategy: Flow handlers are interchangeable strategies
    - Builder: with_timers(), with_poll_interval() for configuration

    From Dave Cheney: "Design APIs for their default use case"
    Default configuration works out of the box, but customizable.

    Usage:
        storage = SqliteExecutionLog("workflow.db")
        await storage.connect()

        worker = Worker(storage, "worker-1") \\
            .with_timers() \\
            .with_poll_interval(1.0)

        # Register flow handlers
        await worker.register(lambda flow: flow.process_order())

        # Start worker
        handle = await worker.start()

        # ... let it run ...

        # Shutdown gracefully
        await handle.shutdown()
    """

    def __init__(
        self,
        storage: ExecutionLog,
        worker_id: str,
        enable_timers: bool = False,
        poll_interval: float = 1.0,
        timer_interval: float = 0.1,
        max_retries: int = 3,
        backoff_base: float = 2.0,
    ):
        """
        Initialize worker with storage backend.

        From Dave Cheney: "Avoid package level state"
        All dependencies passed explicitly, no globals.

        Args:
            storage: Storage backend for persistence
            worker_id: Unique worker identifier
            enable_timers: Whether to process timers
            poll_interval: Seconds between queue polls
            timer_interval: Seconds between timer checks
            max_retries: Maximum retry attempts before marking as FAILED
            backoff_base: Base for exponential backoff (delay = base^retry_count)
        """
        self._storage = storage
        self._worker_id = worker_id
        self._enable_timers = enable_timers
        self._poll_interval = poll_interval
        self._timer_interval = timer_interval
        self._max_retries = max_retries
        self._backoff_base = backoff_base

        # Add jitter to poll interval to avoid thundering herd
        # (all workers waking simultaneously and hammering the database)
        # **Rust Reference**: `src/executor/worker.rs` lines 820-828
        worker_hash = sum(ord(c) for c in worker_id)
        jitter_ms = 1 + (worker_hash % 5)  # 1-5ms jitter
        self._poll_interval_with_jitter = poll_interval + (jitter_ms / 1000.0)

        # Signal processing configuration
        self._signal_source: Any | None = None  # SignalSource protocol
        self._signal_poll_interval: float = 0.5  # 500ms default

        # Registry pattern: Flow type → executor function (mirrors Rust)
        # **Rust Reference**: Worker has Arc<RwLock<Registry<S>>>
        self._registry: Registry[ExecutionLog] = Registry()

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()
        self._running = False

        # Event-driven notification support (runtime protocol checking)
        # From Rust: Worker requires WorkNotificationSource at compile time via trait bounds
        # Python: Runtime isinstance() check provides flexibility
        self._supports_work_notifications = isinstance(storage, WorkNotificationSource)
        self._supports_timer_notifications = isinstance(storage, TimerNotificationSource)

        if self._supports_work_notifications:
            self._work_notify = storage.work_notify()
            logger.debug(f"Worker {worker_id}: Event-driven work notifications enabled")
        else:
            self._work_notify = None
            logger.debug(f"Worker {worker_id}: Polling-based work detection (no notifications)")

        if self._supports_timer_notifications:
            self._timer_notify = storage.timer_notify()
            logger.debug(f"Worker {worker_id}: Event-driven timer notifications enabled")
        else:
            self._timer_notify = None
            logger.debug(f"Worker {worker_id}: Polling-based timer detection (no notifications)")

    # ====================================================================
    # Builder Pattern - Fluent Configuration
    # ====================================================================

    def with_timers(self, interval: float = 0.1) -> "Worker":
        """
        Enable timer processing (builder pattern).

        From Software Design Patterns (Chapter 7):
        Builder pattern allows fluent configuration:
        worker.with_timers().with_poll_interval(2.0)

        Args:
            interval: Seconds between timer checks

        Returns:
            self for method chaining
        """
        self._enable_timers = True
        self._timer_interval = interval
        return self

    def with_poll_interval(self, interval: float) -> "Worker":
        """
        Configure polling interval (builder pattern).

        From Dave Cheney: "Design APIs for their default use case"
        Default 1.0 second works for most cases, but allow customization.

        Args:
            interval: Seconds between queue polls

        Returns:
            self for method chaining
        """
        self._poll_interval = interval
        # Recalculate jitter when poll interval changes
        worker_hash = sum(ord(c) for c in self._worker_id)
        jitter_ms = 1 + (worker_hash % 5)
        self._poll_interval_with_jitter = interval + (jitter_ms / 1000.0)
        return self

    def with_signals(self, signal_source: Any, poll_interval: float = 0.5) -> "Worker":
        """
        Enable external signal processing (builder pattern).

        **Rust Reference**: `src/executor/worker.rs` lines 567-586

        Enables the worker to poll the signal source and deliver signals
        to waiting flows automatically.

        Example:
            signal_source = SimulatedUserInputSource()
            worker = Worker(storage, "worker-1").with_signals(signal_source)

        Args:
            signal_source: SignalSource protocol implementation to poll
            poll_interval: Seconds between signal polls (default: 0.5)

        Returns:
            self for method chaining
        """
        self._signal_source = signal_source
        self._signal_poll_interval = poll_interval
        return self

    # ====================================================================
    # Strategy Pattern - Handler Registration
    # ====================================================================

    async def register(
        self, flow_class: type, executor: Callable[[Any], Awaitable[Any]] | None = None
    ) -> None:
        """
        Register a flow type with its executor function.

        **Rust Reference**: `src/executor/worker.rs` lines 742-757

        This mirrors the Rust Worker::register() which delegates to Registry::register().

        In Rust, #[flow] macro marks the entry point method. In Python, we auto-detect
        the method decorated with @flow.

        Args:
            flow_class: The flow type class to register
            executor: Optional lambda that specifies which method to call.
                     If None, automatically finds the @flow decorated method.

        Example:
            # Auto-detect @flow method (recommended)
            await worker.register(DataPipeline)

            # Or explicit method specification
            await worker.register(HolidaySaga, lambda saga: saga.run_saga())
        """
        # Auto-detect @flow method if no executor provided
        if executor is None:
            # Find method decorated with @flow
            flow_method = None
            for name in dir(flow_class):
                attr = getattr(flow_class, name)
                if callable(attr) and hasattr(attr, "_is_ergon_flow_method"):
                    flow_method = name
                    break

            if flow_method:
                # Found @flow method - use it
                def executor(flow, method=flow_method):
                    return getattr(flow, method)()
            else:
                # Fallback to 'run' method for backward compatibility
                def executor(flow):
                    return flow.run()

        # Delegate to registry (mirrors Rust pattern)
        self._registry.register(flow_class, executor)

    # ====================================================================
    # Main Loop - Template Method Pattern
    # ====================================================================

    async def start(self) -> "WorkerHandle":
        """
        Start the worker main loop.

        From Dave Cheney: "Leave concurrency to the caller"
        Returns WorkerHandle immediately, letting caller decide
        whether to await or run concurrently.

        Returns:
            WorkerHandle for shutdown control
        """
        self._running = True
        task = asyncio.create_task(self._run())
        return WorkerHandle(self, task)

    async def _run(self) -> None:
        """
        Main worker loop with event-driven timer processing.

        **Rust Reference**: `src/executor/worker.rs` lines 880-963

        From Software Design Patterns (Chapter 8):
        Template Method Pattern - fixed algorithm skeleton with variation points.

        Event-Driven Design:
        Uses asyncio.wait_for() to sleep until next timer expires OR timer_notify signals.
        This matches Rust's tokio::select! pattern for efficient timer processing.

        Loop structure:
        1. Process signals (if enabled)
        2. Process flows from queue (event-driven via work_notify)
        3. If got flow, loop immediately
        4. If no flow, wait on timer (event-driven via timer_notify)
        """
        logger.info(f"Worker {self._worker_id} started")

        try:
            while self._running and not self._shutdown_event.is_set():
                try:
                    logger.debug(f"Worker {self._worker_id}: Loop iteration starting")

                    # HOOK 1: Process external signals (if enabled)
                    # From Rust: signal.rs process_signals()
                    if self._signal_source is not None:
                        await self._process_signals()

                    # HOOK 2: Process one flow from queue
                    # **Rust Pattern**: After successfully dequeuing, immediately loop again!
                    # Only wait when queue is empty (got_flow=False)
                    got_flow = await self._process_flow()
                    logger.debug(
                        f"Worker {self._worker_id}: _process_flow() returned got_flow={got_flow}"
                    )

                    # If we got a flow, immediately loop to check for more work!
                    # From Rust lines 867-874: background task only waits when !got_task
                    if got_flow:
                        logger.debug(
                            f"Worker {self._worker_id}: Got flow, looping immediately for more work"
                        )
                        continue  # Skip waiting, go directly to next iteration

                    # HOOK 3: Event-driven timer wait (if timers enabled)
                    # **Rust Reference**: lines 880-963
                    # No flow found - wait for timer event or timeout
                    if self._enable_timers:
                        await self._wait_for_timer()

                except Exception as e:
                    logger.error(f"Worker {self._worker_id} error: {e}")
                    # Continue running despite errors

            logger.info(
                f"Worker {self._worker_id}: Exiting main loop "
                f"(running={self._running}, shutdown={self._shutdown_event.is_set()})"
            )
        finally:
            logger.info(f"Worker {self._worker_id} stopped")

    # ====================================================================
    # Hooks - Variation Points
    # ====================================================================

    async def _wait_for_timer(self) -> None:
        """
        Event-driven timer wait - sleeps until next timer fires OR new timer scheduled.

        **Rust Reference**: `src/executor/worker.rs` lines 889-963

        Rust uses tokio::select! to wait on:
        1. timer_sleep - sleeps until next_timer_wake
        2. timer_notified - wakes when new timer is scheduled

        Python equivalent uses asyncio.wait_for():
        - Timeout = time until next timer fires
        - Event = timer_notify (signals new timer scheduled)

        This is more efficient than polling - worker only wakes when needed.
        """
        try:
            if self._supports_timer_notifications:
                # Event-driven path (like Rust tokio::select!)
                # **Rust Reference**: lines 889-963
                next_fire = await self._storage.get_next_timer_fire_time()

                if next_fire is None:
                    # No timers - sleep forever until notified
                    # **Rust Reference**: line 895 (std::future::pending())
                    logger.debug(f"Worker {self._worker_id}: No timers, waiting for notification")
                    try:
                        await self._timer_notify.wait()
                        self._timer_notify.clear()
                        logger.debug(f"Worker {self._worker_id}: Timer notification received")
                    except Exception:
                        pass  # Shutdown or error
                    return

                # Calculate timeout until next timer fires
                now = datetime.now()
                if next_fire <= now:
                    # Timer already expired - process immediately
                    # **Rust Reference**: lines 950-951
                    logger.debug(f"Worker {self._worker_id}: Timer already expired, processing")
                    await self._process_timers()
                    return

                # Sleep until timer fires OR notification
                # **Rust Reference**: lines 890-896, 900, 902
                timeout_seconds = (next_fire - now).total_seconds()
                logger.debug(
                    f"Worker {self._worker_id}: "
                    f"Waiting {timeout_seconds:.2f}s for timer or notification"
                )

                try:
                    # Wait for either:
                    # 1. Timeout (timer expired) - like Rust timer_sleep future
                    # 2. Event (new timer) - like Rust timer_notified future
                    await asyncio.wait_for(self._timer_notify.wait(), timeout=timeout_seconds)
                    self._timer_notify.clear()
                    logger.debug(
                        f"Worker {self._worker_id}: "
                        "Woke from timer notification (new timer scheduled)"
                    )
                except TimeoutError:
                    # Timeout - timer expired
                    # **Rust Reference**: line 934 (timer_sleep matched)
                    logger.debug(
                        f"Worker {self._worker_id}: Timer expired after {timeout_seconds:.2f}s"
                    )

                # Process any expired timers
                # **Rust Reference**: line 939
                await self._process_timers()

            else:
                # Polling mode - no timer notifications available
                # Fall back to periodic polling with timer_interval
                await asyncio.sleep(self._timer_interval)
                await self._process_timers()

        except Exception as e:
            logger.error(f"Worker {self._worker_id} timer wait error: {e}")

    async def _process_timers(self) -> None:
        """
        Process expired timers (HOOK for Template Method).

        **Rust Reference**: `src/executor/timer.rs` lines 59-148

        This method:
        1. Gets expired timers from storage
        2. Claims each timer atomically (optimistic concurrency)
        3. Stores suspension result for timer (marks it as fired)
        4. Resumes the suspended flow

        **Distributed**: No in-memory notification - all state in database.
        Timers work across multiple processes and machines.
        """
        now = datetime.now()

        try:
            expired = await self._storage.get_expired_timers(now)

            if expired:
                logger.debug(f"Processing {len(expired)} expired timers")

            for timer_info in expired:
                # **Rust Reference**: storage/mod.rs lines 66-71 (TimerInfo struct)
                flow_id = timer_info.flow_id
                step = timer_info.step
                timer_name = timer_info.timer_name or ""

                # Optimistic concurrency: try to claim timer
                # **Rust Reference**: line 74
                claimed = await self._storage.claim_timer(flow_id, step)

                if claimed:
                    logger.info(f"Timer fired: flow={flow_id} step={step} name={timer_name!r}")

                    # Store timer result (marks timer as fired)
                    # **Rust Reference**: lines 82-112
                    # Use same payload structure as signals/child flows
                    payload = {
                        "success": True,
                        "data": b"",  # Empty - timer doesn't carry data
                        "is_retryable": None,
                    }
                    result_bytes = pickle.dumps(payload)

                    try:
                        await self._storage.store_suspension_result(
                            flow_id, step, timer_name, result_bytes
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to store timer result (will still resume): "
                            f"flow={flow_id} step={step} key={timer_name!r} error={e}"
                        )

                    # Resume the flow by re-enqueuing it
                    # **Rust Reference**: lines 115-125
                    try:
                        resumed = await self._storage.resume_flow(flow_id)
                        if resumed:
                            logger.debug(f"Resumed flow after timer: {flow_id}")
                        else:
                            logger.debug(
                                f"Flow {flow_id} not in SUSPENDED state after timer "
                                "(may have already resumed)"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to resume flow after timer: "
                            f"flow={flow_id} step={step} error={e}"
                        )

                else:
                    # Another worker already claimed it
                    # **Rust Reference**: lines 127-132
                    logger.debug(
                        f"Timer already fired by another worker: flow={flow_id} step={step}"
                    )

        except Exception as e:
            logger.error(f"Timer processing error: {e}")

    async def _process_signals(self) -> None:
        """
        Process external signals (HOOK for Template Method).

        **Rust Reference**: `src/executor/signal.rs` lines 75-138

        This method:
        1. Gets all flows waiting for signals
        2. For each waiting flow, polls the signal source
        3. If signal exists, stores it in storage and resumes flow
        4. Consumes signal from source to prevent reprocessing
        """
        try:
            # Get all flows waiting for signals
            waiting_signals = await self._storage.get_waiting_signals()

            if waiting_signals:
                logger.debug(f"Processing {len(waiting_signals)} waiting signals")

            for signal_info in waiting_signals:
                if signal_info.signal_name is None:
                    continue

                # Check if signal source has the signal
                signal_data = await self._signal_source.poll_for_signal(signal_info.signal_name)

                if signal_data is not None:
                    try:
                        # Store signal result
                        await self._storage.store_suspension_result(
                            signal_info.flow_id,
                            signal_info.step,
                            signal_info.signal_name,
                            signal_data,
                        )

                        # Consume signal so we don't re-process
                        await self._signal_source.consume_signal(signal_info.signal_name)

                        # Try to resume flow
                        resumed = await self._storage.resume_flow(signal_info.flow_id)

                        if resumed:
                            logger.debug(
                                f"Signal '{signal_info.signal_name}' delivered to "
                                f"flow {signal_info.flow_id}"
                            )
                        else:
                            logger.debug(
                                f"Signal '{signal_info.signal_name}' stored for "
                                f"flow {signal_info.flow_id} "
                                "(will resume when suspended)"
                            )

                    except Exception as e:
                        logger.warning(
                            f"Failed to store signal params for '{signal_info.signal_name}': {e}"
                        )

        except Exception as e:
            logger.error(f"Signal processing error: {e}")

    async def _process_flow(self) -> bool:
        """
        Process one flow from queue with event-driven wakeup (HOOK for Template Method).

        **Rust Reference**: `src/executor/worker.rs` lines 830-878

        From Rust ergon: Background dequeue task loops continuously,
        waiting on notification when queue is empty.

        From Dave Cheney: "Keep yourself busy or do the work yourself"
        Spawns background task so worker continues polling while flow executes.

        **Event-Driven Design**:
        Loops until a flow is claimed. When queue is empty, waits on work_notify
        event with timeout, then immediately tries again (no sleep).

        Returns:
            True if a flow was dequeued and spawned, False if queue is empty
        """
        # Loop until we claim a flow (matches Rust background dequeue task behavior)
        while True:
            try:
                logger.debug(f"Worker {self._worker_id}: Attempting to dequeue flow")
                scheduled_flow = await self._storage.dequeue_flow(self._worker_id)

                if scheduled_flow is None:
                    logger.debug(f"Worker {self._worker_id}: Queue empty, waiting for notification")
                    # Queue empty - wait for notification with timeout (hybrid approach)
                    # From Rust lines 867-873
                    if self._supports_work_notifications:
                        # Event-driven: wait for notification with timeout
                        # This wakes immediately when work is enqueued (fast path)
                        # or after poll_interval + jitter (slow path/safety net)
                        # Jitter prevents thundering herd (all workers polling simultaneously)
                        try:
                            await asyncio.wait_for(
                                self._work_notify.wait(), timeout=self._poll_interval_with_jitter
                            )
                            # Clear the event after waking up so we wait again next time
                            self._work_notify.clear()
                            logger.debug(
                                f"Worker {self._worker_id}: "
                                "Woke from notification, retrying dequeue"
                            )
                            # Loop back and try to dequeue again immediately!
                            continue
                        except TimeoutError:
                            # Timeout - return False (no flow processed)
                            # Main loop will check timers/signals then loop again
                            logger.debug(
                                f"Worker {self._worker_id}: Notification timeout, no flow found"
                            )
                            return False
                    # else: polling mode - return False
                    return False

                # Got a flow! Spawn background task (non-blocking) and return True
                asyncio.create_task(self._execute_flow(scheduled_flow))

                logger.debug(
                    f"Worker {self._worker_id} claimed flow: "
                    f"task_id={scheduled_flow.task_id}, "
                    f"flow_type={scheduled_flow.flow_type}"
                )

                # Return True to signal main loop to immediately try again (Rust pattern!)
                # From Rust: after successful dequeue, background task loops immediately
                return True

            except Exception as e:
                logger.error(f"Flow dequeue error: {e}")
                # On error, return False
                return False

    async def _execute_flow(self, scheduled_flow: ScheduledFlow) -> None:
        """
        Execute a flow using registry and handle FlowOutcome.

        **Rust Reference**: `src/executor/worker.rs` lines 1022-1057

        This method mirrors Rust's registry-based dispatch:
        1. Get executor from registry (by flow_type)
        2. If no executor, return error
        3. Call executor with (flow_data, flow_id, storage, parent_metadata)
        4. Handle outcome (Suspended/Completed)

        From Dave Cheney: "Only handle an error once"
        Error handling is delegated to execution.handle_flow_error().
        """
        try:
            # Get executor from registry (Rust lines 1026-1029)
            executor = self._registry.get_executor(scheduled_flow.flow_type)

            # Prepare parent metadata (Rust lines 1031-1032)
            parent_metadata = scheduled_flow.parent_metadata

            if executor is None:
                # No executor registered - treat as error (Rust lines 1038-1042)
                error = WorkerError(
                    f"No executor registered for flow type: {scheduled_flow.flow_type}. "
                    f"Did you forget to call worker.register()?"
                )
                await handle_flow_error(
                    storage=self._storage,
                    worker_id=self._worker_id,
                    flow=scheduled_flow,
                    flow_task_id=scheduled_flow.task_id,
                    error=error,
                    parent_metadata=parent_metadata,
                )
                return

            logger.debug(f"Executing flow type: {scheduled_flow.flow_type}")

            # Execute via registry (Rust line 1036)
            # Executor signature: (flow_data, flow_id, storage, parent_metadata) -> FlowOutcome
            outcome = await executor(
                scheduled_flow.flow_data, scheduled_flow.flow_id, self._storage, parent_metadata
            )

            # Handle outcome
            # From Rust lines 982-994
            if isinstance(outcome, Suspended):
                # Flow suspended for timer or signal
                await handle_suspended_flow(
                    storage=self._storage,
                    worker_id=self._worker_id,
                    flow_task_id=scheduled_flow.task_id,
                    flow_id=scheduled_flow.flow_id,
                    reason=outcome.reason,
                )

            elif isinstance(outcome, Completed):
                # Flow completed (success or error)
                if outcome.is_success():
                    # Success
                    await handle_flow_completion(
                        storage=self._storage,
                        worker_id=self._worker_id,
                        flow_task_id=scheduled_flow.task_id,
                        flow_id=scheduled_flow.flow_id,
                        parent_metadata=scheduled_flow.parent_metadata,
                    )
                else:
                    # Error - result is an Exception
                    await handle_flow_error(
                        storage=self._storage,
                        worker_id=self._worker_id,
                        flow=scheduled_flow,
                        flow_task_id=scheduled_flow.task_id,
                        error=outcome.result,  # The exception
                        parent_metadata=scheduled_flow.parent_metadata,
                    )

        except Exception as e:
            # Unexpected error during flow execution
            logger.error(
                f"Worker {self._worker_id} unexpected error: "
                f"task_id={scheduled_flow.task_id}, error={e}"
            )

            # Treat as flow error
            await handle_flow_error(
                storage=self._storage,
                worker_id=self._worker_id,
                flow=scheduled_flow,
                flow_task_id=scheduled_flow.task_id,
                error=e,
                parent_metadata=scheduled_flow.parent_metadata,
            )

    # ====================================================================
    # Shutdown
    # ====================================================================

    async def shutdown(self) -> None:
        """
        Gracefully shutdown the worker.

        From Dave Cheney: "Never start a goroutine without knowing when
        it will stop" - explicit shutdown, not relying on GC.
        """
        logger.info(f"Worker {self._worker_id} shutting down...")
        self._running = False
        self._shutdown_event.set()


class WorkerHandle:
    """
    Handle for controlling a running worker.

    From Software Design Patterns (Chapter 7):
    Composition - handle HAS-A worker, not IS-A worker.

    Usage:
        handle = await worker.start()
        await handle.shutdown()
    """

    def __init__(self, worker: Worker, task: asyncio.Task):
        """
        Initialize handle.

        From Dave Cheney: "Choose identifiers for clarity"
        worker and task are clear; w and t would be cryptic.
        """
        self._worker = worker
        self._task = task

    async def shutdown(self) -> None:
        """
        Shutdown worker and wait for completion.

        From Dave Cheney: "Return early rather than nesting deeply"
        Simple sequence: shutdown → wait → done.
        """
        await self._worker.shutdown()
        await self._task

        logger.info("Worker handle closed")


class WorkerError(Exception):
    """
    Worker operation failed.

    From Dave Cheney: "Errors are values"
    Custom exception with context.
    """

    pass
