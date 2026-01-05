"""Distributed worker for polling and executing flows.

Design: Template Method Pattern
Main loop defines the algorithm skeleton:
1. Process timers (if enabled)
2. Process flows
3. Sleep before next poll

Design: Strategy Pattern
Flow handlers are strategies - different execution strategies for
different flow types. Registered via register() method, executed
polymorphically.

Features:
- Polls storage queue for pending flows
- Non-blocking flow execution in background
- Processes expired timers (if enabled)
- Retry logic with exponential backoff
- Graceful shutdown via WorkerHandle
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
    """Registry mapping flow type names to their executors.

    Stores executor functions that can deserialize and execute flows
    based on their type name. This allows the worker to handle different
    flow types dynamically.

    Example:
        ```python
        registry = Registry()

        # Register with explicit method reference
        registry.register(HolidaySaga, lambda saga: saga.run_saga())

        # Or register with default 'run' method
        registry.register(OrderFlow, lambda flow: flow.run())
        ```
    """

    def __init__(self):
        """Create a new empty flow registry."""
        self._executors: dict[str, Callable] = {}

    def register(self, flow_class: type, executor: Callable[[Any], Awaitable[Any]]) -> None:
        """Register a flow type with its executor function.

        The executor function receives the deserialized flow instance
        and should return a future that produces the flow's result.

        Args:
            flow_class: The flow type class (must have FlowType protocol)
            executor: Function that takes flow instance and returns awaitable result

        Example:
            registry.register(HolidaySaga, lambda saga: saga.run_saga())
            registry.register(OrderFlow, lambda flow: flow.process())
        """
        if hasattr(flow_class, "type_id"):
            type_name = flow_class.type_id()
        else:
            type_name = flow_class.__name__

        async def boxed_executor(
            flow_data: bytes, flow_id: UUID, storage: S, parent_metadata: tuple | None = None
        ):
            """Boxed executor for flow execution.

            Deserializes the flow from bytes, creates an Executor,
            executes the user's closure, and returns the outcome.
            """
            try:
                flow_instance = pickle.loads(flow_data)
            except Exception as e:
                return Completed(result=Exception(f"failed to deserialize flow: {e}"))

            exec = Executor(flow=flow_instance, storage=storage, flow_id=flow_id)
            outcome = await exec.run(lambda _: executor(flow_instance))
            return outcome

        logger.debug(f"Registered flow type: {type_name}")
        self._executors[type_name] = boxed_executor

    def get_executor(self, flow_type: str) -> Callable | None:
        """Get an executor for a flow type.

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
    """Worker that polls and executes flows from distributed queue.

    Design Patterns:
    - Template Method: _run() defines fixed algorithm skeleton
    - Strategy: Flow handlers are interchangeable strategies
    - Builder: with_timers(), with_poll_interval() for configuration

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
        """Initialize worker with storage backend.

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
        worker_hash = sum(ord(c) for c in worker_id)
        jitter_ms = 1 + (worker_hash % 5)
        self._poll_interval_with_jitter = poll_interval + (jitter_ms / 1000.0)

        self._signal_source: Any | None = None
        self._signal_poll_interval: float = 0.5

        self._registry: Registry[ExecutionLog] = Registry()

        self._shutdown_event = asyncio.Event()
        self._running = False

        # Track background tasks to prevent garbage collection
        # See: asyncio docs - "Save a reference to avoid task disappearing mid-execution"
        self._background_tasks: set[asyncio.Task] = set()

        # Dequeue channel (bounded to 1 like Rust) to prevent work hogging
        # Rust ref: worker.rs line 809-811
        self._dequeue_queue: asyncio.Queue[ScheduledFlow | None] = asyncio.Queue(maxsize=1)
        self._dequeue_task: asyncio.Task | None = None

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

    def with_timers(self, interval: float = 0.1) -> "Worker":
        """Enable timer processing (builder pattern).

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
        """Configure polling interval (builder pattern).

        Default 1.0 second works for most cases, but allow customization.

        Args:
            interval: Seconds between queue polls

        Returns:
            self for method chaining
        """
        self._poll_interval = interval
        worker_hash = sum(ord(c) for c in self._worker_id)
        jitter_ms = 1 + (worker_hash % 5)
        self._poll_interval_with_jitter = interval + (jitter_ms / 1000.0)
        return self

    def with_signals(self, signal_source: Any, poll_interval: float = 0.5) -> "Worker":
        """Enable external signal processing (builder pattern).

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

    async def register(
        self, flow_class: type, executor: Callable[[Any], Awaitable[Any]] | None = None
    ) -> None:
        """Register a flow type with its executor function.

        Auto-detects the method decorated with @flow if no executor is provided.

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
        if executor is None:
            flow_method = None
            for name in dir(flow_class):
                attr = getattr(flow_class, name)
                if callable(attr) and hasattr(attr, "_is_ergon_flow_method"):
                    flow_method = name
                    break

            if flow_method:

                def executor(flow, method=flow_method):
                    return getattr(flow, method)()
            else:
                # Fallback to 'run' method for backward compatibility
                def executor(flow):
                    return flow.run()

        self._registry.register(flow_class, executor)

    async def start(self) -> "WorkerHandle":
        """Start the worker main loop.

        Returns WorkerHandle immediately, letting caller decide
        whether to await or run concurrently.

        Returns:
            WorkerHandle for shutdown control
        """
        self._running = True
        task = asyncio.create_task(self._run())
        return WorkerHandle(self, task)

    async def _background_dequeue_loop(self) -> None:
        """Background task that continuously dequeues flows and puts them in bounded queue.

        Matches Rust's dedicated dequeue task (worker.rs lines 830-878).
        Uses bounded queue (maxsize=1) to prevent hogging - worker must consume
        before next dequeue happens.
        """
        while self._running and not self._shutdown_event.is_set():
            try:
                scheduled_flow = await self._storage.dequeue_flow(self._worker_id)

                if scheduled_flow is not None:
                    # Put in queue - blocks if queue is full (worker hasn't consumed)
                    await self._dequeue_queue.put(scheduled_flow)
                else:
                    # No flow available, wait for notification
                    if self._supports_work_notifications:
                        try:
                            await asyncio.wait_for(
                                self._work_notify.wait(),
                                timeout=self._poll_interval_with_jitter
                            )
                            self._work_notify.clear()
                        except TimeoutError:
                            pass

            except Exception as e:
                logger.error(f"Background dequeue error: {e}")
                await asyncio.sleep(0.1)

    async def _run(self) -> None:
        """Main worker loop with event-driven timer processing.

        Template Method Pattern - fixed algorithm skeleton with variation points.

        Event-Driven Design:
        Uses asyncio.wait_for() to sleep until next timer expires OR timer_notify signals.

        Loop structure:
        1. Process signals (if enabled)
        2. Process flows from queue (event-driven via work_notify)
        3. If got flow, loop immediately
        4. If no flow, wait on timer (event-driven via timer_notify)
        """
        logger.info(f"Worker {self._worker_id} started")

        # Start background dequeue task (Rust ref: worker.rs line 830)
        self._dequeue_task = asyncio.create_task(self._background_dequeue_loop())

        try:
            while self._running and not self._shutdown_event.is_set():
                try:
                    logger.debug(f"Worker {self._worker_id}: Loop iteration starting")

                    if self._signal_source is not None:
                        await self._process_signals()

                    # Try to get flow from bounded queue with timeout
                    # This matches Rust's dequeue_rx.recv() which always awaits
                    try:
                        scheduled_flow = await asyncio.wait_for(
                            self._dequeue_queue.get(),
                            timeout=self._poll_interval if not self._enable_timers else 0.01
                        )
                        self._dequeue_queue.task_done()

                        # Execute flow in background
                        task = asyncio.create_task(self._execute_flow(scheduled_flow))
                        self._background_tasks.add(task)
                        task.add_done_callback(self._background_tasks.discard)

                        logger.debug(
                            f"Worker {self._worker_id} claimed flow: "
                            f"task_id={scheduled_flow.task_id}"
                        )
                    except asyncio.TimeoutError:
                        # No flow available, wait on timer if enabled
                        if self._enable_timers:
                            await self._wait_for_timer()

                except Exception as e:
                    logger.error(f"Worker {self._worker_id} error: {e}")

            logger.info(
                f"Worker {self._worker_id}: Exiting main loop "
                f"(running={self._running}, shutdown={self._shutdown_event.is_set()})"
            )
        finally:
            logger.info(f"Worker {self._worker_id} stopped")

    async def _wait_for_timer(self) -> None:
        """Event-driven timer wait - sleeps until next timer fires OR new timer scheduled.

        Waits on:
        1. timer_sleep - sleeps until next_timer_wake
        2. timer_notified - wakes when new timer is scheduled

        Uses asyncio.wait_for():
        - Timeout = time until next timer fires
        - Event = timer_notify (signals new timer scheduled)

        This is more efficient than polling - worker only wakes when needed.
        """
        try:
            if self._supports_timer_notifications:
                next_fire = await self._storage.get_next_timer_fire_time()

                if next_fire is None:
                    logger.debug(f"Worker {self._worker_id}: No timers, waiting for notification")
                    try:
                        await self._timer_notify.wait()
                        self._timer_notify.clear()
                        logger.debug(f"Worker {self._worker_id}: Timer notification received")
                    except Exception:
                        pass
                    return

                now = datetime.now()
                if next_fire <= now:
                    logger.debug(f"Worker {self._worker_id}: Timer already expired, processing")
                    await self._process_timers()
                    return

                timeout_seconds = (next_fire - now).total_seconds()
                logger.debug(
                    f"Worker {self._worker_id}: "
                    f"Waiting {timeout_seconds:.2f}s for timer or notification"
                )

                try:
                    await asyncio.wait_for(self._timer_notify.wait(), timeout=timeout_seconds)
                    self._timer_notify.clear()
                    logger.debug(
                        f"Worker {self._worker_id}: "
                        "Woke from timer notification (new timer scheduled)"
                    )
                except TimeoutError:
                    logger.debug(
                        f"Worker {self._worker_id}: Timer expired after {timeout_seconds:.2f}s"
                    )

                await self._process_timers()

            else:
                # Polling mode fallback
                await asyncio.sleep(self._timer_interval)
                await self._process_timers()

        except Exception as e:
            logger.error(f"Worker {self._worker_id} timer wait error: {e}")

    async def _process_timers(self) -> None:
        """Process expired timers (HOOK for Template Method).

        This method:
        1. Gets expired timers from storage
        2. Claims each timer atomically (optimistic concurrency)
        3. Stores suspension result for timer (marks it as fired)
        4. Resumes the suspended flow

        Distributed: No in-memory notification - all state in database.
        Timers work across multiple processes and machines.
        """
        now = datetime.now()

        try:
            expired = await self._storage.get_expired_timers(now)

            if expired:
                logger.debug(f"Processing {len(expired)} expired timers")

            for timer_info in expired:
                flow_id = timer_info.flow_id
                step = timer_info.step
                timer_name = timer_info.timer_name or ""

                claimed = await self._storage.claim_timer(flow_id, step)

                if claimed:
                    logger.info(f"Timer fired: flow={flow_id} step={step} name={timer_name!r}")

                    payload = {
                        "success": True,
                        "data": b"",
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
                    logger.debug(
                        f"Timer already fired by another worker: flow={flow_id} step={step}"
                    )

        except Exception as e:
            logger.error(f"Timer processing error: {e}")

    async def _process_signals(self) -> None:
        """Process external signals (HOOK for Template Method).

        This method:
        1. Gets all flows waiting for signals
        2. For each waiting flow, polls the signal source
        3. If signal exists, stores it in storage and resumes flow
        4. Consumes signal from source to prevent reprocessing
        """
        try:
            waiting_signals = await self._storage.get_waiting_signals()

            if waiting_signals:
                logger.debug(f"Processing {len(waiting_signals)} waiting signals")

            for signal_info in waiting_signals:
                if signal_info.signal_name is None:
                    continue

                signal_data = await self._signal_source.poll_for_signal(signal_info.signal_name)

                if signal_data is not None:
                    try:
                        await self._storage.store_suspension_result(
                            signal_info.flow_id,
                            signal_info.step,
                            signal_info.signal_name,
                            signal_data,
                        )

                        # Consume signal to prevent reprocessing
                        await self._signal_source.consume_signal(signal_info.signal_name)

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
        """Process one flow from queue with event-driven wakeup (HOOK for Template Method).

        Background dequeue task loops continuously, waiting on notification when queue is empty.

        Spawns background task so worker continues polling while flow executes.

        Event-Driven Design:
        Loops until a flow is claimed. When queue is empty, waits on work_notify
        event with timeout, then immediately tries again (no sleep).

        Returns:
            True if a flow was dequeued and spawned, False if queue is empty
        """
        while True:
            try:
                logger.debug(f"Worker {self._worker_id}: Attempting to dequeue flow")
                scheduled_flow = await self._storage.dequeue_flow(self._worker_id)

                if scheduled_flow is None:
                    logger.debug(f"Worker {self._worker_id}: Queue empty, waiting for notification")
                    if self._supports_work_notifications:
                        # Event-driven: wait for notification with timeout
                        # Wakes immediately when work is enqueued (fast path)
                        # or after poll_interval + jitter (slow path/safety net)
                        # Jitter prevents thundering herd
                        try:
                            await asyncio.wait_for(
                                self._work_notify.wait(), timeout=self._poll_interval_with_jitter
                            )
                            self._work_notify.clear()
                            logger.debug(
                                f"Worker {self._worker_id}: "
                                "Woke from notification, retrying dequeue"
                            )
                            continue
                        except TimeoutError:
                            logger.debug(
                                f"Worker {self._worker_id}: Notification timeout, no flow found"
                            )
                            return False
                    return False

                # Create background task with strong reference to prevent GC
                # asyncio docs: "Save a reference to avoid task disappearing mid-execution"
                task = asyncio.create_task(self._execute_flow(scheduled_flow))
                self._background_tasks.add(task)
                # Auto-cleanup when task completes
                task.add_done_callback(self._background_tasks.discard)

                logger.debug(
                    f"Worker {self._worker_id} claimed flow: "
                    f"task_id={scheduled_flow.task_id}, "
                    f"flow_type={scheduled_flow.flow_type}"
                )

                return True

            except Exception as e:
                logger.error(f"Flow dequeue error: {e}")
                return False

    async def _execute_flow(self, scheduled_flow: ScheduledFlow) -> None:
        """Execute a flow using registry and handle FlowOutcome.

        This method:
        1. Get executor from registry (by flow_type)
        2. If no executor, return error
        3. Call executor with (flow_data, flow_id, storage, parent_metadata)
        4. Handle outcome (Suspended/Completed)

        Error handling is delegated to execution.handle_flow_error().
        """
        try:
            executor = self._registry.get_executor(scheduled_flow.flow_type)
            parent_metadata = scheduled_flow.parent_metadata

            if executor is None:
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

            outcome = await executor(
                scheduled_flow.flow_data, scheduled_flow.flow_id, self._storage, parent_metadata
            )

            if isinstance(outcome, Suspended):
                await handle_suspended_flow(
                    storage=self._storage,
                    worker_id=self._worker_id,
                    flow_task_id=scheduled_flow.task_id,
                    flow_id=scheduled_flow.flow_id,
                    reason=outcome.reason,
                )

            elif isinstance(outcome, Completed):
                if outcome.is_success():
                    await handle_flow_completion(
                        storage=self._storage,
                        worker_id=self._worker_id,
                        flow_task_id=scheduled_flow.task_id,
                        flow_id=scheduled_flow.flow_id,
                        parent_metadata=scheduled_flow.parent_metadata,
                    )
                else:
                    await handle_flow_error(
                        storage=self._storage,
                        worker_id=self._worker_id,
                        flow=scheduled_flow,
                        flow_task_id=scheduled_flow.task_id,
                        error=outcome.result,
                        parent_metadata=scheduled_flow.parent_metadata,
                    )

        except Exception as e:
            logger.error(
                f"Worker {self._worker_id} unexpected error: "
                f"task_id={scheduled_flow.task_id}, error={e}"
            )

            await handle_flow_error(
                storage=self._storage,
                worker_id=self._worker_id,
                flow=scheduled_flow,
                flow_task_id=scheduled_flow.task_id,
                error=e,
                parent_metadata=scheduled_flow.parent_metadata,
            )

    async def shutdown(self) -> None:
        """Gracefully shutdown the worker.

        Explicit shutdown, not relying on GC.
        Waits for all background tasks to complete.
        """
        logger.info(f"Worker {self._worker_id} shutting down...")
        self._running = False
        self._shutdown_event.set()

        # Stop background dequeue task
        if self._dequeue_task and not self._dequeue_task.done():
            self._dequeue_task.cancel()
            try:
                await self._dequeue_task
            except asyncio.CancelledError:
                pass

        # Wait for background flow execution tasks to complete
        if self._background_tasks:
            logger.info(
                f"Worker {self._worker_id}: Waiting for {len(self._background_tasks)} "
                "background tasks to complete..."
            )
            # Use return_exceptions=True to handle any task failures gracefully
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            logger.info(f"Worker {self._worker_id}: All background tasks completed")


class WorkerHandle:
    """Handle for controlling a running worker.

    Composition - handle HAS-A worker, not IS-A worker.

    Usage:
        handle = await worker.start()
        await handle.shutdown()
    """

    def __init__(self, worker: Worker, task: asyncio.Task):
        """Initialize handle.

        Args:
            worker: Worker instance to control
            task: Asyncio task running the worker loop
        """
        self._worker = worker
        self._task = task

    async def shutdown(self) -> None:
        """Shutdown worker and wait for completion.

        Simple sequence: shutdown → wait → done.
        """
        await self._worker.shutdown()
        await self._task

        logger.info("Worker handle closed")


class WorkerError(Exception):
    """Worker operation failed.

    Custom exception with context for worker errors.
    """

    pass
