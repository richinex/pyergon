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

            # Convert UUID to string for Executor
            exec = Executor(flow=flow_instance, storage=storage, flow_id=str(flow_id))
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

        # Dequeue channel (bounded to 1) to prevent work hogging
        # Worker must consume before next dequeue happens
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
        """Main worker loop using asyncio.wait with FIRST_COMPLETED (mirrors Rust tokio::select!).

        Concurrently waits on multiple event sources:
        1. Shutdown signal
        2. Flow dequeue (from background task via bounded queue)
        3. Timer sleep (sleeps until next timer fires)
        4. Timer notification (wakes when new timer scheduled)
        5. Signal processing (periodic, if enabled)

        Whichever completes first is handled, then loop repeats.
        This matches Rust's tokio::select! pattern for true event-driven execution.
        """
        logger.info(f"Worker {self._worker_id} started")

        # Start background dequeue task
        self._dequeue_task = asyncio.create_task(self._background_dequeue_loop())

        # Track next timer wake time (like Rust)
        next_timer_wake: datetime | None = None

        try:
            while self._running and not self._shutdown_event.is_set():
                try:
                    logger.debug(f"Worker {self._worker_id}: Loop iteration starting")

                    # Build list of concurrent tasks to wait on
                    pending_tasks = {}

                    # 0. Shutdown event - always wait for shutdown signal
                    shutdown_task = asyncio.create_task(self._shutdown_event.wait())
                    pending_tasks["shutdown"] = shutdown_task

                    # 1. Dequeue task - always wait for flows
                    dequeue_task = asyncio.create_task(self._dequeue_queue.get())
                    pending_tasks["dequeue"] = dequeue_task

                    # 2. Maintenance: Move ready delayed tasks (for retries)
                    delayed_task = asyncio.create_task(asyncio.sleep(1.0))  # Check every 1s
                    pending_tasks["delayed_tasks"] = delayed_task

                    # 3. Timer sleep task - if timers enabled
                    if self._enable_timers:
                        timer_sleep_task = asyncio.create_task(
                            self._create_timer_sleep(next_timer_wake)
                        )
                        pending_tasks["timer_sleep"] = timer_sleep_task

                        # 4. Timer notification - wake when new timer scheduled
                        if self._supports_timer_notifications:
                            timer_notify_task = asyncio.create_task(self._timer_notify.wait())
                            pending_tasks["timer_notify"] = timer_notify_task

                    # 5. Signal processing - if enabled (periodic check)
                    if self._signal_source is not None:
                        signal_task = asyncio.create_task(asyncio.sleep(self._signal_poll_interval))
                        pending_tasks["signal"] = signal_task

                    # Wait for FIRST task to complete (like tokio::select!)
                    done, pending = await asyncio.wait(
                        pending_tasks.values(), return_when=asyncio.FIRST_COMPLETED
                    )

                    # Cancel all pending tasks
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                    # Handle the completed task
                    for completed_task in done:
                        # Find which task completed
                        task_name = None
                        for name, task in pending_tasks.items():
                            if task == completed_task:
                                task_name = name
                                break

                        # Check if task raised an exception
                        try:
                            # Get result (will raise if task failed)
                            result = completed_task.result()
                        except Exception as task_error:
                            logger.error(
                                f"Worker {self._worker_id}: Task '{task_name}' failed: {task_error}"
                            )
                            # Skip this iteration and try again
                            continue

                        if task_name == "shutdown":
                            # Shutdown signal received - exit loop
                            logger.debug(f"Worker {self._worker_id}: Shutdown signal received")
                            break

                        elif task_name == "dequeue":
                            # Got a flow - execute it
                            scheduled_flow = result
                            self._dequeue_queue.task_done()

                            # Execute flow in background
                            task = asyncio.create_task(self._execute_flow(scheduled_flow))
                            self._background_tasks.add(task)
                            task.add_done_callback(self._background_tasks.discard)

                            logger.debug(
                                f"Worker {self._worker_id} claimed flow: "
                                f"task_id={scheduled_flow.task_id}"
                            )

                        elif task_name == "delayed_tasks":
                            # Maintenance: Move ready delayed tasks (retries) to ready queue
                            try:
                                count = await self._storage.move_ready_delayed_tasks()
                                if count > 0:
                                    logger.debug(
                                        f"Worker {self._worker_id} moved {count} delayed tasks "
                                        "to ready queue"
                                    )
                            except Exception as e:
                                logger.warning(
                                    f"Worker {self._worker_id} failed to move delayed tasks: {e}"
                                )

                        elif task_name == "timer_sleep":
                            # Timer fired - process expired timers
                            await self._process_timers()

                            # Recalculate next wake time
                            next_timer_wake = await self._calculate_next_timer_wake()

                        elif task_name == "timer_notify":
                            # Timer notification - recalculate next wake time
                            self._timer_notify.clear()
                            next_timer_wake = await self._calculate_next_timer_wake()
                            logger.debug(f"Worker {self._worker_id}: Timer notification received")

                        elif task_name == "signal":
                            # Process signals
                            await self._process_signals()

                except Exception as e:
                    logger.error(f"Worker {self._worker_id} error: {e}")

            logger.info(
                f"Worker {self._worker_id}: Exiting main loop "
                f"(running={self._running}, shutdown={self._shutdown_event.is_set()})"
            )
        finally:
            logger.info(f"Worker {self._worker_id} stopped")

    async def _create_timer_sleep(self, next_timer_wake: datetime | None) -> None:
        """Create timer sleep future (mirrors Rust timer_sleep).

        If next_timer_wake is None, sleeps forever (like std::future::pending()).
        Otherwise, sleeps until the specified time.
        """
        if next_timer_wake is None:
            # No timers - sleep forever (will be cancelled when timer notification arrives)
            # This matches Rust's std::future::pending()
            await asyncio.sleep(float('inf'))
        else:
            # Calculate sleep duration
            now = datetime.now()
            if next_timer_wake <= now:
                # Timer already expired - return immediately
                return
            else:
                # Sleep until timer fires
                sleep_duration = (next_timer_wake - now).total_seconds()
                await asyncio.sleep(sleep_duration)

    async def _calculate_next_timer_wake(self) -> datetime | None:
        """Calculate next timer wake time (mirrors Rust timer wake calculation).

        Returns:
            datetime when next timer should fire, or None if no timers
        """
        try:
            return await self._storage.get_next_timer_fire_time()
        except Exception as e:
            logger.warning(f"Worker {self._worker_id}: Failed to get next timer: {e}")
            return None

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

                    # Use SuspensionPayload dataclass
                    # Store success=true with empty data to indicate timer fired
                    # The step will execute and store its actual result
                    from pyergon.executor.suspension_payload import SuspensionPayload

                    payload = SuspensionPayload(
                        success=True,
                        data=b"",  # Empty - timer doesn't carry data, just marks delay completion
                        is_retryable=None,
                    )
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
