"""
SQLite-backed storage implementation for pyergon.

Design Pattern: Adapter Pattern (Chapter 10)
SqliteExecutionLog adapts SQLite database to the ExecutionLog interface.

From Dave Cheney's Practical Go:
"Keep package main small as small as possible" - complex database logic
is isolated here, not scattered across the application.

From Software Design Patterns:
Like FootballData and VolleyballData adapting different data sources
to GameData interface, SqliteExecutionLog adapts SQLite to ExecutionLog.

Implementation follows Rust ergon's sqlite.rs with Python idioms:
- aiosqlite for async operations (instead of r2d2 connection pool)
- WAL mode for concurrent reads
- IMMEDIATE transactions for pessimistic locking
- Indexes on (status, created_at) for efficient queue queries
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite

from pyergon.models import (
    Invocation,
    InvocationStatus,
    RetryPolicy,
    ScheduledFlow,
    TaskStatus,
    TimerInfo,
)
from pyergon.storage.base import ExecutionLog, SignalInfo, StorageError


class SqliteExecutionLog(ExecutionLog):
    """
    SQLite-backed durable storage.

    Design Principles Applied:
    - Single Responsibility: Only handles persistence (doesn't execute logic)
    - Dependency Inversion: Implements ExecutionLog protocol
    - Interface Segregation: Only implements needed methods

    From Dave Cheney: "Make the zero value useful"
    After __init__, the instance is not yet usable. Call connect() first.
    This follows asyncio best practices (no async in __init__).

    Usage:
        storage = SqliteExecutionLog("workflow.db")
        await storage.connect()
        try:
            await storage.log_invocation_start(...)
        finally:
            await storage.close()
    """

    def __init__(self, db_path: str):
        """
        Initialize storage with notification support (connection not opened yet).

        **Rust Reference**: `src/storage/sqlite.rs` lines 65-76

        From Dave Cheney: "Choose identifiers for clarity, not brevity"
        db_path is clear; dbp or database_path would be worse.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()  # Serialize access to shared connection

        # Notification events (implements WorkNotificationSource and TimerNotificationSource)
        # From Rust: work_notify: Arc<Notify>, timer_notify: Arc<Notify>
        self._work_notify = asyncio.Event()
        self._timer_notify = asyncio.Event()
        self._status_notify = asyncio.Event()

    @classmethod
    async def in_memory(cls) -> SqliteExecutionLog:
        """
        Create an in-memory SQLite storage for testing.

        Convenience factory that creates storage with ":memory:" path
        and automatically calls connect().

        Returns:
            Connected in-memory storage instance

        Example:
            storage = await SqliteExecutionLog.in_memory()
            # Ready to use immediately
        """
        instance = cls(":memory:")
        await instance.connect()
        return instance

    def __repr__(self) -> str:
        """Return string representation of storage instance."""
        if self.db_path == ":memory:":
            return "SqliteExecutionLog(in-memory)"
        return f"SqliteExecutionLog({self.db_path})"

    async def connect(self) -> None:
        """
        Open database connection and initialize schema.

        **Rust Reference**: `src/storage/sqlite.rs` lines 75-98

        From Dave Cheney: "Return early rather than nesting deeply"
        Uses guard clauses to check preconditions.

        Pattern: Template Method
        Fixed initialization sequence:
        1. Open connection
        2. Enable WAL mode
        3. Create tables
        4. Create indexes
        """
        # Guard: Don't reconnect if already connected
        if self._connection is not None:
            return

        # Create database file if doesn't exist
        # From Rust: create_if_missing(true)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        # Open connection with timeout
        # From Rust: busy_timeout(Duration::from_secs(5))
        self._connection = await aiosqlite.connect(
            self.db_path,
            timeout=5.0,
            isolation_level=None,  # Autocommit mode for better concurrency
        )

        # Enable WAL mode for concurrent reads (from Rust ergon)
        # From Rust: journal_mode(SqliteJournalMode::Wal)
        # IMPORTANT: Must fetch the result, not just execute
        # Note: In-memory databases return "memory" and don't support WAL
        cursor = await self._connection.execute("PRAGMA journal_mode=WAL")
        result = await cursor.fetchone()
        await cursor.close()

        # Verify WAL mode was set (or accept "memory" for in-memory databases)
        if result:
            mode = result[0].upper()
            if mode not in ("WAL", "MEMORY"):
                raise StorageError(f"Failed to enable WAL mode, got: {result[0]}")

        # Set synchronous mode to NORMAL for better performance
        # From Rust: synchronous(SqliteSynchronous::Normal)
        await self._connection.execute("PRAGMA synchronous=NORMAL")

        # Set busy timeout
        await self._connection.execute("PRAGMA busy_timeout=5000")

        # Create schema
        await self._create_schema()

        # Commit changes (even though we're in autocommit mode)
        await self._connection.commit()

    async def _create_schema(self) -> None:
        """
        Create database tables and indexes.

        **Rust Reference**: `src/storage/sqlite.rs` lines 131-226

        EXACT MATCH: Schema matches Rust ergon's sqlite.rs precisely
        - execution_log table (not execution_log)
        - flow_queue table (not scheduled_flows)
        - UPPERCASE status values to match Rust
        - INTEGER timestamps (milliseconds) not TEXT
        """
        # execution_log table - tracks individual step executions
        # **Rust Reference**: lines 133-154
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS execution_log (
                id TEXT NOT NULL,
                step INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                class_name TEXT NOT NULL,
                method_name TEXT NOT NULL,
                delay INTEGER,
                status TEXT CHECK( status IN (
                    'PENDING','WAITING_FOR_SIGNAL','WAITING_FOR_TIMER','COMPLETE'
                ) ) NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1,
                parameters BLOB,
                params_hash INTEGER NOT NULL DEFAULT 0,
                return_value BLOB,
                retry_policy TEXT,
                is_retryable INTEGER,
                timer_fire_at INTEGER,
                timer_name TEXT,
                PRIMARY KEY (id, step)
            )
        """)

        # Create index for efficient flow lookups
        # **Rust Reference**: lines 157-159
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_execution_log_id
            ON execution_log(id)
        """)

        # Create index for incomplete flow queries
        # **Rust Reference**: lines 162-166
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_execution_log_status
            ON execution_log(step, status)
        """)

        # Create index for timer queries (find expired timers)
        # **Rust Reference**: lines 169-173
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_execution_log_timers
            ON execution_log(status, timer_fire_at)
        """)

        # flow_queue table - distributed work queue
        # **Rust Reference**: lines 176-195
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS flow_queue (
                task_id TEXT PRIMARY KEY,
                flow_id TEXT NOT NULL,
                flow_type TEXT NOT NULL,
                flow_data BLOB NOT NULL,
                status TEXT CHECK( status IN (
                    'PENDING','RUNNING','SUSPENDED','COMPLETE','FAILED'
                ) ) NOT NULL,
                parent_flow_id TEXT,
                signal_token TEXT,
                locked_by TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                scheduled_for INTEGER,
                version TEXT
            )
        """)

        # Create index for efficient pending flow lookups
        # **Rust Reference**: lines 198-202
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_flow_queue_status
            ON flow_queue(status, created_at)
        """)

        # suspension_params table - durable suspension results (signals and timers)
        # **Rust Reference**: lines 205-216
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS suspension_params (
                flow_id TEXT NOT NULL,
                step INTEGER NOT NULL,
                suspension_key TEXT NOT NULL,
                result BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (flow_id, step, suspension_key)
            )
        """)

        # Create index for suspension parameter cleanup
        # **Rust Reference**: lines 219-223
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_suspension_params_created
            ON suspension_params(created_at)
        """)

    # ========================================================================
    # Invocation Operations
    # ========================================================================

    async def log_invocation_start(
        self,
        flow_id: str,
        step: int,
        class_name: str,
        method_name: str,
        parameters: bytes,
        params_hash: int,
        delay: int | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Invocation:
        """
        Record the start of a step execution.

        RUST COMPLIANCE: Matches Rust ExecutionLog::log_invocation_start
        Updated to handle all new Invocation fields.

        From Dave Cheney: "Eliminate error handling by eliminating errors"
        Using REPLACE instead of INSERT handles duplicate starts gracefully
        (idempotent operation).
        """
        import json

        self._check_connected()

        # Get current timestamp in milliseconds (matches Rust line 373)
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        # Use flow_id as invocation id (matches Rust line 371: .bind(id.to_string()))
        invocation_id = flow_id

        # Serialize retry policy to JSON if present
        retry_policy_json = None
        if retry_policy is not None:
            retry_policy_json = json.dumps(
                {
                    "max_attempts": retry_policy.max_attempts,
                    "initial_delay_ms": retry_policy.initial_delay_ms,
                    "max_delay_ms": retry_policy.max_delay_ms,
                    "backoff_multiplier": retry_policy.backoff_multiplier,
                }
            )

        # Use INSERT with ON CONFLICT for idempotency (matches Rust line 365-369)
        await self._connection.execute(
            """
            INSERT INTO execution_log (
                id, step, timestamp, class_name, method_name, delay, status, attempts,
                parameters, params_hash, retry_policy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id, step)
            DO UPDATE SET attempts = attempts + 1
        """,
            (
                invocation_id,
                step,
                timestamp_ms,
                class_name,
                method_name,
                delay,
                InvocationStatus.PENDING.value,
                1,
                parameters,
                params_hash,
                retry_policy_json,
            ),
        )

        await self._connection.commit()

        # Return created invocation (note: Rust doesn't return, but Python code expects it)
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000.0)
        return Invocation(
            id=invocation_id,
            flow_id=flow_id,  # Redundant with id, but kept for compatibility
            step=step,
            class_name=class_name,
            method_name=method_name,
            parameters=parameters,
            params_hash=params_hash,
            return_value=None,
            status=InvocationStatus.PENDING,
            attempts=1,  # Matches INSERT value
            is_retryable=None,
            timestamp=timestamp_dt,
            updated_at=timestamp_dt,
            delay=delay,
            retry_policy=retry_policy,
            timer_fire_at=None,
            timer_name=None,
        )

    async def log_invocation_completion(
        self, flow_id: str, step: int, return_value: bytes
    ) -> Invocation:
        """
        Record the completion of a step execution.

        From Dave Cheney: "Only handle an error once"
        If invocation not found, raises StorageError. Caller handles it.
        """
        self._check_connected()

        # Update existing invocation (matches Rust line 399-408)
        cursor = await self._connection.execute(
            """
            UPDATE execution_log
            SET status = 'COMPLETE', return_value = ?
            WHERE id = ? AND step = ?
        """,
            (return_value, flow_id, step),
        )

        await self._connection.commit()

        # Check if update succeeded
        if cursor.rowcount == 0:
            raise StorageError(f"Invocation not found: flow_id={flow_id}, step={step}")

        # Fetch and return updated invocation
        invocation = await self.get_invocation(flow_id, step)
        if invocation is None:
            raise StorageError(
                f"Failed to retrieve updated invocation: flow_id={flow_id}, step={step}"
            )

        return invocation

    async def get_invocation(self, flow_id: str, step: int) -> Invocation | None:
        """
        Retrieve a specific step invocation.

        From Dave Cheney: "Make the zero value useful"
        Returns None when not found (not an error condition).
        """
        self._check_connected()

        # Matches Rust SELECT query (sqlite.rs:435-437)
        cursor = await self._connection.execute(
            """
            SELECT id, step, timestamp, class_name, method_name, status, attempts,
                   parameters, params_hash, return_value, delay, retry_policy,
                   is_retryable, timer_fire_at, timer_name
            FROM execution_log
            WHERE id = ? AND step = ?
        """,
            (flow_id, step),
        )

        row = await cursor.fetchone()

        if row is None:
            return None

        return self._row_to_invocation(row)

    async def get_incomplete_flows(self) -> list[Invocation]:
        """
        Get all flows with incomplete execution_log.

        From Dave Cheney: "Return early rather than nesting deeply"
        Simple query, early return if no results.
        """
        self._check_connected()

        # Get incomplete flows - step 0 is the flow entry point
        cursor = await self._connection.execute("""
            SELECT id, step, timestamp, class_name, method_name, status, attempts,
                   parameters, params_hash, return_value, delay, retry_policy,
                   is_retryable, timer_fire_at, timer_name
            FROM execution_log
            WHERE step = 0 AND status != 'COMPLETE'
        """)

        rows = await cursor.fetchall()

        return [self._row_to_invocation(row) for row in rows]

    async def has_non_retryable_error(self, flow_id: str) -> bool:
        """
        Check if flow has encountered a non-retryable error.

        From Dave Cheney: "Eliminate error handling by eliminating errors"
        Simple boolean return eliminates need for exception handling.

        **Rust Reference**: has_non_retryable_error (sqlite.rs:531-549)
        """
        self._check_connected()

        cursor = await self._connection.execute(
            """
            SELECT EXISTS(
                SELECT 1 FROM execution_log
                WHERE id = ? AND is_retryable = 0
            )
        """,
            (flow_id,),
        )

        row = await cursor.fetchone()
        return bool(row[0]) if row else False

    # ========================================================================
    # Queue Operations
    # ========================================================================

    async def enqueue_flow(self, flow: ScheduledFlow) -> str:
        """
        Add a flow to the distributed work queue.

        **Rust Reference**: `src/storage/sqlite.rs` lines 593-629

        Args:
            flow: ScheduledFlow object with all metadata

        Returns:
            task_id of the scheduled flow
        """
        self._check_connected()

        # Extract parent metadata if present
        parent_flow_id = None
        signal_token = None
        if flow.parent_metadata:
            parent_flow_id, signal_token = flow.parent_metadata

        # Convert to milliseconds timestamps (Rust uses INTEGER milliseconds)
        created_at_millis = int(flow.created_at.timestamp() * 1000)
        updated_at_millis = int(flow.updated_at.timestamp() * 1000)
        scheduled_for_millis = (
            int(flow.scheduled_for.timestamp() * 1000) if flow.scheduled_for else None
        )

        async with self._lock:  # Serialize access to connection
            await self._connection.execute(
                """
                INSERT INTO flow_queue (
                    task_id, flow_id, flow_type, flow_data,
                    status, locked_by, created_at, updated_at,
                    retry_count, error_message, scheduled_for,
                    parent_flow_id, signal_token, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    flow.task_id,
                    flow.flow_id,
                    flow.flow_type,
                    flow.flow_data,
                    flow.status.value,  # Already uppercase (TaskStatus enum)
                    flow.locked_by,
                    created_at_millis,
                    updated_at_millis,
                    flow.retry_count,
                    flow.error_message,
                    scheduled_for_millis,
                    parent_flow_id,
                    signal_token,
                    flow.version,
                ),
            )

            await self._connection.commit()

            # Wake up one waiting worker if this is an immediate (non-delayed) flow
            # **Rust Reference**: lines 624-626
            if flow.scheduled_for is None:
                self._work_notify.set()
                # NOTE: Don't clear here! Worker clears after waking up.
                # Clearing immediately would lose the notification if worker isn't waiting yet.

        return flow.task_id

    async def dequeue_flow(self, worker_id: str) -> ScheduledFlow | None:
        """
        Claim and retrieve a pending flow from the queue.

        **Rust Reference**: `src/storage/sqlite.rs` lines 631-671

        From Rust ergon: Uses optimistic concurrency with atomic UPDATE
        to prevent double-execution in distributed system.

        Design Pattern: Optimistic Concurrency Control
        UPDATE with WHERE status = 'PENDING' ensures only one worker claims each flow.
        """
        self._check_connected()

        now_millis = int(datetime.now().timestamp() * 1000)

        # Optimistic concurrency: Find and claim in a single atomic operation
        # **Rust Reference**: lines 642-661
        async with self._lock:  # Serialize access to connection
            try:
                # Find a pending flow that is ready to execute
                # (scheduled_for IS NULL or scheduled_for <= NOW)
                cursor = await self._connection.execute(
                    """
                    UPDATE flow_queue
                    SET status = 'RUNNING',
                        locked_by = ?,
                        updated_at = ?
                    WHERE task_id = (
                        SELECT task_id
                        FROM flow_queue
                        WHERE status = 'PENDING'
                          AND (scheduled_for IS NULL OR scheduled_for <= ?)
                        ORDER BY created_at ASC
                        LIMIT 1
                    )
                    RETURNING task_id, flow_id, flow_type, flow_data,
                              status, locked_by, created_at, updated_at,
                              retry_count, error_message, scheduled_for,
                              parent_flow_id, signal_token, version
                """,
                    (
                        worker_id,
                        now_millis,
                        now_millis,  # For scheduled_for comparison
                    ),
                )

                row = await cursor.fetchone()

                if row is None:
                    # No pending flows, or another worker claimed it first
                    # Must commit even when no row to close the transaction
                    await self._connection.commit()
                    return None

                # Commit the update
                await self._connection.commit()

                # Parse timestamps from milliseconds
                created_at = datetime.fromtimestamp(row[6] / 1000.0)
                updated_at = datetime.fromtimestamp(row[7] / 1000.0)
                scheduled_for = datetime.fromtimestamp(row[10] / 1000.0) if row[10] else None

                # Parse parent_metadata from parent_flow_id and signal_token
                parent_metadata = None
                if row[11] and row[12]:  # parent_flow_id and signal_token
                    parent_metadata = (row[11], row[12])

                # Return ScheduledFlow
                return ScheduledFlow(
                    task_id=row[0],
                    flow_id=row[1],
                    flow_type=row[2],
                    flow_data=row[3],
                    status=TaskStatus.RUNNING,
                    locked_by=worker_id,
                    retry_count=row[8],
                    created_at=created_at,
                    updated_at=updated_at,
                    error_message=row[9],
                    scheduled_for=scheduled_for,
                    parent_metadata=parent_metadata,
                    version=row[13],
                    claimed_at=datetime.now(),  # Python extension
                    completed_at=None,  # Python extension
                )

            except Exception as e:
                # No need to rollback - aiosqlite handles it automatically
                raise StorageError(f"Failed to dequeue flow: {e}") from e

    async def complete_flow(
        self, task_id: str, status: TaskStatus, error_message: str | None = None
    ) -> None:
        """
        Mark a flow task as complete, failed, or suspended.

        **Rust Reference**: `src/storage/sqlite.rs` lines 673-708

        Args:
            task_id: The task identifier
            status: The final status (COMPLETE, FAILED, or SUSPENDED)
            error_message: Optional error message if status is FAILED
        """
        self._check_connected()

        now_millis = int(datetime.now().timestamp() * 1000)

        # When marking as SUSPENDED, clear the lock so flow can be resumed
        # **Rust Reference**: lines 675-686
        if status == TaskStatus.SUSPENDED:
            if error_message is not None:
                cursor = await self._connection.execute(
                    """
                    UPDATE flow_queue
                    SET status = ?,
                        locked_by = NULL,
                        error_message = ?,
                        updated_at = ?
                    WHERE task_id = ?
                """,
                    (status.value, error_message, now_millis, task_id),
                )
            else:
                cursor = await self._connection.execute(
                    """
                    UPDATE flow_queue
                    SET status = ?,
                        locked_by = NULL,
                        updated_at = ?
                    WHERE task_id = ?
                """,
                    (status.value, now_millis, task_id),
                )
        else:
            if error_message is not None:
                cursor = await self._connection.execute(
                    """
                    UPDATE flow_queue
                    SET status = ?,
                        error_message = ?,
                        updated_at = ?
                    WHERE task_id = ?
                """,
                    (status.value, error_message, now_millis, task_id),
                )
            else:
                cursor = await self._connection.execute(
                    """
                    UPDATE flow_queue
                    SET status = ?,
                        updated_at = ?
                    WHERE task_id = ?
                """,
                    (status.value, now_millis, task_id),
                )

        if cursor.rowcount == 0:
            raise StorageError(f"Flow not found: task_id={task_id}")

        await self._connection.commit()

        # Notify any waiters that a flow status changed
        # **Rust Reference**: line 706
        self._status_notify.set()
        self._status_notify.clear()  # Reset for next notification

    async def retry_flow(self, task_id: str, error_message: str, delay: timedelta) -> None:
        """
        Reschedule a failed flow for retry after a delay.

        **Rust Reference**: `src/storage/sqlite.rs` lines 710-745

        This method:
        1. Increments retry_count
        2. Sets error_message
        3. Sets status back to PENDING
        4. Clears locked_by
        5. Sets scheduled_for (current time + delay)
        6. Re-enqueues the flow

        Args:
            task_id: Task identifier
            error_message: Error description
            delay: Delay before retry
        """
        self._check_connected()

        # Calculate scheduled_for timestamp (current time + delay)
        now = datetime.now()
        scheduled_for_millis = int((now + delay).timestamp() * 1000)
        now_millis = int(now.timestamp() * 1000)

        # UPDATE flow_queue SET retry_count = retry_count + 1, ...
        # **Rust Reference**: lines 721-737
        cursor = await self._connection.execute(
            """
            UPDATE flow_queue
            SET retry_count = retry_count + 1,
                error_message = ?,
                status = 'PENDING',
                locked_by = NULL,
                scheduled_for = ?,
                updated_at = ?
            WHERE task_id = ?
            """,
            (error_message, scheduled_for_millis, now_millis, task_id),
        )

        if cursor.rowcount == 0:
            raise StorageError(f"Flow not found: task_id={task_id}")

        await self._connection.commit()

        # Notify workers that new work is available (or will be available soon)
        # **Rust Reference**: line 743
        self._work_notify.set()
        self._work_notify.clear()  # Reset for next notification

    async def get_scheduled_flow(self, task_id: str) -> ScheduledFlow | None:
        """
        Retrieve a scheduled flow by task ID.

        From Dave Cheney: "Return early rather than nesting deeply"
        Simple query → None if not found → return result.
        """
        self._check_connected()

        cursor = await self._connection.execute(
            """
            SELECT task_id, flow_id, flow_type, flow_data,
                   status, locked_by, retry_count,
                   created_at, updated_at, parent_flow_id, signal_token,
                   error_message
            FROM flow_queue
            WHERE task_id = ?
        """,
            (task_id,),
        )

        row = await cursor.fetchone()

        if row is None:
            return None

        # Extract parent metadata if present
        parent_metadata = None
        if row[9] and row[10]:  # parent_flow_id and signal_token
            parent_metadata = (row[9], row[10])

        return ScheduledFlow(
            task_id=row[0],
            flow_id=row[1],
            flow_type=row[2],
            flow_data=row[3],
            status=TaskStatus(row[4]),
            locked_by=row[5],
            retry_count=row[6],
            created_at=datetime.fromtimestamp(row[7] / 1000.0),
            updated_at=datetime.fromtimestamp(row[8] / 1000.0),
            parent_metadata=parent_metadata,
            error_message=row[11],
        )

    # ========================================================================
    # Timer Operations
    # ========================================================================

    async def log_timer(
        self, flow_id: str, step: int, timer_fire_at: datetime, timer_name: str | None = None
    ) -> None:
        """
        Schedule a durable timer.

        From Dave Cheney: "Discourage the use of nil as a parameter"
        timer_name is Optional[str] for explicit null handling.
        """
        self._check_connected()

        fire_at_millis = int(timer_fire_at.timestamp() * 1000)

        await self._connection.execute(
            """
            UPDATE execution_log
            SET status = ?,
                timer_fire_at = ?,
                timer_name = ?
            WHERE id = ? AND step = ?
        """,
            (InvocationStatus.WAITING_FOR_TIMER.value, fire_at_millis, timer_name, flow_id, step),
        )

        await self._connection.commit()

        # Notify timer processor that a new timer was scheduled
        self._timer_notify.set()
        self._timer_notify.clear()  # Reset for next notification

    async def get_expired_timers(self, now: datetime) -> list[TimerInfo]:
        """
        Get all timers that have expired.

        **Rust Reference**: storage/mod.rs lines 66-71 (TimerInfo struct)

        Returns TimerInfo objects with flow_id, step, fire_at, and timer_name.
        """
        self._check_connected()

        now_millis = int(now.timestamp() * 1000)

        cursor = await self._connection.execute(
            """
            SELECT id, step, timer_fire_at, timer_name
            FROM execution_log
            WHERE status = ? AND timer_fire_at <= ?
        """,
            (InvocationStatus.WAITING_FOR_TIMER.value, now_millis),
        )

        rows = await cursor.fetchall()

        return [
            TimerInfo(
                flow_id=row[0],
                step=row[1],
                fire_at=datetime.fromtimestamp(row[2] / 1000.0, tz=UTC),
                timer_name=row[3],
            )
            for row in rows
        ]

    async def claim_timer(self, flow_id: str, step: int) -> bool:
        """
        Claim an expired timer (optimistic concurrency).

        **Rust Reference**: `src/storage/sqlite.rs` lines 815-858

        Uses UPDATE with WHERE status check for optimistic concurrency control.
        Multiple workers can safely call this - only one will succeed.

        From Dave Cheney: "Design APIs that are hard to misuse"
        Returns bool (True = claimed, False = already claimed).

        Args:
            flow_id: Flow identifier
            step: Step number

        Returns:
            True if timer was claimed, False if already claimed by another worker
        """
        self._check_connected()

        # Optimistic concurrency: only update if still waiting
        cursor = await self._connection.execute(
            """
            UPDATE execution_log
            SET status = ?
            WHERE id = ? AND step = ? AND status = ?
        """,
            (
                InvocationStatus.COMPLETE.value,
                flow_id,
                step,
                InvocationStatus.WAITING_FOR_TIMER.value,
            ),
        )

        await self._connection.commit()

        claimed = cursor.rowcount > 0

        # Notify timer processor that timer was claimed (may need to recalculate next wake time)
        if claimed:
            self._timer_notify.set()
            self._timer_notify.clear()  # Reset for next notification

        # True if we updated (claimed), False otherwise
        return claimed

    async def resume_flow(self, flow_id: str) -> bool:
        """
        Resume a suspended flow by changing status SUSPENDED → PENDING.

        **Rust Reference**: `src/storage/sqlite.rs` lines 1099-1132

        This method atomically:
        1. Checks if flow is SUSPENDED
        2. Changes status to PENDING
        3. Clears locked_by
        4. Re-enqueues to work queue

        Args:
            flow_id: Flow identifier

        Returns:
            True if flow was resumed, False if not suspended
        """
        self._check_connected()

        now_millis = int(datetime.now().timestamp() * 1000)

        # UPDATE flow_queue SET status = 'PENDING', locked_by = NULL
        # WHERE id = ? AND status = 'SUSPENDED'
        # **Rust Reference**: lines 1103-1114
        cursor = await self._connection.execute(
            """
            UPDATE flow_queue
            SET status = 'PENDING',
                locked_by = NULL,
                updated_at = ?
            WHERE flow_id = ?
              AND status = 'SUSPENDED'
            """,
            (now_millis, flow_id),
        )

        await self._connection.commit()

        # Return false if no rows affected (flow not in SUSPENDED state)
        # **Rust Reference**: lines 1118-1124
        if cursor.rowcount == 0:
            return False

        # Wake up one waiting worker since we just made a flow available
        # **Rust Reference**: line 1129
        self._work_notify.set()
        # NOTE: Don't clear here! Worker clears after waking up.

        return True

    async def get_invocations_for_flow(self, flow_id: str) -> list[Invocation]:
        """
        Get all execution_log (steps) for a specific flow.

        **Rust Reference**: `src/storage/mod.rs` lines 164-172

        Args:
            flow_id: Flow identifier

        Returns:
            List of all execution_log for this flow
        """
        self._check_connected()

        # Matches Rust query (sqlite.rs:467-483)
        cursor = await self._connection.execute(
            """
            SELECT id, step, timestamp, class_name, method_name, status, attempts,
                   parameters, params_hash, return_value, delay, retry_policy,
                   is_retryable, timer_fire_at, timer_name
            FROM execution_log
            WHERE id = ?
            ORDER BY step ASC
            """,
            (flow_id,),
        )

        rows = await cursor.fetchall()
        return [self._row_to_invocation(row) for row in rows]

    async def get_next_timer_fire_time(self) -> datetime | None:
        """
        Get the earliest timer fire time across all flows.

        **Rust Reference**: `src/storage/sqlite.rs` lines 844-863

        Returns:
            datetime of next timer, or None if no timers pending
        """
        self._check_connected()

        # SELECT MIN(timer_fire_at) FROM execution_log
        # WHERE status = 'WAITING_FOR_TIMER' AND timer_fire_at IS NOT NULL
        cursor = await self._connection.execute(
            """
            SELECT MIN(timer_fire_at) as next_fire_time
            FROM execution_log
            WHERE status = 'WAITING_FOR_TIMER'
              AND timer_fire_at IS NOT NULL
            """
        )

        row = await cursor.fetchone()

        if row and row[0]:
            # Convert from milliseconds to datetime
            return datetime.fromtimestamp(row[0] / 1000.0)

        return None

    async def store_suspension_result(
        self, flow_id: str, step: int, suspension_key: str, result: bytes
    ) -> None:
        """
        Store the result of a suspended invocation for later retrieval.

        **Rust Reference**: `src/storage/sqlite.rs` lines 913-940

        This is used when a flow suspends waiting for external signal or child flow.
        The result is stored in the suspension_params table.

        Args:
            flow_id: Flow identifier
            step: Step number
            suspension_key: Key to identify this suspension (e.g., signal name, timer name)
            result: Serialized result to store
        """
        self._check_connected()

        # Store in suspension_params table
        now_millis = int(datetime.now().timestamp() * 1000)

        await self._connection.execute(
            """
            INSERT INTO suspension_params (flow_id, step, suspension_key, result, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(flow_id, step, suspension_key)
            DO UPDATE SET result = excluded.result, created_at = excluded.created_at
            """,
            (flow_id, step, suspension_key, result, now_millis),
        )

        await self._connection.commit()

    async def get_suspension_result(
        self, flow_id: str, step: int, suspension_key: str
    ) -> bytes | None:
        """
        Get the stored result of a suspended invocation.

        **Rust Reference**: `src/storage/sqlite.rs` lines 942-958

        Args:
            flow_id: Flow identifier
            step: Step number
            suspension_key: Key to identify this suspension

        Returns:
            The serialized result, or None if not found
        """
        self._check_connected()

        cursor = await self._connection.execute(
            """
            SELECT result
            FROM suspension_params
            WHERE flow_id = ? AND step = ? AND suspension_key = ?
            """,
            (flow_id, step, suspension_key),
        )

        row = await cursor.fetchone()

        if row and row[0]:
            return row[0]

        return None

    async def remove_suspension_result(self, flow_id: str, step: int, suspension_key: str) -> None:
        """
        Remove the stored result of a suspended invocation.

        **Rust Reference**: `src/storage/sqlite.rs` lines 960-981

        Args:
            flow_id: Flow identifier
            step: Step number
            suspension_key: Key to identify this suspension
        """
        self._check_connected()

        await self._connection.execute(
            """
            DELETE FROM suspension_params
            WHERE flow_id = ? AND step = ? AND suspension_key = ?
            """,
            (flow_id, step, suspension_key),
        )

        await self._connection.commit()

    async def log_signal(self, flow_id: str, step: int, signal_name: str) -> None:
        """
        Mark an invocation as waiting for an external signal.

        **Rust Reference**: `src/storage/sqlite.rs` lines 892-911

        Updates the invocation status to WAITING_FOR_SIGNAL and stores the signal name.
        Called by child flow invocation before scheduling the child.

        Args:
            flow_id: Flow identifier
            step: Step number
            signal_name: Signal identifier (typically the child flow ID)
        """
        self._check_connected()

        await self._connection.execute(
            """
            UPDATE execution_log
            SET status = 'WAITING_FOR_SIGNAL',
                timer_name = ?
            WHERE id = ? AND step = ?
            """,
            (signal_name, flow_id, step),
        )

        await self._connection.commit()

    async def get_waiting_signals(self) -> list[SignalInfo]:
        """
        Get all flows waiting for external signals.

        **Rust Reference**: `src/storage/sqlite.rs` lines 1013-1045

        Returns flows where:
        - execution_log.status = 'WAITING_FOR_SIGNAL'
        - flow_queue.status = 'SUSPENDED'

        Returns:
            List of SignalInfo with flow_id, step, signal_name
        """

        self._check_connected()

        cursor = await self._connection.execute("""
            SELECT el.id, el.step, el.timer_name
            FROM execution_log el
            JOIN flow_queue fq ON el.id = fq.flow_id
            WHERE el.status = 'WAITING_FOR_SIGNAL'
              AND fq.status = 'SUSPENDED'
            ORDER BY el.timestamp ASC
            LIMIT 100
        """)

        rows = await cursor.fetchall()

        signals = []
        for row in rows:
            flow_id = row[0]
            step = row[1]
            signal_name = row[2]  # timer_name is used for signal_name
            signals.append(SignalInfo(flow_id=flow_id, step=step, signal_name=signal_name))

        return signals

    async def update_is_retryable(self, flow_id: str, step: int, is_retryable: bool) -> None:
        """
        Update the is_retryable flag for a specific step invocation.

        **Rust Reference**: `src/storage/sqlite.rs` lines 551-569

        Args:
            flow_id: Flow identifier
            step: Step number
            is_retryable: True if error is retryable, False if permanent
        """
        self._check_connected()

        await self._connection.execute(
            """
            UPDATE execution_log
            SET is_retryable = ?
            WHERE id = ? AND step = ?
            """,
            (1 if is_retryable else 0, flow_id, step),
        )
        await self._connection.commit()

    # ========================================================================
    # Utility Operations
    # ========================================================================

    async def reset(self) -> None:
        """
        Clear all data (for testing/demos).

        **Rust Reference**: `src/storage/sqlite.rs` lines 571-585

        From Dave Cheney: "Make the zero value useful"
        After reset, storage is empty but functional.
        """
        self._check_connected()

        await self._connection.execute("DELETE FROM execution_log")
        await self._connection.execute("DELETE FROM flow_queue")
        await self._connection.execute("DELETE FROM suspension_params")
        await self._connection.commit()

    async def close(self) -> None:
        """
        Close storage connections.

        From Dave Cheney: "Never start a goroutine without knowing when it will stop"
        Explicit resource cleanup, not relying on GC.
        """
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    # ========================================================================
    # Notification Protocol Methods
    # ========================================================================

    def work_notify(self) -> asyncio.Event:
        """
        Return event for work notifications (WorkNotificationSource protocol).

        **Rust Reference**: `src/storage/sqlite.rs` lines 741-744

        Workers wait on this event to be notified when work becomes available,
        instead of polling with sleep().

        Returns:
            asyncio.Event that is set when work is enqueued or resumed
        """
        return self._work_notify

    def timer_notify(self) -> asyncio.Event:
        """
        Return event for timer notifications (TimerNotificationSource protocol).

        **Rust Reference**: `src/storage/sqlite.rs` lines 746-749

        Timer processors wait on this event to be notified when timer state changes,
        instead of polling every N seconds.

        Returns:
            asyncio.Event that is set when timers are scheduled or claimed
        """
        return self._timer_notify

    def status_notify(self) -> asyncio.Event:
        """
        Return event for flow status change notifications.

        **Rust Reference**: `src/storage/sqlite.rs` lines 1129-1131

        Callers can use this to wait for flow status changes (completion, failure, etc.)
        instead of polling. The notification is triggered whenever any flow status changes.

        Returns:
            asyncio.Event that is set when any flow status changes
        """
        return self._status_notify

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _check_connected(self) -> None:
        """
        Guard clause: Ensure connection is open.

        From Dave Cheney: "Return early rather than nesting deeply"
        Check precondition at method start, not nested in try/catch.
        """
        if self._connection is None:
            raise StorageError("Not connected. Call connect() first.")

    def _row_to_invocation(self, row: tuple) -> Invocation:
        """
        Convert database row to Invocation object.

        **Rust Reference**: row_to_invocation (sqlite.rs:280-329)

        Row format (matches SELECT query):
        0:id, 1:step, 2:timestamp, 3:class_name, 4:method_name, 5:status,
        6:attempts, 7:parameters, 8:params_hash, 9:return_value, 10:delay,
        11:retry_policy, 12:is_retryable, 13:timer_fire_at, 14:timer_name
        """
        # Parse timestamp from milliseconds
        timestamp_ms = row[2]
        timestamp = (
            datetime.fromtimestamp(timestamp_ms / 1000.0) if timestamp_ms else datetime.now()
        )

        # Parse status string to enum (uppercase in DB, matching Rust)
        status_str = row[5]
        status = InvocationStatus(status_str)

        # Parse retry_policy from JSON
        retry_policy_json = row[11]
        retry_policy = None
        if retry_policy_json:
            import json

            try:
                policy_dict = json.loads(retry_policy_json)
                retry_policy = RetryPolicy(
                    max_attempts=policy_dict.get("max_attempts", 3),
                    initial_delay_ms=policy_dict.get("initial_delay_ms", 100),
                    max_delay_ms=policy_dict.get("max_delay_ms", 10000),
                    backoff_multiplier=policy_dict.get("backoff_multiplier", 2.0),
                )
            except (json.JSONDecodeError, TypeError, KeyError, ValueError):
                pass

        # Parse is_retryable (0/1/NULL -> False/True/None)
        is_retryable_int = row[12]
        is_retryable = None if is_retryable_int is None else bool(is_retryable_int)

        # Parse timer_fire_at from milliseconds
        timer_fire_at_ms = row[13]
        timer_fire_at = (
            datetime.fromtimestamp(timer_fire_at_ms / 1000.0) if timer_fire_at_ms else None
        )

        return Invocation(
            id=row[0],  # Flow execution ID
            flow_id=row[0],  # Same as id (redundant but kept for compatibility)
            step=row[1],
            timestamp=timestamp,
            class_name=row[3],
            method_name=row[4],
            status=status,
            attempts=row[6],
            parameters=row[7],
            params_hash=row[8],
            return_value=row[9],
            delay=row[10],
            retry_policy=retry_policy,
            is_retryable=is_retryable,
            updated_at=timestamp,  # Use timestamp (no separate updated_at in DB)
            timer_fire_at=timer_fire_at,
            timer_name=row[14],
        )
