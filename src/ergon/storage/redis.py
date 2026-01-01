"""
Redis-based execution log implementation.

This module provides a Redis backend for distributed execution with true
multi-machine support. Unlike SQLite which requires shared filesystem access,
Redis enables workers to run on completely separate machines.

Data Structures:
- ergon:queue:pending (LIST): FIFO queue of pending task IDs
- ergon:flow:{task_id} (HASH): Flow metadata and serialized data
- ergon:invocations:{flow_id} (LIST): Invocation history per flow
- ergon:running (ZSET): Running flows index (score = start time)
- ergon:timers:pending (ZSET): Pending timers (score = fire_at timestamp)
- ergon:inv:{flow_id}:{step} (HASH): Invocation data including timer info

Key Features:
- Blocking dequeue: Uses BLPOP for efficient worker polling
- Atomic operations: Uses MULTI/EXEC for consistency
- Network-accessible: True distributed execution across machines
- Connection pooling: redis-py connection pool for concurrent access

Performance Characteristics:
- Enqueue: O(1) with RPUSH
- Dequeue: O(1) with BLPOP (blocks until available)
- Status update: O(1) with HSET
- Network overhead: ~0.1-0.5ms per operation (local network)

From Software Design Patterns:
- Adapter Pattern: Implements ExecutionLog protocol for Redis
- Template Method: Common operations follow fixed patterns
- Strategy Pattern: Different data structures for different access patterns

From Dave Cheney:
- Simple, clear naming (flow_key, invocations_key)
- Explicit dependencies (Redis connection passed to constructor)
- Return early pattern (guard clauses for existence checks)
"""

import pickle
from typing import Optional
from datetime import datetime, timezone

try:
    import redis.asyncio as redis
except ImportError:
    raise ImportError(
        "redis-py is required for RedisExecutionLog. "
        "Install with: pip install redis"
    )

from ergon.core import Invocation, InvocationStatus, TaskStatus, ScheduledFlow
from ergon.storage.base import ExecutionLog, StorageError


class RedisExecutionLog(ExecutionLog):
    """
    Redis execution log using connection pooling.

    Design Pattern: Adapter Pattern
    Adapts Redis key-value store to ExecutionLog protocol.

    From Dave Cheney: "Avoid package level state"
    All dependencies (Redis connection) passed explicitly.

    Usage:
        storage = RedisExecutionLog("redis://localhost:6379")
        await storage.connect()

        # Enqueue flow
        task_id = await storage.enqueue_flow(flow_id, flow_type, flow_data)

        # Worker dequeue
        flow = await storage.dequeue_flow("worker-1")
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        max_connections: int = 16
    ):
        """
        Initialize Redis execution log.

        From Dave Cheney: "Design APIs for their default use case"
        Default redis_url works for local development.

        Args:
            redis_url: Redis connection URL
            max_connections: Maximum pool size
        """
        self._redis_url = redis_url
        self._max_connections = max_connections
        self._redis: Optional[redis.Redis] = None

    # ========================================================================
    # Connection Management
    # ========================================================================

    async def connect(self) -> None:
        """
        Establish Redis connection pool.

        From Dave Cheney: "Make the zero value useful"
        Connection pool initialized lazily on connect().
        """
        self._redis = redis.from_url(
            self._redis_url,
            encoding="utf-8",
            decode_responses=False,  # We handle binary data
            max_connections=self._max_connections
        )

    async def close(self) -> None:
        """
        Close Redis connection pool.

        From Dave Cheney: "Never start a goroutine without knowing when it will stop"
        Explicit close for resource cleanup.
        """
        if self._redis:
            await self._redis.close()

    def _check_connected(self) -> None:
        """
        Guard: Ensure connection established.

        From Dave Cheney: "Return early rather than nesting deeply"
        Raises immediately if not connected.
        """
        if self._redis is None:
            raise StorageError("Not connected. Call connect() first.")

    # ========================================================================
    # Key Builders - Simple, Clear Naming
    # ========================================================================

    @staticmethod
    def _flow_key(task_id: str) -> str:
        """Build Redis key for flow metadata."""
        return f"ergon:flow:{task_id}"

    @staticmethod
    def _invocations_key(flow_id: str) -> str:
        """Build Redis key for invocations list."""
        return f"ergon:invocations:{flow_id}"

    @staticmethod
    def _invocation_key(flow_id: str, step: int) -> str:
        """Build Redis key for specific invocation."""
        return f"ergon:inv:{flow_id}:{step}"

    # ========================================================================
    # Invocation Operations
    # ========================================================================

    async def log_invocation_start(
        self,
        flow_id: str,
        step: int,
        class_name: str,
        step_name: str,
        params: bytes
    ) -> Invocation:
        """
        Log the start of a step invocation.

        From Rust ergon: Uses Redis HASH for invocation data,
        atomic MULTI/EXEC for consistency.
        """
        self._check_connected()

        now = datetime.now()
        inv_key = self._invocation_key(flow_id, step)

        # Atomic pipeline: store invocation + add to invocations list
        async with self._redis.pipeline(transaction=True) as pipe:
            # Store invocation as HASH
            await pipe.hset(inv_key, "flow_id", flow_id)
            await pipe.hset(inv_key, "step", str(step))
            await pipe.hset(inv_key, "class_name", class_name)
            await pipe.hset(inv_key, "step_name", step_name)
            await pipe.hset(inv_key, "params", params)
            await pipe.hset(inv_key, "status", InvocationStatus.PENDING.value)
            await pipe.hset(inv_key, "attempts", "1")
            await pipe.hset(inv_key, "created_at", now.isoformat())
            await pipe.hset(inv_key, "updated_at", now.isoformat())
            await pipe.hset(inv_key, "is_retryable", "1")  # Default: retryable

            # Add to invocations list
            await pipe.lpush(self._invocations_key(flow_id), inv_key)

            await pipe.execute()

        return Invocation(
            flow_id=flow_id,
            step=step,
            class_name=class_name,
            step_name=step_name,
            params=params,
            result=None,
            status=InvocationStatus.PENDING,
            attempts=1,
            is_retryable=True,
            created_at=now,
            updated_at=now,
            fire_at=None,
            timer_name=""
        )

    async def log_invocation_completion(
        self,
        flow_id: str,
        step: int,
        result: bytes
    ) -> Invocation:
        """
        Log the completion of a step invocation.

        From Dave Cheney: "Only handle an error once"
        Either succeeds or raises, no double error handling.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)

        # Check existence
        exists = await self._redis.exists(inv_key)
        if not exists:
            raise StorageError(
                f"Invocation not found: flow_id={flow_id}, step={step}"
            )

        now = datetime.now()

        # Atomic update
        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(inv_key, "status", InvocationStatus.COMPLETE.value)
            await pipe.hset(inv_key, "result", result)
            await pipe.hset(inv_key, "updated_at", now.isoformat())
            await pipe.execute()

        # Fetch and return
        return await self._get_invocation_by_key(inv_key)

    async def get_invocation(
        self,
        flow_id: str,
        step: int
    ) -> Optional[Invocation]:
        """
        Retrieve a specific invocation.

        From Dave Cheney: "Return early rather than nesting deeply"
        Check existence → return None if not found → return result.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)
        exists = await self._redis.exists(inv_key)

        if not exists:
            return None

        return await self._get_invocation_by_key(inv_key)

    async def get_latest_invocation(
        self,
        flow_id: str
    ) -> Optional[Invocation]:
        """
        Get the latest invocation for a flow.

        From Dave Cheney: "Design APIs for their default use case"
        Common need: check latest step → single method.
        """
        invocations = await self.get_invocations_for_flow(flow_id)
        if not invocations:
            return None

        return max(invocations, key=lambda inv: inv.step)

    async def get_invocations_for_flow(
        self,
        flow_id: str
    ) -> list[Invocation]:
        """
        Get all invocations for a flow.

        From Rust ergon: Uses LRANGE to get all invocation keys,
        then HGETALL for each invocation.
        """
        self._check_connected()

        inv_list_key = self._invocations_key(flow_id)

        # Get all invocation keys
        inv_keys = await self._redis.lrange(inv_list_key, 0, -1)

        invocations = []
        for key in inv_keys:
            inv = await self._get_invocation_by_key(key.decode())
            invocations.append(inv)

        # Sort by step
        invocations.sort(key=lambda inv: inv.step)
        return invocations

    async def get_incomplete_flows(self) -> list[Invocation]:
        """
        Get all incomplete flows (step 0 not COMPLETE).

        From Rust ergon: Scans for all invocation keys,
        filters by status != COMPLETE.
        """
        self._check_connected()

        # Scan for all invocation keys
        incomplete = []
        async for key in self._redis.scan_iter(match="ergon:inv:*"):
            data = await self._redis.hgetall(key)
            if not data:
                continue

            # Parse invocation
            inv = self._parse_invocation(data)

            # Filter: step 0 and not complete
            if inv.step == 0 and inv.status != InvocationStatus.COMPLETE:
                incomplete.append(inv)

        return incomplete

    async def has_non_retryable_error(self, flow_id: str) -> bool:
        """
        Check if flow has non-retryable error.

        From Dave Cheney: "Eliminate error handling by eliminating errors"
        Boolean return, no exceptions.
        """
        self._check_connected()

        inv_list_key = self._invocations_key(flow_id)

        # Get all invocation keys
        inv_keys = await self._redis.lrange(inv_list_key, 0, -1)

        for key in inv_keys:
            is_retryable = await self._redis.hget(key, "is_retryable")
            if is_retryable == b"0":
                return True

        return False

    # ========================================================================
    # Queue Operations - Distributed Work Queue
    # ========================================================================

    async def enqueue_flow(
        self,
        flow_id: str,
        flow_type: str,
        flow_data: bytes
    ) -> str:
        """
        Add a flow to the distributed work queue.

        From Rust ergon: Atomic MULTI/EXEC to store metadata + enqueue.
        Uses RPUSH for FIFO ordering.
        """
        self._check_connected()

        import uuid
        task_id = str(uuid.uuid4())
        flow_key = self._flow_key(task_id)
        now = datetime.now()

        # Atomic: store flow metadata + enqueue
        async with self._redis.pipeline(transaction=True) as pipe:
            # Store flow metadata as HASH
            await pipe.hset(flow_key, "task_id", task_id)
            await pipe.hset(flow_key, "flow_id", flow_id)
            await pipe.hset(flow_key, "flow_type", flow_type)
            await pipe.hset(flow_key, "flow_data", flow_data)
            await pipe.hset(flow_key, "status", TaskStatus.PENDING.value)
            await pipe.hset(flow_key, "created_at", now.isoformat())
            await pipe.hset(flow_key, "updated_at", now.isoformat())
            await pipe.hset(flow_key, "retry_count", "0")

            # Add to pending queue (FIFO)
            await pipe.rpush("ergon:queue:pending", task_id)

            await pipe.execute()

        return task_id

    async def dequeue_flow(
        self,
        worker_id: str
    ) -> Optional[ScheduledFlow]:
        """
        Claim and retrieve a pending flow from the queue.

        From Rust ergon: Uses BLPOP for efficient blocking dequeue.
        Atomic update of status + locked_by.

        Design Pattern: Observer Pattern
        Workers block on queue until work available.
        """
        self._check_connected()

        # Blocking pop with 1 second timeout
        result = await self._redis.blpop("ergon:queue:pending", timeout=1.0)

        if result is None:
            return None

        _, task_id_bytes = result
        task_id = task_id_bytes.decode()

        flow_key = self._flow_key(task_id)
        now = datetime.now()

        # Check if flow is ready (scheduled_for has passed)
        scheduled_for_bytes = await self._redis.hget(flow_key, "scheduled_for")

        if scheduled_for_bytes:
            scheduled_for = datetime.fromisoformat(scheduled_for_bytes.decode())
            if scheduled_for > now:
                # Not ready yet, push back to queue
                await self._redis.rpush("ergon:queue:pending", task_id)
                return None

        # Atomic: update status + lock + add to running index
        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(flow_key, "status", TaskStatus.RUNNING.value)
            await pipe.hset(flow_key, "locked_by", worker_id)
            await pipe.hset(flow_key, "updated_at", now.isoformat())
            await pipe.hset(flow_key, "claimed_at", now.isoformat())

            # Add to running index (sorted set, score = start time)
            await pipe.zadd("ergon:running", {task_id: now.timestamp()})

            await pipe.execute()

        # Fetch flow metadata
        data = await self._redis.hgetall(flow_key)
        return self._parse_scheduled_flow(data)

    async def complete_flow(
        self,
        task_id: str,
        status: TaskStatus,
        error_message: Optional[str] = None
    ) -> None:
        """
        Mark a flow task as complete or failed.

        From Dave Cheney: "Eliminate error handling by eliminating errors"
        Type system enforces status must be TaskStatus.

        Args:
            task_id: The task identifier
            status: The final status (COMPLETE, FAILED, or SUSPENDED)
            error_message: Optional error message if status is FAILED
        """
        self._check_connected()

        # Guard: Validate terminal status
        if not status.is_terminal:
            raise ValueError(
                f"Status must be terminal (COMPLETE/FAILED), got {status}"
            )

        flow_key = self._flow_key(task_id)

        # Check existence
        exists = await self._redis.exists(flow_key)
        if not exists:
            raise StorageError(f"Flow not found: task_id={task_id}")

        now = datetime.now()

        # Atomic: update status + remove from running index
        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(flow_key, "status", status.value)
            await pipe.hset(flow_key, "completed_at", now.isoformat())
            await pipe.hset(flow_key, "updated_at", now.isoformat())
            if error_message is not None:
                await pipe.hset(flow_key, "error_message", error_message)

            # Remove from running index
            await pipe.zrem("ergon:running", task_id)

            await pipe.execute()

    async def get_scheduled_flow(
        self,
        task_id: str
    ) -> Optional[ScheduledFlow]:
        """
        Retrieve a scheduled flow by task ID.

        From Dave Cheney: "Return early rather than nesting deeply"
        Check existence → None if not found → return result.
        """
        self._check_connected()

        flow_key = self._flow_key(task_id)
        exists = await self._redis.exists(flow_key)

        if not exists:
            return None

        data = await self._redis.hgetall(flow_key)
        return self._parse_scheduled_flow(data)

    # ========================================================================
    # Timer Operations - Durable Timers
    # ========================================================================

    async def log_timer(
        self,
        flow_id: str,
        step: int,
        timer_fire_at: datetime,
        timer_name: Optional[str] = None
    ) -> None:
        """
        Schedule a durable timer.

        From Rust ergon: Uses ZADD to sorted set for efficient expiry queries.
        Score = fire_at timestamp for range queries.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)
        fire_at_millis = int(timer_fire_at.timestamp() * 1000)

        # Atomic: update invocation + add to timers sorted set
        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(
                inv_key, "status", InvocationStatus.WAITING_FOR_TIMER.value
            )
            await pipe.hset(inv_key, "fire_at", timer_fire_at.isoformat())
            await pipe.hset(inv_key, "timer_name", timer_name)

            # Add to sorted set (score = fire_at_millis)
            timer_member = f"{flow_id}:{step}"
            await pipe.zadd("ergon:timers:pending", {timer_member: fire_at_millis})

            await pipe.execute()

    async def get_expired_timers(
        self,
        now: datetime
    ) -> list['TimerInfo']:
        """
        Get all timers that have expired.

        **Rust Reference**: storage/mod.rs lines 66-71 (TimerInfo struct)

        Returns TimerInfo objects with flow_id, step, fire_at, and timer_name.
        """
        from ergon.core import TimerInfo

        self._check_connected()

        now_millis = int(now.timestamp() * 1000)

        # Query sorted set for timers with score <= now_millis
        expired = await self._redis.zrangebyscore(
            "ergon:timers:pending",
            0,
            now_millis,
            withscores=True,
            start=0,
            num=100
        )

        timers = []
        for member, score in zip(expired[::2], expired[1::2]):
            # member format: "flow_id:step"
            parts = member.decode().split(":")
            if len(parts) != 2:
                continue

            flow_id = parts[0]
            try:
                step = int(parts[1])
                # Get timer details from invocation
                inv_key = f"ergon:inv:{flow_id}:{step}"
                inv_data = await self._redis.hgetall(inv_key)
                timer_name = inv_data.get(b"timer_name", b"").decode() if inv_data else None
                fire_at = datetime.fromtimestamp(score / 1000.0, tz=timezone.utc)

                timers.append(TimerInfo(
                    flow_id=flow_id,
                    step=step,
                    fire_at=fire_at,
                    timer_name=timer_name
                ))
            except (ValueError, KeyError):
                continue

        return timers

    async def claim_timer(
        self,
        flow_id: str,
        step: int,
        worker_id: str
    ) -> bool:
        """
        Atomically claim a timer.

        From Rust ergon: Uses Lua script for atomic check-and-update.
        Ensures only one worker claims each timer.

        Design Pattern: Optimistic Concurrency Control
        Lua script ensures atomicity without locks.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)
        timer_member = f"{flow_id}:{step}"

        # Lua script for atomic claim
        script = """
        local inv_key = KEYS[1]
        local timer_key = KEYS[2]
        local member = ARGV[1]
        local unit_value = ARGV[2]

        local status = redis.call('HGET', inv_key, 'status')

        if status == 'waiting_for_timer' then
            redis.call('HSET', inv_key, 'status', 'complete')
            redis.call('HSET', inv_key, 'result', unit_value)
            redis.call('ZREM', timer_key, member)
            return 1
        else
            return 0
        end
        """

        # Unit value for timer completion
        unit_value = pickle.dumps(None)

        # Execute Lua script
        claimed = await self._redis.eval(
            script,
            2,  # Number of keys
            inv_key,
            "ergon:timers:pending",
            timer_member,
            unit_value
        )

        return claimed == 1

    async def reset(self) -> None:
        """
        Reset all ergon data (clear all keys).

        From Dave Cheney: "Be cautious with destructive operations"
        Only deletes ergon:* keys, doesn't affect other Redis data.
        """
        self._check_connected()

        # Scan and delete all ergon keys
        keys = []
        async for key in self._redis.scan_iter(match="ergon:*"):
            keys.append(key)

        if keys:
            await self._redis.delete(*keys)

    # ========================================================================
    # Helpers - Parsing and Conversion
    # ========================================================================

    async def _get_invocation_by_key(self, key: str) -> Invocation:
        """
        Fetch and parse invocation from Redis HASH.

        From Dave Cheney: "Keep functions small and focused"
        Single responsibility: fetch + parse.
        """
        data = await self._redis.hgetall(key)
        return self._parse_invocation(data)

    def _parse_invocation(self, data: dict) -> Invocation:
        """
        Parse invocation from Redis HASH data.

        From Dave Cheney: "Errors are values"
        Returns Invocation or raises StorageError.
        """
        try:
            flow_id = data[b"flow_id"].decode()
            step = int(data[b"step"].decode())
            class_name = data[b"class_name"].decode()
            step_name = data[b"step_name"].decode()
            params = data[b"params"]
            status = InvocationStatus(data[b"status"].decode())
            attempts = int(data[b"attempts"].decode())
            is_retryable = data[b"is_retryable"].decode() == "1"

            created_at = datetime.fromisoformat(data[b"created_at"].decode())
            updated_at = datetime.fromisoformat(data[b"updated_at"].decode())

            result = data.get(b"result")

            fire_at = None
            if b"fire_at" in data:
                fire_at = datetime.fromisoformat(data[b"fire_at"].decode())

            timer_name = ""
            if b"timer_name" in data:
                timer_name = data[b"timer_name"].decode()

            return Invocation(
                flow_id=flow_id,
                step=step,
                class_name=class_name,
                step_name=step_name,
                params=params,
                result=result,
                status=status,
                attempts=attempts,
                is_retryable=is_retryable,
                created_at=created_at,
                updated_at=updated_at,
                fire_at=fire_at,
                timer_name=timer_name
            )
        except (KeyError, ValueError, UnicodeDecodeError) as e:
            raise StorageError(f"Failed to parse invocation: {e}")

    def _parse_scheduled_flow(self, data: dict) -> ScheduledFlow:
        """
        Parse scheduled flow from Redis HASH data.

        From Dave Cheney: "Return early rather than nesting deeply"
        Simple parse → return result.
        """
        try:
            task_id = data[b"task_id"].decode()
            flow_id = data[b"flow_id"].decode()
            flow_type = data[b"flow_type"].decode()
            flow_data = data[b"flow_data"]
            status = TaskStatus(data[b"status"].decode())
            retry_count = int(data.get(b"retry_count", b"0").decode())

            created_at = datetime.fromisoformat(data[b"created_at"].decode())

            locked_by = None
            if b"locked_by" in data:
                locked_by = data[b"locked_by"].decode()

            claimed_at = None
            if b"claimed_at" in data:
                claimed_at = datetime.fromisoformat(data[b"claimed_at"].decode())

            completed_at = None
            if b"completed_at" in data:
                completed_at = datetime.fromisoformat(data[b"completed_at"].decode())

            return ScheduledFlow(
                task_id=task_id,
                flow_id=flow_id,
                flow_type=flow_type,
                flow_data=flow_data,
                status=status,
                locked_by=locked_by,
                retry_count=retry_count,
                scheduled_at=created_at,  # Use created_at as scheduled_at
                claimed_at=claimed_at,
                completed_at=completed_at
            )
        except (KeyError, ValueError, UnicodeDecodeError) as e:
            raise StorageError(f"Failed to parse scheduled flow: {e}")
