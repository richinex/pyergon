"""Redis-based execution log implementation.

Provides a Redis backend for distributed execution with true
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

Design: Adapter Pattern
Implements ExecutionLog protocol for Redis, adapting Redis key-value
store to the ExecutionLog interface.
"""

from __future__ import annotations

import asyncio
import pickle
from datetime import UTC, datetime

from uuid_extensions import uuid7

try:
    import redis.asyncio as redis
except ImportError:
    raise ImportError("redis-py is required for RedisExecutionLog. Install with: pip install redis")

from pyergon.models import (
    Invocation,
    InvocationStatus,
    RetryPolicy,
    ScheduledFlow,
    TaskStatus,
    TimerInfo,
)
from pyergon.storage.base import ExecutionLog, StorageError


class RedisExecutionLog(ExecutionLog):
    """Redis execution log using connection pooling.

    Design: Adapter Pattern
    Adapts Redis key-value store to ExecutionLog protocol.

    All dependencies (Redis connection) passed explicitly.

    Usage:
        storage = RedisExecutionLog("redis://localhost:6379")
        await storage.connect()

        task_id = await storage.enqueue_flow(flow_id, flow_type, flow_data)
        flow = await storage.dequeue_flow("worker-1")
    """

    def __init__(self, redis_url: str = "redis://localhost:6379", max_connections: int = 16):
        """Initialize Redis execution log.

        Default redis_url works for local development.

        Args:
            redis_url: Redis connection URL
            max_connections: Maximum pool size
        """
        self._redis_url = redis_url
        self._max_connections = max_connections
        self._redis: redis.Redis | None = None

        self._work_notify = asyncio.Event()
        self._timer_notify = asyncio.Event()
        self._status_notify = asyncio.Event()

    async def connect(self) -> None:
        """Establish Redis connection pool."""
        self._redis = redis.from_url(
            self._redis_url,
            encoding="utf-8",
            decode_responses=False,  # We handle binary data
            max_connections=self._max_connections,
        )

    async def close(self) -> None:
        """Close Redis connection pool."""
        if self._redis:
            await self._redis.close()

    def _check_connected(self) -> None:
        """Ensure connection established.

        Raises immediately if not connected.
        """
        if self._redis is None:
            raise StorageError("Not connected. Call connect() first.")

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
        """Log the start of a step invocation.

        Uses Redis HASH for invocation data with atomic MULTI/EXEC
        for consistency.
        """
        self._check_connected()

        # Check if invocation already exists
        existing = await self.get_invocation(flow_id, step)
        if existing is not None:
            if existing.status == InvocationStatus.COMPLETE:
                # Don't overwrite cached results during replay
                return existing
            if existing.status == InvocationStatus.WAITING_FOR_SIGNAL:
                # Don't overwrite signal state during replay
                return existing
            if existing.status == InvocationStatus.WAITING_FOR_TIMER:
                # Don't overwrite timer state during replay
                return existing

        now = datetime.now(UTC)
        now_ts = int(now.timestamp())
        inv_key = self._invocation_key(flow_id, step)
        inv_id = existing.id if existing else str(uuid7())
        attempts = existing.attempts + 1 if existing else 1

        # Atomic pipeline: store invocation + add to invocations list
        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(inv_key, "id", inv_id)
            await pipe.hset(inv_key, "flow_id", flow_id)
            await pipe.hset(inv_key, "step", str(step))
            await pipe.hset(inv_key, "class_name", class_name)
            await pipe.hset(inv_key, "method_name", method_name)
            await pipe.hset(inv_key, "parameters", parameters)
            await pipe.hset(inv_key, "params_hash", str(params_hash))
            await pipe.hset(inv_key, "status", InvocationStatus.PENDING.value)
            await pipe.hset(inv_key, "attempts", str(attempts))
            await pipe.hset(inv_key, "created_at", now_ts)
            await pipe.hset(inv_key, "updated_at", now_ts)
            await pipe.hset(inv_key, "is_retryable", "1")  # Default: retryable

            if delay is not None:
                await pipe.hset(inv_key, "delay", str(delay))

            if retry_policy is not None:
                import pickle

                await pipe.hset(inv_key, "retry_policy", pickle.dumps(retry_policy))

            # Add to invocations list (only if new invocation)
            if existing is None:
                await pipe.lpush(self._invocations_key(flow_id), inv_key)

            await pipe.execute()

        return Invocation(
            id=inv_id,
            flow_id=flow_id,
            step=step,
            timestamp=now,
            class_name=class_name,
            method_name=method_name,
            status=InvocationStatus.PENDING,
            attempts=attempts,
            parameters=parameters,
            params_hash=params_hash,
            return_value=None,
            delay=delay,
            retry_policy=retry_policy,
            is_retryable=None,
            timer_fire_at=None,
            timer_name=None,
            updated_at=now,
        )

    async def log_invocation_completion(
        self, flow_id: str, step: int, return_value: bytes, is_retryable: bool | None = None
    ) -> Invocation:
        """Log the completion of a step invocation.

        Either succeeds or raises StorageError.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)

        # Check existence
        exists = await self._redis.exists(inv_key)
        if not exists:
            raise StorageError(f"Invocation not found: flow_id={flow_id}, step={step}")

        now = datetime.now(UTC)
        now_ts = int(now.timestamp())

        # Atomic update
        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(inv_key, "status", InvocationStatus.COMPLETE.value)
            await pipe.hset(inv_key, "return_value", return_value)
            await pipe.hset(inv_key, "updated_at", now_ts)

            if is_retryable is not None:
                await pipe.hset(inv_key, "is_retryable", "1" if is_retryable else "0")

            await pipe.execute()

        # Fetch and return
        return await self._get_invocation_by_key(inv_key)

    async def get_invocation(self, flow_id: str, step: int) -> Invocation | None:
        """Retrieve a specific invocation.

        Returns None if not found.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)
        exists = await self._redis.exists(inv_key)

        if not exists:
            return None

        return await self._get_invocation_by_key(inv_key)

    async def get_latest_invocation(self, flow_id: str) -> Invocation | None:
        """Get the latest invocation for a flow.

        Returns None if no invocations exist.
        """
        invocations = await self.get_invocations_for_flow(flow_id)
        if not invocations:
            return None

        return max(invocations, key=lambda inv: inv.step)

    async def get_invocations_for_flow(self, flow_id: str) -> list[Invocation]:
        """Get all invocations for a flow.

        Uses LRANGE to get all invocation keys, then HGETALL for each.
        """
        self._check_connected()

        inv_list_key = self._invocations_key(flow_id)

        inv_keys = await self._redis.lrange(inv_list_key, 0, -1)

        invocations = []
        for key in inv_keys:
            inv = await self._get_invocation_by_key(key.decode())
            invocations.append(inv)

        invocations.sort(key=lambda inv: inv.step)
        return invocations

    async def get_incomplete_flows(self) -> list[Invocation]:
        """Get all incomplete flows (step 0 not COMPLETE).

        Scans all invocation keys and filters by status.
        """
        self._check_connected()

        incomplete = []
        async for key in self._redis.scan_iter(match="ergon:inv:*"):
            data = await self._redis.hgetall(key)
            if not data:
                continue

            inv = self._parse_invocation(data)

            if inv.step == 0 and inv.status != InvocationStatus.COMPLETE:
                incomplete.append(inv)

        return incomplete

    async def has_non_retryable_error(self, flow_id: str) -> bool:
        """Check if flow has non-retryable error.

        Returns boolean, no exceptions.
        """
        self._check_connected()

        inv_list_key = self._invocations_key(flow_id)
        inv_keys = await self._redis.lrange(inv_list_key, 0, -1)

        for key in inv_keys:
            is_retryable = await self._redis.hget(key, "is_retryable")
            if is_retryable == b"0":
                return True

        return False

    async def enqueue_flow(self, flow: ScheduledFlow) -> str:
        """Add a flow to the distributed work queue.

        Atomic MULTI/EXEC to store metadata and enqueue.
        Uses RPUSH for FIFO ordering, or ZADD for delayed execution.
        """
        self._check_connected()

        task_id = flow.task_id
        flow_key = self._flow_key(task_id)
        now = datetime.now(UTC)

        if flow.scheduled_for:
            scheduled_ts_ms = int(flow.scheduled_for.timestamp() * 1000)

            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.hset(flow_key, "task_id", task_id)
                await pipe.hset(flow_key, "flow_id", flow.flow_id)
                await pipe.hset(flow_key, "flow_type", flow.flow_type)
                await pipe.hset(flow_key, "flow_data", flow.flow_data)
                await pipe.hset(flow_key, "status", TaskStatus.PENDING.value)
                await pipe.hset(flow_key, "created_at", int(flow.created_at.timestamp()))
                await pipe.hset(flow_key, "updated_at", int(now.timestamp()))
                await pipe.hset(flow_key, "scheduled_for", scheduled_ts_ms)
                await pipe.hset(flow_key, "retry_count", str(flow.retry_count))

                if flow.parent_metadata:
                    parent_flow_id, signal_token = flow.parent_metadata
                    await pipe.hset(flow_key, "parent_flow_id", parent_flow_id)
                    await pipe.hset(flow_key, "signal_token", signal_token)
                if flow.version:
                    await pipe.hset(flow_key, "version", flow.version)

                await pipe.set(f"ergon:flow_task_map:{flow.flow_id}", task_id)
                await pipe.zadd("ergon:queue:delayed", {task_id: scheduled_ts_ms})
                await pipe.execute()
        else:
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.hset(flow_key, "task_id", task_id)
                await pipe.hset(flow_key, "flow_id", flow.flow_id)
                await pipe.hset(flow_key, "flow_type", flow.flow_type)
                await pipe.hset(flow_key, "flow_data", flow.flow_data)
                await pipe.hset(flow_key, "status", TaskStatus.PENDING.value)
                await pipe.hset(flow_key, "created_at", int(flow.created_at.timestamp()))
                await pipe.hset(flow_key, "updated_at", int(now.timestamp()))
                await pipe.hset(flow_key, "retry_count", str(flow.retry_count))

                if flow.parent_metadata:
                    parent_flow_id, signal_token = flow.parent_metadata
                    await pipe.hset(flow_key, "parent_flow_id", parent_flow_id)
                    await pipe.hset(flow_key, "signal_token", signal_token)
                if flow.version:
                    await pipe.hset(flow_key, "version", flow.version)

                await pipe.set(f"ergon:flow_task_map:{flow.flow_id}", task_id)
                await pipe.rpush("ergon:queue:pending", task_id)
                await pipe.execute()

            # Wake up waiting worker
            # Don't clear here - worker clears after waking up
            self._work_notify.set()

        return task_id

    async def dequeue_flow(self, worker_id: str) -> ScheduledFlow | None:
        """Claim and retrieve a pending flow from the queue.

        Uses BLPOP for efficient blocking dequeue with atomic update
        of status and locked_by.

        Design: Observer Pattern
        Workers block on queue until work available.
        """
        self._check_connected()

        result = await self._redis.blpop("ergon:queue:pending", timeout=1.0)

        if result is None:
            return None

        _, task_id_bytes = result
        task_id = task_id_bytes.decode()

        flow_key = self._flow_key(task_id)
        now = datetime.now(UTC)
        now_ts = int(now.timestamp())

        scheduled_for_bytes = await self._redis.hget(flow_key, "scheduled_for")

        if scheduled_for_bytes:
            scheduled_for_ms = int(scheduled_for_bytes.decode())
            scheduled_for = datetime.fromtimestamp(scheduled_for_ms / 1000, tz=UTC)
            if scheduled_for > now:
                await self._redis.rpush("ergon:queue:pending", task_id)
                return None

        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(flow_key, "status", TaskStatus.RUNNING.value)
            await pipe.hset(flow_key, "locked_by", worker_id)
            await pipe.hset(flow_key, "updated_at", now_ts)
            await pipe.hset(flow_key, "claimed_at", now_ts)
            await pipe.zadd("ergon:running", {task_id: now_ts})
            await pipe.execute()

        data = await self._redis.hgetall(flow_key)
        return self._parse_scheduled_flow(data)

    async def complete_flow(
        self, task_id: str, status: TaskStatus, error_message: str | None = None
    ) -> None:
        """Mark a flow task as complete or failed.

        Args:
            task_id: The task identifier
            status: The final status (COMPLETE, FAILED, or SUSPENDED)
            error_message: Optional error message if status is FAILED
        """
        self._check_connected()

        if not status.is_terminal:
            raise ValueError(f"Status must be terminal (COMPLETE/FAILED), got {status}")

        flow_key = self._flow_key(task_id)

        exists = await self._redis.exists(flow_key)
        if not exists:
            raise StorageError(f"Flow not found: task_id={task_id}")

        now = datetime.now(UTC)
        now_ts = int(now.timestamp())

        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(flow_key, "status", status.value)
            await pipe.hset(flow_key, "completed_at", now_ts)
            await pipe.hset(flow_key, "updated_at", now_ts)
            if error_message is not None:
                await pipe.hset(flow_key, "error_message", error_message)

            await pipe.zrem("ergon:running", task_id)
            await pipe.execute()

    async def get_scheduled_flow(self, task_id: str) -> ScheduledFlow | None:
        """Retrieve a scheduled flow by task ID.

        Returns None if not found.
        """
        self._check_connected()

        flow_key = self._flow_key(task_id)
        exists = await self._redis.exists(flow_key)

        if not exists:
            return None

        data = await self._redis.hgetall(flow_key)
        return self._parse_scheduled_flow(data)

    async def log_timer(
        self, flow_id: str, step: int, timer_fire_at: datetime, timer_name: str | None = None
    ) -> None:
        """Schedule a durable timer.

        Uses ZADD to sorted set for efficient expiry queries.
        Score = fire_at timestamp for range queries.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)
        fire_at_millis = int(timer_fire_at.timestamp() * 1000)

        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(inv_key, "status", InvocationStatus.WAITING_FOR_TIMER.value)
            await pipe.hset(inv_key, "fire_at", fire_at_millis)
            await pipe.hset(inv_key, "timer_name", timer_name or "")

            timer_member = f"{flow_id}:{step}"
            await pipe.zadd("ergon:timers:pending", {timer_member: fire_at_millis})
            await pipe.execute()

        # Notify timer processor
        self._timer_notify.set()
        self._timer_notify.clear()

    async def get_expired_timers(self, now: datetime) -> list[TimerInfo]:
        """Get all timers that have expired.

        Returns TimerInfo objects with flow_id, step, fire_at, and timer_name.
        """
        self._check_connected()

        now_millis = int(now.timestamp() * 1000)

        expired = await self._redis.zrangebyscore(
            "ergon:timers:pending", 0, now_millis, withscores=True, start=0, num=100
        )

        timers = []
        for member, score in zip(expired[::2], expired[1::2]):
            parts = member.decode().split(":")
            if len(parts) != 2:
                continue

            flow_id = parts[0]
            try:
                step = int(parts[1])
                inv_key = f"ergon:inv:{flow_id}:{step}"
                inv_data = await self._redis.hgetall(inv_key)
                timer_name = inv_data.get(b"timer_name", b"").decode() if inv_data else None
                fire_at = datetime.fromtimestamp(score / 1000.0, tz=UTC)

                timers.append(
                    TimerInfo(flow_id=flow_id, step=step, fire_at=fire_at, timer_name=timer_name)
                )
            except (ValueError, KeyError):
                continue

        return timers

    async def claim_timer(self, flow_id: str, step: int) -> bool:
        """Atomically claim a timer.

        Uses Lua script for atomic check-and-update.
        Ensures only one worker claims each timer.

        Design: Optimistic Concurrency Control
        Lua script ensures atomicity without locks.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)
        timer_member = f"{flow_id}:{step}"

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

        unit_value = pickle.dumps(None)

        claimed = await self._redis.eval(
            script,
            2,
            inv_key,
            "ergon:timers:pending",
            timer_member,
            unit_value,
        )

        if claimed == 1:
            self._timer_notify.set()
            self._timer_notify.clear()

        return claimed == 1

    async def get_next_timer_fire_time(self) -> datetime | None:
        """Get the next timer fire time from the pending timers sorted set.

        Returns:
            Timestamp of the next timer to fire, or None if no timers pending
        """
        self._check_connected()

        result = await self._redis.zrange("ergon:timers:pending", 0, 0, withscores=True)

        if result:
            _, timestamp_ms = result[0]
            return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)

        return None

    async def log_signal(self, flow_id: str, step: int, signal_name: str) -> None:
        """Log that an invocation is waiting for a signal.

        Updates invocation status to WAITING_FOR_SIGNAL and stores signal_name.
        """
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)
        now = datetime.now(UTC)
        now_ts = int(now.timestamp())

        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hset(inv_key, "status", InvocationStatus.WAITING_FOR_SIGNAL.value)
            await pipe.hset(inv_key, "timer_name", signal_name)
            await pipe.hset(inv_key, "updated_at", now_ts)
            await pipe.execute()

    async def store_suspension_result(
        self, flow_id: str, step: int, suspension_key: str, result: bytes
    ) -> None:
        """Store the result of a suspension (signal or timer completion).

        Args:
            flow_id: Flow identifier
            step: Step number
            suspension_key: Signal name or timer name
            result: Serialized result value
        """
        self._check_connected()

        key = f"ergon:suspension:{flow_id}:{step}:{suspension_key}"
        await self._redis.setex(key, 30 * 24 * 3600, result)

    async def get_suspension_result(
        self, flow_id: str, step: int, suspension_key: str
    ) -> bytes | None:
        """Retrieve the result of a suspension.

        Returns:
            Serialized result if available, None otherwise
        """
        self._check_connected()

        key = f"ergon:suspension:{flow_id}:{step}:{suspension_key}"
        return await self._redis.get(key)

    async def remove_suspension_result(self, flow_id: str, step: int, suspension_key: str) -> None:
        """Remove a suspension result after it's been consumed."""
        self._check_connected()

        key = f"ergon:suspension:{flow_id}:{step}:{suspension_key}"
        await self._redis.delete(key)

    async def get_waiting_signals(self) -> list:
        """Get all invocations waiting for signals.

        Returns:
            List of SignalInfo tuples (flow_id, step, signal_name)
        """
        self._check_connected()

        signals = []

        async for inv_key in self._redis.scan_iter(match="ergon:inv:*"):
            status = await self._redis.hget(inv_key, "status")

            if status and status.decode() == InvocationStatus.WAITING_FOR_SIGNAL.value:
                parts = inv_key.decode().split(":")
                if len(parts) == 4:
                    flow_id = parts[2]
                    try:
                        step = int(parts[3])
                        signal_name_bytes = await self._redis.hget(inv_key, "timer_name")
                        signal_name = signal_name_bytes.decode() if signal_name_bytes else None

                        signals.append(
                            {"flow_id": flow_id, "step": step, "signal_name": signal_name}
                        )
                    except ValueError:
                        continue

        return signals

    async def update_is_retryable(self, flow_id: str, step: int, is_retryable: bool) -> None:
        """Update the is_retryable flag for an invocation."""
        self._check_connected()

        inv_key = self._invocation_key(flow_id, step)
        value = "1" if is_retryable else "0"

        await self._redis.hset(inv_key, "is_retryable", value)

    async def resume_flow(self, flow_id: str) -> bool:
        """Resume a suspended flow (after signal or timer completion).

        Uses flow_id -> task_id index for O(1) lookup.
        Atomically checks SUSPENDED status and updates to PENDING.
        Re-enqueues to pending queue if successful.

        Returns:
            True if flow was resumed, False if already resumed or not suspended
        """
        self._check_connected()

        task_id = await self._redis.get(f"ergon:flow_task_map:{flow_id}")

        if not task_id:
            return False

        task_id = task_id.decode()
        flow_key = self._flow_key(task_id)

        script = """
        local flow_key = KEYS[1]
        local now = ARGV[1]

        local status = redis.call('HGET', flow_key, 'status')

        if status == 'SUSPENDED' then
            redis.call('HSET', flow_key, 'status', 'PENDING')
            redis.call('HDEL', flow_key, 'locked_by')
            redis.call('HSET', flow_key, 'updated_at', now)
            return 1
        else
            return 0
        end
        """

        now = datetime.now(UTC).timestamp()
        resumed = await self._redis.eval(script, 1, flow_key, int(now))

        if resumed == 1:
            await self._redis.rpush("ergon:queue:pending", task_id)

            # Wake up waiting worker
            # Don't clear here - worker clears after waking up
            self._work_notify.set()

            return True

        return False

    async def retry_flow(self, task_id: str, error_message: str, delay_seconds: int) -> None:
        """Retry a failed flow after a delay.

        Updates flow metadata, increments retry_count, and adds to delayed queue.
        """
        self._check_connected()

        flow_key = self._flow_key(task_id)
        scheduled_for = datetime.now(UTC).timestamp() + delay_seconds
        scheduled_for_ms = int(scheduled_for * 1000)

        now = datetime.now(UTC).timestamp()

        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.hincrby(flow_key, "retry_count", 1)
            await pipe.hset(flow_key, "error_message", error_message)
            await pipe.hset(flow_key, "status", TaskStatus.PENDING.value)
            await pipe.hset(flow_key, "scheduled_for", scheduled_for_ms)
            await pipe.hdel(flow_key, "locked_by")
            await pipe.hset(flow_key, "updated_at", int(now))
            await pipe.zadd("ergon:queue:delayed", {task_id: scheduled_for_ms})
            await pipe.execute()

        self._work_notify.set()
        self._work_notify.clear()

    async def reset(self) -> None:
        """Reset all ergon data (clear all keys).

        Only deletes ergon:* keys, doesn't affect other Redis data.
        """
        self._check_connected()

        keys = []
        async for key in self._redis.scan_iter(match="ergon:*"):
            keys.append(key)

        if keys:
            await self._redis.delete(*keys)

    async def _get_invocation_by_key(self, key: str) -> Invocation:
        """Fetch and parse invocation from Redis HASH."""
        data = await self._redis.hgetall(key)
        return self._parse_invocation(data)

    def _parse_invocation(self, data: dict) -> Invocation:
        """Parse invocation from Redis HASH data.

        Returns Invocation or raises StorageError.
        """
        try:
            inv_id = data[b"id"].decode()
            flow_id = data[b"flow_id"].decode()
            step = int(data[b"step"].decode())
            class_name = data[b"class_name"].decode()
            method_name = data[b"method_name"].decode()
            parameters = data[b"parameters"]
            params_hash = int(data[b"params_hash"].decode())
            status = InvocationStatus(data[b"status"].decode())
            attempts = int(data[b"attempts"].decode())
            is_retryable_str = data.get(b"is_retryable", b"1").decode()
            is_retryable = is_retryable_str == "1" if is_retryable_str else None

            created_at_ts = int(data[b"created_at"].decode())
            timestamp = datetime.fromtimestamp(created_at_ts, tz=UTC)

            updated_at_ts = int(data.get(b"updated_at", b"0").decode())
            updated_at = (
                datetime.fromtimestamp(updated_at_ts, tz=UTC) if updated_at_ts else timestamp
            )

            return_value = data.get(b"return_value")

            delay = None
            if b"delay" in data:
                delay = int(data[b"delay"].decode())

            retry_policy = None
            if b"retry_policy" in data:
                retry_policy = pickle.loads(data[b"retry_policy"])

            timer_fire_at = None
            if b"fire_at" in data:
                fire_at_ms = int(data[b"fire_at"].decode())
                if fire_at_ms > 0:
                    timer_fire_at = datetime.fromtimestamp(fire_at_ms / 1000, tz=UTC)

            timer_name = None
            if b"timer_name" in data:
                timer_name = data[b"timer_name"].decode()
                if not timer_name:
                    timer_name = None

            return Invocation(
                id=inv_id,
                flow_id=flow_id,
                step=step,
                timestamp=timestamp,
                class_name=class_name,
                method_name=method_name,
                status=status,
                attempts=attempts,
                parameters=parameters,
                params_hash=params_hash,
                return_value=return_value,
                delay=delay,
                retry_policy=retry_policy,
                is_retryable=is_retryable,
                timer_fire_at=timer_fire_at,
                timer_name=timer_name,
                updated_at=updated_at,
            )
        except (KeyError, ValueError, UnicodeDecodeError) as e:
            raise StorageError(f"Failed to parse invocation: {e}")

    def _parse_scheduled_flow(self, data: dict) -> ScheduledFlow:
        """Parse scheduled flow from Redis HASH data.

        Returns ScheduledFlow or raises StorageError.
        """
        try:
            task_id = data[b"task_id"].decode()
            flow_id = data[b"flow_id"].decode()
            flow_type = data[b"flow_type"].decode()
            flow_data = data[b"flow_data"]
            status = TaskStatus(data[b"status"].decode())
            retry_count = int(data.get(b"retry_count", b"0").decode())

            # Parse timestamps as Unix timestamps (integers)
            created_at_ts = int(data[b"created_at"].decode())
            created_at = datetime.fromtimestamp(created_at_ts, tz=UTC)

            updated_at_ts = int(data.get(b"updated_at", b"0").decode())
            updated_at = (
                datetime.fromtimestamp(updated_at_ts, tz=UTC) if updated_at_ts else created_at
            )

            locked_by = None
            if b"locked_by" in data:
                locked_by = data[b"locked_by"].decode()

            error_message = None
            if b"error_message" in data:
                error_message = data[b"error_message"].decode()

            scheduled_for = None
            if b"scheduled_for" in data:
                scheduled_for_ms = int(data[b"scheduled_for"].decode())
                if scheduled_for_ms > 0:
                    scheduled_for = datetime.fromtimestamp(scheduled_for_ms / 1000, tz=UTC)

            # Parse parent metadata
            parent_metadata = None
            if b"parent_flow_id" in data:
                parent_flow_id = data[b"parent_flow_id"].decode()
                signal_token = data.get(b"signal_token", b"").decode()
                if parent_flow_id:
                    parent_metadata = (parent_flow_id, signal_token)

            version = None
            if b"version" in data:
                version = data[b"version"].decode()
                if not version:
                    version = None

            return ScheduledFlow(
                task_id=task_id,
                flow_id=flow_id,
                flow_type=flow_type,
                flow_data=flow_data,
                status=status,
                locked_by=locked_by,
                retry_count=retry_count,
                created_at=created_at,
                updated_at=updated_at,
                error_message=error_message,
                scheduled_for=scheduled_for,
                parent_metadata=parent_metadata,
                version=version,
            )
        except (KeyError, ValueError, UnicodeDecodeError) as e:
            raise StorageError(f"Failed to parse scheduled flow: {e}")

    def work_notify(self) -> asyncio.Event:
        """Return event for work notifications.

        Workers use this to wait for work instead of polling. Set when
        new work is enqueued or flows are resumed.

        Returns:
            asyncio.Event that workers wait on
        """
        return self._work_notify

    def timer_notify(self) -> asyncio.Event:
        """Return event for timer notifications.

        Timer processors use this to wake up when timers are scheduled
        or claimed, instead of polling at fixed intervals.

        Returns:
            asyncio.Event that timer processors wait on
        """
        return self._timer_notify

    def status_notify(self) -> asyncio.Event:
        """Return event for flow status change notifications.

        Callers can use this to wait for flow status changes instead
        of polling.

        Returns:
            asyncio.Event that is set when any flow status changes
        """
        return self._status_notify
