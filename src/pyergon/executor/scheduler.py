"""
Scheduler - Simple API for enqueuing flows for distributed execution.

Design Principle: Single Responsibility (SOLID)
Scheduler has ONE job: schedule flows for execution.
It does NOT execute flows (that's Worker's job).

From Dave Cheney's Practical Go:
"A good package starts with its name" - Scheduler clearly describes
what it provides (flow scheduling), not what it contains.

From Software Design Patterns:
Like the Factory pattern (Chapter 9), Scheduler creates work items
(ScheduledFlows) without executing them. Separation of creation and execution.

From Rust ergon scheduler.rs:
- Simple API: schedule() method takes flow instance
- Serializes flow and stores in queue
- Returns task_id for tracking
- No execution logic (pure scheduling)
- Versioning support: with_version(), unversioned(), from_env()

Key Benefit:
Client code doesn't need to know about storage, serialization, or task IDs.
Scheduler hides these details (Façade-like simplification).
"""

import os
import pickle
from datetime import UTC
from typing import Any

from uuid_extensions import uuid7

from pyergon.storage.base import ExecutionLog


class Scheduler:
    """
    Scheduler for distributed flow execution with versioning support.

    **Rust Reference**: `src/executor/scheduler.rs` lines 51-153

    Design Pattern: Façade Pattern (Chapter 10)
    Simplifies the complex process of:
    1. Generating task ID
    2. Determining flow type
    3. Serializing flow instance
    4. Applying deployment version
    5. Storing in distributed queue

    Into a single schedule() method call.

    From Dave Cheney: "Design APIs for their default use case"
    Most common use: schedule a flow, get back task_id for tracking.

    **Versioning** (matches Rust scheduler.rs:10-22):
    You must configure versioning before use:
    - `.with_version("v1.0")` - Set deployment version
    - `.unversioned()` - Explicitly opt out
    - `.from_env()` - Read from DEPLOY_VERSION env var

    Usage:
        storage = SqliteExecutionLog("workflow.db")
        await storage.connect()

        # Option 1: Set version explicitly
        scheduler = Scheduler(storage).with_version("v1.0")

        # Option 2: Use environment variable
        # $ export DEPLOY_VERSION=v1.2.3
        scheduler = Scheduler(storage).from_env()

        # Option 3: Explicitly unversioned
        scheduler = Scheduler(storage).unversioned()

        # Schedule a flow for execution
        flow = MyWorkflow(config)
        task_id = await scheduler.schedule(flow)

        # Track the task
        print(f"Scheduled task: {task_id}")
    """

    def __init__(self, storage: ExecutionLog, version: str | None = None):
        """
        Initialize scheduler with storage backend.

        **Note**: For versioning support, use factory methods instead:
        - `Scheduler(storage).with_version("v1.0")`
        - `Scheduler(storage).unversioned()`
        - `Scheduler(storage).from_env()`

        From Dave Cheney: "Avoid package level state"
        Storage is passed explicitly, not accessed via global.

        From Software Design Patterns (Chapter 7):
        Composition over inheritance - Scheduler HAS-A storage,
        not IS-A storage subclass.

        Args:
            storage: Storage backend for persisting scheduled flows
            version: Internal parameter for version configuration
        """
        self._storage = storage
        self._version = version

    async def schedule(self, flow_instance: Any, flow_id: str | None = None) -> str:
        """
        Schedule a flow for distributed execution.

        **Rust Reference**: `src/executor/scheduler.rs` schedule() method

        Retry policy is configured via @flow(retry_policy=...) decorator,
        NOT as a parameter to schedule(). This matches Rust's compile-time
        #[flow(retry = ...)] attribute macro.

        From Dave Cheney: "Let functions define the behavior they require"
        Takes Any flow_instance (duck typing), not requiring specific base class.

        From Dave Cheney: "Discourage the use of nil as a parameter"
        flow_id is Optional (explicit None default), not ambiguous.

        Args:
            flow_instance: Instance of @flow_type decorated class
            flow_id: Optional flow ID (generated if not provided)

        Returns:
            task_id: Unique task identifier for tracking

        Raises:
            SchedulerError: If flow cannot be serialized or stored

        Example:
            from pyergon.core import RetryPolicy

            @dataclass
            @flow_type
            class OrderProcessor:
                order_id: str

                @flow(retry_policy=RetryPolicy.STANDARD)  # Set here, not in schedule()
                async def process(self):
                    ...

            # Schedule flow
            order = OrderProcessor(order_id="ORD-123")
            task_id = await scheduler.schedule(order)
        """
        # Generate flow_id if not provided
        if flow_id is None:
            flow_id = str(uuid7())

        # Determine flow type using type_id() method (matches registry)
        # **Rust Reference**: Uses T::type_id() for stable flow type identification
        # This MUST match what Worker.register() stores in the registry
        if hasattr(flow_instance.__class__, "type_id"):
            flow_type = flow_instance.__class__.type_id()
        else:
            # Fallback to class name for non-@flow classes
            flow_type = flow_instance.__class__.__name__

        # Serialize flow instance (now includes _ergon_retry_policy if set)
        # From Dave Cheney: "Only handle an error once"
        # If serialization fails, let exception propagate
        try:
            flow_data = pickle.dumps(flow_instance)
        except Exception as e:
            raise SchedulerError(f"Failed to serialize flow {flow_type}: {e}") from e

        # Create ScheduledFlow object
        # From Rust: child_flow.rs lines 248-266 (creating ScheduledFlow for child)
        # Same structure applies for all flows - child or top-level
        from datetime import datetime

        from pyergon.core import ScheduledFlow, TaskStatus

        now = datetime.now(UTC)
        task_id = flow_id  # In Rust, task_id == flow_id for top-level flows

        scheduled_flow = ScheduledFlow(
            task_id=task_id,
            flow_id=flow_id,
            flow_type=flow_type,
            flow_data=flow_data,
            status=TaskStatus.PENDING,
            locked_by=None,
            created_at=now,
            updated_at=now,
            retry_count=0,
            error_message=None,
            scheduled_for=None,  # Execute immediately
            parent_metadata=None,  # Top-level flow has no parent
            retry_policy=None,  # Retry policy is stored in flow instance, not here
            version=self._version,  # Deployment version from configuration
        )

        # Enqueue to storage
        try:
            task_id = await self._storage.enqueue_flow(scheduled_flow)
        except Exception as e:
            raise SchedulerError(f"Failed to enqueue flow {flow_type}: {e}") from e

        return task_id

    def with_version(self, version: str) -> "Scheduler":
        """
        Set deployment version for all scheduled flows.

        **Rust Reference**: `src/executor/scheduler.rs` lines 212-217

        This is the most common configuration. The version is applied automatically
        to all flows scheduled by this scheduler, representing the code version
        that deployed the flows.

        Args:
            version: Semantic version string (e.g., "1.0.0", "v2.0", "2024-01-15")

        Returns:
            New Scheduler configured with version

        Example:
            # Use package version from environment
            import os
            scheduler = Scheduler(storage).with_version(os.getenv("APP_VERSION", "dev"))

            # Or hardcode version
            scheduler = Scheduler(storage).with_version("production-2024-01")
        """
        return Scheduler(self._storage, version=version)

    def unversioned(self) -> "Scheduler":
        """
        Explicitly opt out of versioning.

        **Rust Reference**: `src/executor/scheduler.rs` lines 237-240

        Use this when you don't need version tracking. All flows scheduled
        by this scheduler will have `version = None`.

        Returns:
            New Scheduler explicitly configured as unversioned

        Example:
            # For development or when version tracking isn't needed
            scheduler = Scheduler(storage).unversioned()
        """
        return Scheduler(self._storage, version=None)

    def from_env(self) -> "Scheduler":
        """
        Configure version from DEPLOY_VERSION environment variable.

        **Rust Reference**: `src/executor/scheduler.rs` lines 267-270

        If the environment variable is set, uses that value as the version.
        If not set, creates an unversioned scheduler.

        This is useful for deployments where the version is injected at runtime.

        Returns:
            New Scheduler configured from environment

        Example:
            # Reads from DEPLOY_VERSION env var
            # $ export DEPLOY_VERSION=v1.2.3
            # $ python run_worker.py
            scheduler = Scheduler(storage).from_env()
        """
        version = os.getenv("DEPLOY_VERSION")
        return Scheduler(self._storage, version=version)

    def version(self) -> str | None:
        """
        Get the configured deployment version.

        **Rust Reference**: `src/executor/scheduler.rs` lines 409-411

        Returns:
            Version string if configured, None if unversioned
        """
        return self._version

    @property
    def storage(self) -> ExecutionLog:
        """
        Get storage backend (for testing/inspection).

        From Software Design Patterns (Chapter 7):
        @property allows read access without exposing setter.
        """
        return self._storage


class SchedulerError(Exception):
    """
    Flow scheduling failed.

    From Dave Cheney: "Errors are values"
    Custom exception with context, not generic Exception.

    From Dave Cheney: "Add context to errors"
    SchedulerError wraps underlying error with scheduling context.
    """

    pass
