"""Flow scheduling API for distributed execution.

Design: Single Responsibility
Scheduler has one job: schedule flows for execution.
It does not execute flows - that's Worker's job.

Scheduler creates work items (ScheduledFlows) and enqueues them,
separating creation from execution. Provides a simple interface:
schedule() method takes flow instance, serializes it, stores in
queue, and returns task_id for tracking.

Benefits:
- Client code doesn't need to know about storage, serialization, or task IDs
- Versioning support: with_version(), unversioned(), from_env()
- No execution logic (pure scheduling)
"""

import os
import pickle
from datetime import UTC
from typing import Any

from uuid_extensions import uuid7

from pyergon.storage.base import ExecutionLog


class Scheduler:
    """Scheduler for distributed flow execution with versioning support.

    Simplifies scheduling by handling:
    1. Generating task ID
    2. Determining flow type
    3. Serializing flow instance
    4. Applying deployment version
    5. Storing in distributed queue

    Versioning configuration (choose one):
    - with_version("v1.0") - Set deployment version
    - unversioned() - Explicitly opt out
    - from_env() - Read from DEPLOY_VERSION env var

    Example:
        ```python
        storage = SqliteExecutionLog("workflow.db")
        await storage.connect()

        # Set version explicitly
        scheduler = Scheduler(storage).with_version("v1.0")

        # Use environment variable
        # $ export DEPLOY_VERSION=v1.2.3
        scheduler = Scheduler(storage).from_env()

        # Explicitly unversioned
        scheduler = Scheduler(storage).unversioned()

        # Schedule a flow
        flow = MyWorkflow(config)
        task_id = await scheduler.schedule(flow)
        print(f"Scheduled task: {task_id}")
        ```
    """

    def __init__(self, storage: ExecutionLog, version: str | None = None):
        """Initialize scheduler with storage backend.

        For versioning support, use factory methods:
        - Scheduler(storage).with_version("v1.0")
        - Scheduler(storage).unversioned()
        - Scheduler(storage).from_env()

        Args:
            storage: Storage backend for persisting scheduled flows
            version: Internal parameter for version configuration
        """
        self._storage = storage
        self._version = version

    async def schedule(self, flow_instance: Any, flow_id: str | None = None) -> str:
        """Schedule a flow for distributed execution.

        Retry policy is configured via @flow(retry_policy=...) decorator,
        not as a parameter to schedule().

        Args:
            flow_instance: Instance of @flow_type decorated class
            flow_id: Flow ID (generated if not provided)

        Returns:
            Unique task identifier for tracking

        Raises:
            SchedulerError: If flow cannot be serialized or stored

        Example:
            ```python
            from pyergon.core import RetryPolicy

            @dataclass
            @flow_type
            class OrderProcessor:
                order_id: str

                @flow(retry_policy=RetryPolicy.STANDARD)
                async def process(self):
                    ...

            order = OrderProcessor(order_id="ORD-123")
            task_id = await scheduler.schedule(order)
            ```
        """
        # Generate flow_id if not provided
        if flow_id is None:
            flow_id = str(uuid7())

        # Determine flow type using type_id() method (matches registry)
        # Must match what Worker.register() stores in the registry
        if hasattr(flow_instance.__class__, "type_id"):
            flow_type = flow_instance.__class__.type_id()
        else:
            # Fallback to class name for non-@flow classes
            flow_type = flow_instance.__class__.__name__

        # Serialize flow instance
        try:
            flow_data = pickle.dumps(flow_instance)
        except Exception as e:
            raise SchedulerError(f"Failed to serialize flow {flow_type}: {e}") from e

        # Create ScheduledFlow object
        from datetime import datetime

        from pyergon.core import ScheduledFlow, TaskStatus

        now = datetime.now(UTC)
        task_id = flow_id

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
            scheduled_for=None,
            parent_metadata=None,
            retry_policy=None,
            version=self._version,
        )

        # Enqueue to storage
        try:
            task_id = await self._storage.enqueue_flow(scheduled_flow)
        except Exception as e:
            raise SchedulerError(f"Failed to enqueue flow {flow_type}: {e}") from e

        return task_id

    def with_version(self, version: str) -> "Scheduler":
        """Set deployment version for all scheduled flows.

        The version is applied automatically to all flows scheduled
        by this scheduler, representing the code version that
        deployed the flows.

        Args:
            version: Semantic version string (e.g., "1.0.0", "v2.0", "2024-01-15")

        Returns:
            New Scheduler configured with version

        Example:
            ```python
            import os
            scheduler = Scheduler(storage).with_version(os.getenv("APP_VERSION", "dev"))
            scheduler = Scheduler(storage).with_version("production-2024-01")
            ```
        """
        return Scheduler(self._storage, version=version)

    def unversioned(self) -> "Scheduler":
        """Explicitly opt out of versioning.

        Use this when you don't need version tracking. All flows
        scheduled by this scheduler will have version = None.

        Returns:
            New Scheduler explicitly configured as unversioned

        Example:
            ```python
            scheduler = Scheduler(storage).unversioned()
            ```
        """
        return Scheduler(self._storage, version=None)

    def from_env(self) -> "Scheduler":
        """Configure version from DEPLOY_VERSION environment variable.

        If the environment variable is set, uses that value as the version.
        If not set, creates an unversioned scheduler.

        Returns:
            New Scheduler configured from environment

        Example:
            ```python
            # $ export DEPLOY_VERSION=v1.2.3
            # $ python run_worker.py
            scheduler = Scheduler(storage).from_env()
            ```
        """
        version = os.getenv("DEPLOY_VERSION")
        return Scheduler(self._storage, version=version)

    def version(self) -> str | None:
        """Get the configured deployment version.

        Returns:
            Version string if configured, None if unversioned
        """
        return self._version

    @property
    def storage(self) -> ExecutionLog:
        """Get storage backend for testing/inspection."""
        return self._storage


class SchedulerError(Exception):
    """Flow scheduling failed."""

    pass
